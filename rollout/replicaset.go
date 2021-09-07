package rollout

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	experimentutil "github.com/argoproj/argo-rollouts/utils/experiment"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	patchtypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/kubernetes/pkg/controller"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
	"k8s.io/utils/pointer"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	clientset "github.com/argoproj/argo-rollouts/pkg/client/clientset/versioned"
	analysisutil "github.com/argoproj/argo-rollouts/utils/analysis"
	"github.com/argoproj/argo-rollouts/utils/annotations"
	"github.com/argoproj/argo-rollouts/utils/conditions"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	"github.com/argoproj/argo-rollouts/utils/record"
	replicasetutil "github.com/argoproj/argo-rollouts/utils/replicaset"
	log "github.com/sirupsen/logrus"
)

var controllerKind = v1alpha1.SchemeGroupVersion.WithKind("Rollout")

const (
	addScaleDownAtAnnotationsPatch    = `[{ "op": "add", "path": "/metadata/annotations/%s", "value": "%s"}]`
	removeScaleDownAtAnnotationsPatch = `[{ "op": "remove", "path": "/metadata/annotations/%s"}]`
)

type Deployer interface {
	RemoveScaleDownDeadlines() error
	GetAllReplicaSetsAndSyncRevision(createIfNotExisted bool) (*appsv1.ReplicaSet, error)

	ReconcileOthersForBlueGreen() (bool, error)
	ReconcileRevisionHistoryLimit() error

	ReconcileBlueGreen(activeSvc *corev1.Service) error
	ReconcileCanary() (bool, error)

	ReconcileEphemeralMetadata() error

	GetStableHash() string
	GetCanaryHash() string

	AtDesiredReplicaCountsForCanary() bool
	GetNewName() string

	SyncRolloutStatusCanary() error
	SyncRolloutStatusBlueGreen(previewSvc *corev1.Service, activeSvc *corev1.Service) error
}

type RolloutProvider interface {
	GetRollout() *v1alpha1.Rollout
	SetRollout(*v1alpha1.Rollout)
	SetNewRollout(*v1alpha1.Rollout)

	GetNewRS() *appsv1.ReplicaSet
	GetStableRS() *appsv1.ReplicaSet
	GetOtherRSs() []*appsv1.ReplicaSet
	GetNewStatus() v1alpha1.RolloutStatus

	GetCurrentARs() analysisutil.CurrentAnalysisRuns
	GetOtherARs() []*v1alpha1.AnalysisRun
	GetOtherExs() []*v1alpha1.Experiment
	CheckTargetsVerified() (bool, error)

	CalculateRolloutConditions(v1alpha1.RolloutStatus) v1alpha1.RolloutStatus
	ShouldFullPromote(v1alpha1.RolloutStatus) string
	GetRolloutValidationErrors() error
	ResetRolloutStatus(newStatus *v1alpha1.RolloutStatus)
	PromoteStable(newStatus *v1alpha1.RolloutStatus, reason string) error
	PersistRolloutStatus(newStatus *v1alpha1.RolloutStatus) error
	CalculateScaleUpPreviewCheckPoint(newStatus v1alpha1.RolloutStatus) bool
	CompletedCurrentCanaryStep() bool
}

type replicasetDeployer struct {
	kubeclientset kubernetes.Interface
	log           *log.Entry
	allRSs        []*appsv1.ReplicaSet
	olderRSs      []*appsv1.ReplicaSet
	recorder      record.EventRecorder
	pauseContext  *pauseContext
	resyncPeriod  time.Duration

	RolloutProvider

	argoprojclientset clientset.Interface
	replicaSetLister  appslisters.ReplicaSetLister
	refResolver       TemplateRefResolver

	enqueueRolloutAfter func(obj interface{}, duration time.Duration) //nolint:structcheck
	setRolloutRevision  func(revision string) error
	patchCondition      func(r *v1alpha1.Rollout, newStatus *v1alpha1.RolloutStatus, conditionList ...*v1alpha1.RolloutCondition) error

	deleteAnalysisRuns func(ars []*v1alpha1.AnalysisRun) error
	deleteExperiments  func(exs []*v1alpha1.Experiment) error
}

// removeScaleDownDelay removes the `scale-down-deadline` annotation from the ReplicaSet (if it exists)
func (d *replicasetDeployer) removeScaleDownDelay(rs *appsv1.ReplicaSet) error {
	ctx := context.TODO()
	if !replicasetutil.HasScaleDownDeadline(rs) {
		return nil
	}
	patch := fmt.Sprintf(removeScaleDownAtAnnotationsPatch, v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey)
	_, err := d.kubeclientset.AppsV1().ReplicaSets(rs.Namespace).Patch(ctx, rs.Name, patchtypes.JSONPatchType, []byte(patch), metav1.PatchOptions{})
	if err == nil {
		d.log.Infof("Removed '%s' annotation from RS '%s'", v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey, rs.Name)
	}
	return err
}

// addScaleDownDelay injects the `scale-down-deadline` annotation to the ReplicaSet, or if
// scaleDownDelaySeconds is zero, removes the annotation if it exists
func (d *replicasetDeployer) addScaleDownDelay(rs *appsv1.ReplicaSet, scaleDownDelaySeconds time.Duration) error {
	if rs == nil {
		return nil
	}
	ctx := context.TODO()
	if scaleDownDelaySeconds == 0 {
		// If scaledown deadline is zero, it means we need to remove any replicasets with the delay
		// This might happen if we switch from canary with traffic routing to basic canary
		if replicasetutil.HasScaleDownDeadline(rs) {
			return d.removeScaleDownDelay(rs)
		}
		return nil
	}
	deadline := metav1.Now().Add(scaleDownDelaySeconds).UTC().Format(time.RFC3339)
	patch := fmt.Sprintf(addScaleDownAtAnnotationsPatch, v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey, deadline)
	_, err := d.kubeclientset.AppsV1().ReplicaSets(rs.Namespace).Patch(ctx, rs.Name, patchtypes.JSONPatchType, []byte(patch), metav1.PatchOptions{})
	if err == nil {
		d.log.Infof("Set '%s' annotation on '%s' to %s (%s)", v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey, rs.Name, deadline, scaleDownDelaySeconds)
	}
	return err
}

func (c *Controller) getReplicaSetsForRollouts(r *v1alpha1.Rollout) ([]*appsv1.ReplicaSet, error) {
	ctx := context.TODO()
	// List all ReplicaSets to find those we own but that no longer match our
	// selector. They will be orphaned by ClaimReplicaSets().
	rsList, err := c.replicaSetLister.ReplicaSets(r.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	replicaSetSelector, err := metav1.LabelSelectorAsSelector(r.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("rollout %s/%s has invalid label selector: %v", r.Namespace, r.Name, err)
	}
	// If any adoptions are attempted, we should first recheck for deletion with
	// an uncached quorum read sometime after listing ReplicaSets (see #42639).
	canAdoptFunc := controller.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh, err := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != r.UID {
			return nil, fmt.Errorf("original Rollout %v/%v is gone: got uid %v, wanted %v", r.Namespace, r.Name, fresh.UID, r.UID)
		}
		return fresh, nil
	})
	cm := controller.NewReplicaSetControllerRefManager(c.replicaSetControl, r, replicaSetSelector, controllerKind, canAdoptFunc)
	return cm.ClaimReplicaSets(rsList)
}

// RemoveScaleDownDeadlines removes the scale-down-deadline annotation from the new/stable ReplicaSets,
// in the event that we moved back to an older revision that is still within its scaleDownDelay.
func (d *replicasetDeployer) RemoveScaleDownDeadlines() error {
	var toRemove []*appsv1.ReplicaSet
	if d.newRS() != nil && !d.shouldDelayScaleDownOnAbort() {
		toRemove = append(toRemove, d.newRS())
	}
	if d.stableRS() != nil {
		if len(toRemove) == 0 || d.stableRS().Name != d.newRS().Name {
			toRemove = append(toRemove, d.stableRS())
		}
	}
	for _, rs := range toRemove {
		err := d.removeScaleDownDelay(rs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *replicasetDeployer) ReconcileNewReplicaSet() (bool, error) {
	if d.newRS() == nil {
		return false, nil
	}
	newReplicasCount, err := replicasetutil.NewRSNewReplicas(d.rollout(), d.allRSs, d.newRS())
	if err != nil {
		return false, err
	}

	if d.shouldDelayScaleDownOnAbort() {
		abortScaleDownDelaySeconds := defaults.GetAbortScaleDownDelaySecondsOrDefault(d.rollout())
		d.log.Infof("Scale down new rs '%s' on abort (%v)", d.newRS().Name, abortScaleDownDelaySeconds)

		// if the newRS has scale down annotation, check if it should be scaled down now
		if scaleDownAtStr, ok := d.newRS().Annotations[v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey]; ok {
			d.log.Infof("New rs '%s' has scaledown deadline annotation: %s", d.newRS().Name, scaleDownAtStr)
			scaleDownAtTime, err := time.Parse(time.RFC3339, scaleDownAtStr)
			if err != nil {
				d.log.Warnf("Unable to read scaleDownAt label on rs '%s'", d.newRS().Name)
			} else {
				now := metav1.Now()
				scaleDownAt := metav1.NewTime(scaleDownAtTime)
				if scaleDownAt.After(now.Time) {
					d.log.Infof("RS '%s' has not reached the scaleDownTime", d.newRS().Name)
					remainingTime := scaleDownAt.Sub(now.Time)
					if remainingTime < d.resyncPeriod {
						d.enqueueRolloutAfter(d.rollout(), remainingTime)
						return false, nil
					}
				} else {
					d.log.Infof("RS '%s' has reached the scaleDownTime", d.newRS().Name)
					newReplicasCount = int32(0)
				}
			}
		} else if abortScaleDownDelaySeconds != nil {
			err = d.addScaleDownDelay(d.newRS(), *abortScaleDownDelaySeconds)
			if err != nil {
				return false, err
			}
		}
	}

	scaled, _, err := d.ScaleReplicaSetAndRecordEvent(d.newRS(), newReplicasCount)
	return scaled, err
}

// shouldDelayScaleDownOnAbort returns if we are aborted and we should delay scaledown of canary/preview
func (d *replicasetDeployer) shouldDelayScaleDownOnAbort() bool {
	return d.pauseContext.IsAborted() && defaults.GetAbortScaleDownDelaySecondsOrDefault(d.rollout()) != nil
}

func (d *replicasetDeployer) ReconcileOthersForBlueGreen() (bool, error) {
	return d.reconcileOtherReplicaSets(d.scaleDownOldReplicaSetsForBlueGreen)
}

func (d *replicasetDeployer) reconcileOthersForCanary() (bool, error) {
	return d.reconcileOtherReplicaSets(d.scaleDownOldReplicaSetsForCanary)
}

// scaleDownOldReplicaSetsForBlueGreen scales down old replica sets when rollout strategy is "Blue Green".
func (c *replicasetDeployer) scaleDownOldReplicaSetsForBlueGreen(oldRSs []*appsv1.ReplicaSet) (bool, error) {
	if getPauseCondition(c.rollout(), v1alpha1.PauseReasonInconclusiveAnalysis) != nil {
		c.log.Infof("Cannot scale down old ReplicaSets while paused with inconclusive Analysis ")
		return false, nil
	}
	if c.rollout().Spec.Strategy.BlueGreen != nil && c.rollout().Spec.Strategy.BlueGreen.PostPromotionAnalysis != nil && c.rollout().Spec.Strategy.BlueGreen.ScaleDownDelaySeconds == nil && !skipPostPromotionAnalysisRun(c.rollout(), c.newRS()) {
		currentPostAr := c.GetCurrentARs().BlueGreenPostPromotion
		if currentPostAr == nil || currentPostAr.Status.Phase != v1alpha1.AnalysisPhaseSuccessful {
			c.log.Infof("Cannot scale down old ReplicaSets while Analysis is running and no ScaleDownDelaySeconds")
			return false, nil
		}
	}
	sort.Sort(sort.Reverse(replicasetutil.ReplicaSetsByRevisionNumber(oldRSs)))

	hasScaled := false
	annotationedRSs := int32(0)
	rolloutReplicas := defaults.GetReplicasOrDefault(c.rollout().Spec.Replicas)
	for _, targetRS := range oldRSs {
		if replicasetutil.IsStillReferenced(c.rollout().Status, targetRS) {
			// We should technically never get here because we shouldn't be passing a replicaset list
			// which includes referenced ReplicaSets. But we check just in case
			c.log.Warnf("Prevented inadvertent scaleDown of RS '%s'", targetRS.Name)
			continue
		}
		if *targetRS.Spec.Replicas == 0 {
			// cannot scale down this ReplicaSet.
			continue
		}
		var desiredReplicaCount int32
		var err error
		annotationedRSs, desiredReplicaCount, err = c.scaleDownDelayHelper(targetRS, annotationedRSs, rolloutReplicas)
		if err != nil {
			return false, err
		}

		if *targetRS.Spec.Replicas == desiredReplicaCount {
			// already at desired account, nothing to do
			continue
		}
		// Scale down.
		_, _, err = c.ScaleReplicaSetAndRecordEvent(targetRS, desiredReplicaCount)
		if err != nil {
			return false, err
		}
		hasScaled = true
	}

	return hasScaled, nil
}

// scaleDownOldReplicaSetsForCanary scales down old replica sets when rollout strategy is "canary".
func (c *replicasetDeployer) scaleDownOldReplicaSetsForCanary(oldRSs []*appsv1.ReplicaSet) (bool, error) {
	// Clean up unhealthy replicas first, otherwise unhealthy replicas will block rollout
	// and cause timeout. See https://github.com/kubernetes/kubernetes/issues/16737
	oldRSs, totalScaledDown, err := c.cleanupUnhealthyReplicas(oldRSs)
	if err != nil {
		return totalScaledDown > 0, nil
	}
	availablePodCount := replicasetutil.GetAvailableReplicaCountForReplicaSets(c.allRSs)
	minAvailable := defaults.GetReplicasOrDefault(c.rollout().Spec.Replicas) - replicasetutil.MaxUnavailable(c.rollout())
	maxScaleDown := availablePodCount - minAvailable
	if maxScaleDown <= 0 {
		// Cannot scale down.
		return false, nil
	}
	c.log.Infof("Found %d available pods, scaling down old RSes (minAvailable: %d, maxScaleDown: %d)", availablePodCount, minAvailable, maxScaleDown)

	sort.Sort(sort.Reverse(replicasetutil.ReplicaSetsByRevisionNumber(oldRSs)))

	if canProceed, err := c.canProceedWithScaleDownAnnotation(oldRSs); !canProceed || err != nil {
		return false, err
	}

	annotationedRSs := int32(0)
	rolloutReplicas := defaults.GetReplicasOrDefault(c.rollout().Spec.Replicas)
	for _, targetRS := range oldRSs {
		if replicasetutil.IsStillReferenced(c.rollout().Status, targetRS) {
			// We should technically never get here because we shouldn't be passing a replicaset list
			// which includes referenced ReplicaSets. But we check just in case
			c.log.Warnf("Prevented inadvertent scaleDown of RS '%s'", targetRS.Name)
			continue
		}
		if maxScaleDown <= 0 {
			break
		}
		if *targetRS.Spec.Replicas == 0 {
			// cannot scale down this ReplicaSet.
			continue
		}
		desiredReplicaCount := int32(0)
		if c.rollout().Spec.Strategy.Canary.TrafficRouting == nil {
			// For basic canary, we must scale down all other ReplicaSets because existence of
			// those pods will cause traffic to be served by them
			if *targetRS.Spec.Replicas > maxScaleDown {
				desiredReplicaCount = *targetRS.Spec.Replicas - maxScaleDown
			}
		} else {
			// For traffic shaped canary, we leave the old ReplicaSets up until scaleDownDelaySeconds
			annotationedRSs, desiredReplicaCount, err = c.scaleDownDelayHelper(targetRS, annotationedRSs, rolloutReplicas)
			if err != nil {
				return totalScaledDown > 0, err
			}
		}
		if *targetRS.Spec.Replicas == desiredReplicaCount {
			// already at desired account, nothing to do
			continue
		}
		// Scale down.
		_, _, err = c.ScaleReplicaSetAndRecordEvent(targetRS, desiredReplicaCount)
		if err != nil {
			return totalScaledDown > 0, err
		}
		scaleDownCount := *targetRS.Spec.Replicas - desiredReplicaCount
		maxScaleDown -= scaleDownCount
		totalScaledDown += scaleDownCount
	}

	return totalScaledDown > 0, nil
}

// canProceedWithScaleDownAnnotation returns whether or not it is safe to proceed with annotating
// old replicasets with the scale-down-deadline in the traffic-routed canary strategy.
// This method only matters with ALB canary + the target group verification feature.
// The safety guarantees we provide are that we will not scale down *anything* unless we can verify
// stable target group endpoints are registered properly.
// NOTE: this method was written in a way which avoids AWS API calls.
func (c *replicasetDeployer) canProceedWithScaleDownAnnotation(oldRSs []*appsv1.ReplicaSet) (bool, error) {
	isALBCanary := c.rollout().Spec.Strategy.Canary != nil && c.rollout().Spec.Strategy.Canary.TrafficRouting != nil && c.rollout().Spec.Strategy.Canary.TrafficRouting.ALB != nil
	if !isALBCanary {
		// Only ALB
		return true, nil
	}

	needToVerifyTargetGroups := false
	for _, targetRS := range oldRSs {
		if *targetRS.Spec.Replicas > 0 && !replicasetutil.HasScaleDownDeadline(targetRS) {
			// We encountered an old ReplicaSet that is not yet scaled down, and is not annotated
			// We only verify target groups if there is something to scale down.
			needToVerifyTargetGroups = true
			break
		}
	}
	if !needToVerifyTargetGroups {
		// All ReplicaSets are either scaled down, or have a scale-down-deadline annotation.
		// The presence of the scale-down-deadline on all oldRSs, implies we can proceed with
		// scale down, because we only add that annotation when target groups have been verified.
		// Therefore, we return true to avoid performing verification again and making unnecessary
		// AWS API calls.
		return true, nil
	}

	canProceed, err := c.CheckTargetsVerified()
	if err != nil {
		return false, err
	}
	return canProceed, nil
}

// reconcileOtherReplicaSets reconciles "other" ReplicaSets.
// Other ReplicaSets are ReplicaSets are neither the new or stable (allRSs - newRS - stableRS)
func (d *replicasetDeployer) reconcileOtherReplicaSets(scaleDownOldReplicaSets func([]*appsv1.ReplicaSet) (bool, error)) (bool, error) {
	otherRSs := controller.FilterActiveReplicaSets(d.otherRSs())
	oldPodsCount := replicasetutil.GetReplicaCountForReplicaSets(otherRSs)
	if oldPodsCount == 0 {
		// Can't scale down further
		return false, nil
	}
	d.log.Infof("Reconciling %d old ReplicaSets (total pods: %d)", len(otherRSs), oldPodsCount)

	hasScaled, err := scaleDownOldReplicaSets(otherRSs)
	if err != nil {
		return false, nil
	}

	if hasScaled {
		d.log.Infof("Scaled down old RSes")
	}
	return hasScaled, nil
}

// cleanupUnhealthyReplicas will scale down old replica sets with unhealthy replicas, so that all unhealthy replicas will be deleted.
func (d *replicasetDeployer) cleanupUnhealthyReplicas(oldRSs []*appsv1.ReplicaSet) ([]*appsv1.ReplicaSet, int32, error) {
	sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))
	// Safely scale down all old replica sets with unhealthy replicas. Replica set will sort the pods in the order
	// such that not-ready < ready, unscheduled < scheduled, and pending < running. This ensures that unhealthy replicas will
	// been deleted first and won't increase unavailability.
	totalScaledDown := int32(0)
	for i, targetRS := range oldRSs {
		if *(targetRS.Spec.Replicas) == 0 {
			// cannot scale down this replica set.
			continue
		}
		d.log.Infof("Found %d available pods in old RS %s/%s", targetRS.Status.AvailableReplicas, targetRS.Namespace, targetRS.Name)
		if *(targetRS.Spec.Replicas) == targetRS.Status.AvailableReplicas {
			// no unhealthy replicas found, no scaling required.
			continue
		}

		scaledDownCount := *(targetRS.Spec.Replicas) - targetRS.Status.AvailableReplicas
		newReplicasCount := targetRS.Status.AvailableReplicas
		if newReplicasCount > *(targetRS.Spec.Replicas) {
			return nil, 0, fmt.Errorf("when cleaning up unhealthy replicas, got invalid request to scale down %s/%s %d -> %d", targetRS.Namespace, targetRS.Name, *(targetRS.Spec.Replicas), newReplicasCount)
		}
		_, updatedOldRS, err := d.ScaleReplicaSetAndRecordEvent(targetRS, newReplicasCount)
		if err != nil {
			return nil, totalScaledDown, err
		}
		totalScaledDown += scaledDownCount
		oldRSs[i] = updatedOldRS
	}
	return oldRSs, totalScaledDown, nil
}

func (d *replicasetDeployer) ScaleReplicaSetAndRecordEvent(rs *appsv1.ReplicaSet, newScale int32) (bool, *appsv1.ReplicaSet, error) {
	// No need to scale
	if *(rs.Spec.Replicas) == newScale && !annotations.ReplicasAnnotationsNeedUpdate(rs, defaults.GetReplicasOrDefault(d.rollout().Spec.Replicas)) {
		return false, rs, nil
	}
	var scalingOperation string
	if *(rs.Spec.Replicas) < newScale {
		scalingOperation = "up"
	} else {
		scalingOperation = "down"
	}
	scaled, newRS, err := d.scaleReplicaSet(rs, newScale, d.rollout(), scalingOperation)
	return scaled, newRS, err
}

func (d *replicasetDeployer) scaleReplicaSet(rs *appsv1.ReplicaSet, newScale int32, rollout *v1alpha1.Rollout, scalingOperation string) (bool, *appsv1.ReplicaSet, error) {
	ctx := context.TODO()
	sizeNeedsUpdate := *(rs.Spec.Replicas) != newScale
	fullScaleDown := newScale == int32(0)
	rolloutReplicas := defaults.GetReplicasOrDefault(rollout.Spec.Replicas)
	annotationsNeedUpdate := annotations.ReplicasAnnotationsNeedUpdate(rs, rolloutReplicas)

	scaled := false
	var err error
	if sizeNeedsUpdate || annotationsNeedUpdate {
		rsCopy := rs.DeepCopy()
		oldScale := defaults.GetReplicasOrDefault(rs.Spec.Replicas)
		*(rsCopy.Spec.Replicas) = newScale
		annotations.SetReplicasAnnotations(rsCopy, rolloutReplicas)
		if fullScaleDown && !d.shouldDelayScaleDownOnAbort() {
			delete(rsCopy.Annotations, v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey)
		}
		rs, err = d.kubeclientset.AppsV1().ReplicaSets(rsCopy.Namespace).Update(ctx, rsCopy, metav1.UpdateOptions{})
		if err == nil && sizeNeedsUpdate {
			scaled = true
			revision, _ := replicasetutil.Revision(rs)
			d.recorder.Eventf(rollout, record.EventOptions{EventReason: conditions.ScalingReplicaSetReason}, conditions.ScalingReplicaSetMessage, scalingOperation, rs.Name, revision, oldScale, newScale)
		}
	}
	return scaled, rs, err
}

func (d *replicasetDeployer) scaleDownDelayHelper(rs *appsv1.ReplicaSet, annotationedRSs int32, rolloutReplicas int32) (int32, int32, error) {
	desiredReplicaCount := int32(0)
	scaleDownRevisionLimit := GetScaleDownRevisionLimit(d.rollout())
	if !replicasetutil.HasScaleDownDeadline(rs) && *rs.Spec.Replicas > 0 {
		// This ReplicaSet is scaled up but does not have a scale down deadline. Add one.
		if annotationedRSs < scaleDownRevisionLimit {
			annotationedRSs++
			desiredReplicaCount = *rs.Spec.Replicas
			scaleDownDelaySeconds := defaults.GetScaleDownDelaySecondsOrDefault(d.rollout())
			err := d.addScaleDownDelay(rs, scaleDownDelaySeconds)
			if err != nil {
				return annotationedRSs, desiredReplicaCount, err
			}
			d.enqueueRolloutAfter(d.rollout(), scaleDownDelaySeconds)
		}
	} else if replicasetutil.HasScaleDownDeadline(rs) {
		annotationedRSs++
		if annotationedRSs > scaleDownRevisionLimit {
			d.log.Infof("At ScaleDownDelayRevisionLimit (%d) and scaling down the rest", scaleDownRevisionLimit)
		} else {
			remainingTime, err := replicasetutil.GetTimeRemainingBeforeScaleDownDeadline(rs)
			if err != nil {
				d.log.Warnf("%v", err)
			} else if remainingTime != nil {
				d.log.Infof("RS '%s' has not reached the scaleDownTime", rs.Name)
				if *remainingTime < d.resyncPeriod {
					d.enqueueRolloutAfter(d.rollout(), *remainingTime)
				}
				desiredReplicaCount = rolloutReplicas
			}
		}
	}

	return annotationedRSs, desiredReplicaCount, nil
}

// GetAllReplicaSetsAndSyncRevision returns all the replica sets for the provided rollout (new and all old), with new RS's and rollout's revision updated.
//
// 1. Get all old RSes this rollout targets, and calculate the max revision number among them (maxOldV).
// 2. Get new RS this rollout targets (whose pod template matches rollout's), and update new RS's revision number to (maxOldV + 1),
//    only if its revision number is smaller than (maxOldV + 1). If this step failed, we'll update it in the next rollout sync loop.
// 3. Copy new RS's revision number to rollout (update rollout's revision). If this step failed, we'll update it in the next rollout sync loop.
// 4. If there's no existing new RS and createIfNotExisted is true, create one with appropriate revision number (maxOldRevision + 1) and replicas.
//    Note that the pod-template-hash will be added to adopted RSes and pods.
//
// Note that currently the rollout controller is using caches to avoid querying the server for reads.
// This may lead to stale reads of replica sets, thus incorrect  v status.
func (c *replicasetDeployer) GetAllReplicaSetsAndSyncRevision(createIfNotExisted bool) (*appsv1.ReplicaSet, error) {
	// Get new replica set with the updated revision number
	newRS, err := c.syncReplicaSetRevision()
	if err != nil {
		return nil, err
	}
	if newRS == nil && createIfNotExisted {
		newRS, err = c.createDesiredReplicaSet()
		if err != nil {
			return nil, err
		}
	}
	return newRS, nil
}

func (d *replicasetDeployer) rollout() *v1alpha1.Rollout {
	return d.GetRollout()
}

func (d *replicasetDeployer) newRS() *appsv1.ReplicaSet {
	return d.GetNewRS()
}

func (d *replicasetDeployer) stableRS() *appsv1.ReplicaSet {
	return d.GetStableRS()
}

func (d *replicasetDeployer) otherRSs() []*appsv1.ReplicaSet {
	return d.GetOtherRSs()
}

// Returns a replica set that matches the intent of the given rollout. Returns nil if the new replica set doesn't exist yet.
// 1. Get existing new RS (the RS that the given rollout targets, whose pod template is the same as rollout's).
// 2. If there's existing new RS, update its revision number if it's smaller than (maxOldRevision + 1), where maxOldRevision is the max revision number among all old RSes.
func (c *replicasetDeployer) syncReplicaSetRevision() (*appsv1.ReplicaSet, error) {
	if c.newRS() == nil {
		return nil, nil
	}
	ctx := context.TODO()

	// Calculate the max revision number among all old RSes
	maxOldRevision := replicasetutil.MaxRevision(c.olderRSs)
	// Calculate revision number for this new replica set
	newRevision := strconv.FormatInt(maxOldRevision+1, 10)

	// Latest replica set exists. We need to sync its annotations (includes copying all but
	// annotationsToSkip from the parent rollout, and update revision and desiredReplicas)
	// and also update the revision annotation in the rollout with the
	// latest revision.
	rsCopy := c.newRS().DeepCopy()

	// Set existing new replica set's annotation
	annotationsUpdated := annotations.SetNewReplicaSetAnnotations(c.rollout(), rsCopy, newRevision, true)
	minReadySecondsNeedsUpdate := rsCopy.Spec.MinReadySeconds != c.rollout().Spec.MinReadySeconds
	affinityNeedsUpdate := replicasetutil.IfInjectedAntiAffinityRuleNeedsUpdate(rsCopy.Spec.Template.Spec.Affinity, *c.rollout())

	if annotationsUpdated || minReadySecondsNeedsUpdate || affinityNeedsUpdate {
		rsCopy.Spec.MinReadySeconds = c.rollout().Spec.MinReadySeconds
		rsCopy.Spec.Template.Spec.Affinity = replicasetutil.GenerateReplicaSetAffinity(*c.rollout())
		return c.kubeclientset.AppsV1().ReplicaSets(rsCopy.ObjectMeta.Namespace).Update(ctx, rsCopy, metav1.UpdateOptions{})
	}

	// Should use the revision in existingNewRS's annotation, since it set by before
	if err := c.setRolloutRevision(rsCopy.Annotations[annotations.RevisionAnnotation]); err != nil {
		return nil, err
	}

	// If no other Progressing condition has been recorded and we need to estimate the progress
	// of this rollout then it is likely that old users started caring about progress. In that
	// case we need to take into account the first time we noticed their new replica set.
	cond := conditions.GetRolloutCondition(c.rollout().Status, v1alpha1.RolloutProgressing)
	if cond == nil {
		msg := fmt.Sprintf(conditions.FoundNewRSMessage, rsCopy.Name)
		condition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionTrue, conditions.FoundNewRSReason, msg)
		conditions.SetRolloutCondition(&c.rollout().Status, *condition)
		updatedRollout, err := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(c.rollout().Namespace).UpdateStatus(ctx, c.rollout(), metav1.UpdateOptions{})
		if err != nil {
			c.log.WithError(err).Error("Error: updating rollout revision")
			return nil, err
		}
		c.SetRollout(updatedRollout)
		c.SetNewRollout(updatedRollout)
		c.log.Infof("Initialized Progressing condition: %v", condition)
	}
	return rsCopy, nil
}

func (c *replicasetDeployer) createDesiredReplicaSet() (*appsv1.ReplicaSet, error) {
	ctx := context.TODO()
	// Calculate the max revision number among all old RSes
	maxOldRevision := replicasetutil.MaxRevision(c.olderRSs)
	// Calculate revision number for this new replica set
	newRevision := strconv.FormatInt(maxOldRevision+1, 10)

	// new ReplicaSet does not exist, create one.
	newRSTemplate := *c.rollout().Spec.Template.DeepCopy()
	// Add default anti-affinity rule if antiAffinity bool set and RSTemplate meets requirements
	newRSTemplate.Spec.Affinity = replicasetutil.GenerateReplicaSetAffinity(*c.rollout())
	podTemplateSpecHash := controller.ComputeHash(&c.rollout().Spec.Template, c.rollout().Status.CollisionCount)
	newRSTemplate.Labels = labelsutil.CloneAndAddLabel(c.rollout().Spec.Template.Labels, v1alpha1.DefaultRolloutUniqueLabelKey, podTemplateSpecHash)
	// Add podTemplateHash label to selector.
	newRSSelector := labelsutil.CloneSelectorAndAddLabel(c.rollout().Spec.Selector, v1alpha1.DefaultRolloutUniqueLabelKey, podTemplateSpecHash)

	// Create new ReplicaSet
	newRS := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.rollout().Name + "-" + podTemplateSpecHash,
			Namespace:       c.rollout().Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(c.rollout(), controllerKind)},
			Labels:          newRSTemplate.Labels,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas:        new(int32),
			MinReadySeconds: c.rollout().Spec.MinReadySeconds,
			Selector:        newRSSelector,
			Template:        newRSTemplate,
		},
	}
	newRS.Spec.Replicas = pointer.Int32Ptr(0)
	// Set new replica set's annotation
	annotations.SetNewReplicaSetAnnotations(c.rollout(), newRS, newRevision, false)

	if c.rollout().Spec.Strategy.Canary != nil || c.rollout().Spec.Strategy.BlueGreen != nil {
		var ephemeralMetadata *v1alpha1.PodTemplateMetadata
		if c.stableRS() != nil && c.stableRS() != c.newRS() {
			// If this is a canary rollout, with ephemeral *canary* metadata, and there is a stable RS,
			// then inject the canary metadata so that all the RS's new pods get the canary labels/annotation
			if c.rollout().Spec.Strategy.Canary != nil {
				ephemeralMetadata = c.rollout().Spec.Strategy.Canary.CanaryMetadata
			} else {
				ephemeralMetadata = c.rollout().Spec.Strategy.BlueGreen.PreviewMetadata
			}
		} else {
			// Otherwise, if stableRS is nil, we are in a brand-new rollout and then this replicaset
			// will eventually become the stableRS, so we should inject the stable labels/annotation
			if c.rollout().Spec.Strategy.Canary != nil {
				ephemeralMetadata = c.rollout().Spec.Strategy.Canary.StableMetadata
			} else {
				ephemeralMetadata = c.rollout().Spec.Strategy.BlueGreen.ActiveMetadata
			}
		}
		newRS, _ = replicasetutil.SyncReplicaSetEphemeralPodMetadata(newRS, ephemeralMetadata)
	}

	// Create the new ReplicaSet. If it already exists, then we need to check for possible
	// hash collisions. If there is any other error, we need to report it in the status of
	// the Rollout.
	alreadyExists := false
	createdRS, err := c.kubeclientset.AppsV1().ReplicaSets(c.rollout().Namespace).Create(ctx, newRS, metav1.CreateOptions{})
	switch {
	// We may end up hitting this due to a slow cache or a fast resync of the Rollout.
	case errors.IsAlreadyExists(err):
		alreadyExists = true

		// Fetch a copy of the ReplicaSet.
		rs, rsErr := c.replicaSetLister.ReplicaSets(newRS.Namespace).Get(newRS.Name)
		if rsErr != nil {
			return nil, rsErr
		}

		// If the Rollout owns the ReplicaSet and the ReplicaSet's PodTemplateSpec is semantically
		// deep equal to the PodTemplateSpec of the Rollout, it's the Rollout's new ReplicaSet.
		// Otherwise, this is a hash collision and we need to increment the collisionCount field in
		// the status of the Rollout and requeue to try the creation in the next sync.
		controllerRef := metav1.GetControllerOf(rs)
		if controllerRef != nil && controllerRef.UID == c.rollout().UID && replicasetutil.PodTemplateEqualIgnoreHash(&rs.Spec.Template, &c.rollout().Spec.Template) {
			createdRS = rs
			err = nil
			break
		}

		// Matching ReplicaSet is not equal - increment the collisionCount in the RolloutStatus
		// and requeue the Rollout.
		if c.rollout().Status.CollisionCount == nil {
			c.rollout().Status.CollisionCount = new(int32)
		}
		preCollisionCount := *c.rollout().Status.CollisionCount
		*c.rollout().Status.CollisionCount++
		// Update the collisionCount for the Rollout and let it requeue by returning the original
		// error.
		_, roErr := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(c.rollout().Namespace).UpdateStatus(ctx, c.rollout(), metav1.UpdateOptions{})
		if roErr == nil {
			c.log.Warnf("Found a hash collision - bumped collisionCount (%d->%d) to resolve it", preCollisionCount, *c.rollout().Status.CollisionCount)
		}
		return nil, err
	case err != nil:
		msg := fmt.Sprintf(conditions.FailedRSCreateMessage, newRS.Name, err)
		c.recorder.Warnf(c.rollout(), record.EventOptions{EventReason: conditions.FailedRSCreateReason}, msg)
		newStatus := c.rollout().Status.DeepCopy()
		cond := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionFalse, conditions.FailedRSCreateReason, msg)
		patchErr := c.patchCondition(c.rollout(), newStatus, cond)
		if patchErr != nil {
			c.log.Warnf("Error Patching Rollout: %s", patchErr.Error())
		}
		return nil, err
	default:
		c.log.Infof("Created ReplicaSet %s", createdRS.Name)
	}

	if err := c.setRolloutRevision(newRevision); err != nil {
		return nil, err
	}

	if !alreadyExists {
		revision, _ := replicasetutil.Revision(createdRS)
		c.recorder.Eventf(c.rollout(), record.EventOptions{EventReason: conditions.NewReplicaSetReason}, conditions.NewReplicaSetDetailedMessage, createdRS.Name, revision)

		msg := fmt.Sprintf(conditions.NewReplicaSetMessage, createdRS.Name)
		condition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionTrue, conditions.NewReplicaSetReason, msg)
		conditions.SetRolloutCondition(&c.rollout().Status, *condition)
		updatedRollout, err := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(c.rollout().Namespace).UpdateStatus(ctx, c.rollout(), metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
		c.SetRollout(updatedRollout.DeepCopy())
		if err := c.refResolver.Resolve(c.rollout()); err != nil {
			return nil, err
		}
		c.SetNewRollout(updatedRollout)
		c.log.Infof("Set rollout condition: %v", condition)
	}
	return createdRS, err
}

// canary

func (d *replicasetDeployer) GetStableHash() string {
	var stableHash string
	if d.stableRS() != nil {
		stableHash = d.stableRS().Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	}
	return stableHash
}

func (d *replicasetDeployer) GetCanaryHash() string {
	var canaryHash string
	if d.newRS() != nil {
		canaryHash = d.newRS().Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	}
	return canaryHash
}

func (c *replicasetDeployer) ReconcileCanary() (bool, error) {
	err := c.RemoveScaleDownDeadlines()
	if err != nil {
		return false, err
	}
	scaledStableRS, err := c.reconcileCanaryStableReplicaSet()
	if err != nil {
		return false, err
	}
	if scaledStableRS {
		c.log.Infof("Not finished reconciling stableRS")
		return true, nil
	}

	scaledNewRS, err := c.ReconcileNewReplicaSet()
	if err != nil {
		return false, err
	}
	if scaledNewRS {
		c.log.Infof("Not finished reconciling new ReplicaSet '%s'", c.newRS().Name)
		return true, nil
	}

	scaledDown, err := c.reconcileOthersForCanary()
	if err != nil {
		return false, err
	}
	if scaledDown {
		c.log.Info("Not finished reconciling old ReplicaSets")
		return true, nil
	}
	return false, nil
}

func (c *replicasetDeployer) reconcileCanaryStableReplicaSet() (bool, error) {
	if !replicasetutil.CheckStableRSExists(c.newRS(), c.stableRS()) {
		// we skip this because if they are equal, then it will get reconciled in reconcileNewReplicaSet()
		// making this redundant
		c.log.Info("No StableRS exists to reconcile or matches newRS")
		return false, nil
	}
	_, stableRSReplicaCount := replicasetutil.CalculateReplicaCountsForCanary(c.rollout(), c.newRS(), c.stableRS(), c.otherRSs())
	scaled, _, err := c.ScaleReplicaSetAndRecordEvent(c.stableRS(), stableRSReplicaCount)
	return scaled, err
}

// ReconcileRevisionHistoryLimit is responsible for cleaning up a rollout ie. retains all but the latest N old replica sets
// where N=r.Spec.RevisionHistoryLimit. Old replica sets are older versions of the podtemplate of a rollout kept
// around by default 1) for historical reasons.
func (c *replicasetDeployer) ReconcileRevisionHistoryLimit() error {
	ctx := context.TODO()
	revHistoryLimit := defaults.GetRevisionHistoryLimitOrDefault(c.rollout())

	// Avoid deleting replica set with deletion timestamp set
	aliveFilter := func(rs *appsv1.ReplicaSet) bool {
		return rs != nil && rs.ObjectMeta.DeletionTimestamp == nil
	}
	cleanableRSes := controller.FilterReplicaSets(c.otherRSs(), aliveFilter)

	diff := int32(len(cleanableRSes)) - revHistoryLimit
	if diff <= 0 {
		return nil
	}
	c.log.Infof("Cleaning up %d old replicasets from revision history limit %d", len(cleanableRSes), revHistoryLimit)

	sort.Sort(controller.ReplicaSetsByCreationTimestamp(cleanableRSes))
	podHashToArList := analysisutil.SortAnalysisRunByPodHash(c.GetOtherARs())
	podHashToExList := experimentutil.SortExperimentsByPodHash(c.GetOtherExs())
	c.log.Info("Looking to cleanup old replica sets")
	for i := int32(0); i < diff; i++ {
		rs := cleanableRSes[i]
		// Avoid delete replica set with non-zero replica counts
		if rs.Status.Replicas != 0 || *(rs.Spec.Replicas) != 0 || rs.Generation > rs.Status.ObservedGeneration || rs.DeletionTimestamp != nil {
			continue
		}
		c.log.Infof("Trying to cleanup replica set %q", rs.Name)
		if err := c.kubeclientset.AppsV1().ReplicaSets(rs.Namespace).Delete(ctx, rs.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			// Return error instead of aggregating and continuing DELETEs on the theory
			// that we may be overloading the api server.
			return err
		}
		if podHash, ok := rs.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]; ok {
			if ars, ok := podHashToArList[podHash]; ok {
				c.log.Infof("Cleaning up associated analysis runs with ReplicaSet '%s'", rs.Name)
				err := c.deleteAnalysisRuns(ars)
				if err != nil {
					return err
				}
			}
			if exs, ok := podHashToExList[podHash]; ok {
				c.log.Infof("Cleaning up associated experiments with ReplicaSet '%s'", rs.Name)
				err := c.deleteExperiments(exs)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (c *replicasetDeployer) ReconcileBlueGreen(activeSvc *corev1.Service) error {
	err := c.RemoveScaleDownDeadlines()
	if err != nil {
		return err
	}
	err = c.reconcileBlueGreenStableReplicaSet(activeSvc)
	if err != nil {
		return err
	}
	_, err = c.ReconcileNewReplicaSet()
	if err != nil {
		return err
	}
	// Scale down old non-active, non-stable replicasets, if we can.
	_, err = c.ReconcileOthersForBlueGreen()
	if err != nil {
		return err
	}
	if err := c.ReconcileRevisionHistoryLimit(); err != nil {
		return err
	}
	return nil
}

func (c *replicasetDeployer) reconcileBlueGreenStableReplicaSet(activeSvc *corev1.Service) error {
	if _, ok := activeSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]; !ok {
		return nil
	}
	activeRS, _ := replicasetutil.GetReplicaSetByTemplateHash(c.allRSs, activeSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey])
	if activeRS == nil {
		c.log.Warn("There shouldn't be a nil active replicaset if the active Service selector is set")
		return nil
	}

	c.log.Infof("Reconciling stable ReplicaSet '%s'", activeRS.Name)
	_, _, err := c.ScaleReplicaSetAndRecordEvent(activeRS, defaults.GetReplicasOrDefault(c.rollout().Spec.Replicas))
	return err
}

func (c *replicasetDeployer) AtDesiredReplicaCountsForCanary() bool {
	return replicasetutil.AtDesiredReplicaCountsForCanary(c.rollout(), c.newRS(), c.stableRS(), c.otherRSs())
}

func (c *replicasetDeployer) GetNewName() string {
	rsName := ""
	if c.newRS() != nil {
		rsName = c.newRS().Name
	}
	return rsName
}

func (c *replicasetDeployer) updateStatus(newStatus *v1alpha1.RolloutStatus) {
	newStatus.Replicas = replicasetutil.GetActualReplicaCountForReplicaSets(c.allRSs)
	newStatus.UpdatedReplicas = replicasetutil.GetActualReplicaCountForReplicaSets([]*appsv1.ReplicaSet{c.newRS()})
	newStatus.ReadyReplicas = replicasetutil.GetReadyReplicaCountForReplicaSets(c.allRSs)
}

// calculateStatus calculates the common fields for all rollouts by looking into the provided replica sets.
func (c *replicasetDeployer) calculateBaseStatus() v1alpha1.RolloutStatus {
	prevStatus := c.rollout().Status

	prevCond := conditions.GetRolloutCondition(prevStatus, v1alpha1.InvalidSpec)
	err := c.GetRolloutValidationErrors()
	if err == nil && prevCond != nil {
		conditions.RemoveRolloutCondition(&prevStatus, v1alpha1.InvalidSpec)
	}

	var currentPodHash string
	if c.newRS() == nil {
		// newRS potentially might be nil when called by syncReplicasOnly(). For this
		// to happen, the user would have had to simultaneously change the number of replicas, and
		// the pod template spec at the same time.
		currentPodHash = controller.ComputeHash(&c.rollout().Spec.Template, c.rollout().Status.CollisionCount)
		c.log.Infof("Assuming %s for new replicaset pod hash", currentPodHash)
	} else {
		currentPodHash = c.newRS().Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	}

	newStatus := c.GetNewStatus()
	newStatus.CurrentPodHash = currentPodHash
	newStatus.CollisionCount = c.rollout().Status.CollisionCount
	newStatus.Conditions = prevStatus.Conditions
	newStatus.RestartedAt = c.GetNewStatus().RestartedAt
	newStatus.PromoteFull = (newStatus.CurrentPodHash != newStatus.StableRS) && prevStatus.PromoteFull

	c.updateStatus(&newStatus)

	return newStatus
}

package rollout

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	patchtypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
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
	ReconcileNewReplicaSet() (bool, error)
	ReconcileOtherReplicaSets(scaleDownOldReplicaSets func([]*appsv1.ReplicaSet) (bool, error)) (bool, error)
	CleanupUnhealthyReplicas(oldRSs []*appsv1.ReplicaSet) ([]*appsv1.ReplicaSet, int32, error)
	ScaleReplicaSetAndRecordEvent(rs *appsv1.ReplicaSet, newScale int32) (bool, *appsv1.ReplicaSet, error)
	ScaleDownDelayHelper(rs *appsv1.ReplicaSet, annotationedRSs int32, rolloutReplicas int32) (int32, int32, error)
}

type replicasetDeployer struct {
	kubeclientset   kubernetes.Interface
	log             *log.Entry
	newRS, stableRS *appsv1.ReplicaSet
	allRSs          []*appsv1.ReplicaSet
	otherRSs        []*appsv1.ReplicaSet
	rollout         *v1alpha1.Rollout
	recorder        record.EventRecorder
	pauseContext    *pauseContext
	resyncPeriod    time.Duration

	enqueueRolloutAfter func(obj interface{}, duration time.Duration) //nolint:structcheck
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
	if d.newRS != nil && !d.shouldDelayScaleDownOnAbort() {
		toRemove = append(toRemove, d.newRS)
	}
	if d.stableRS != nil {
		if len(toRemove) == 0 || d.stableRS.Name != d.newRS.Name {
			toRemove = append(toRemove, d.stableRS)
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
	if d.newRS == nil {
		return false, nil
	}
	newReplicasCount, err := replicasetutil.NewRSNewReplicas(d.rollout, d.allRSs, d.newRS)
	if err != nil {
		return false, err
	}

	if d.shouldDelayScaleDownOnAbort() {
		abortScaleDownDelaySeconds := defaults.GetAbortScaleDownDelaySecondsOrDefault(d.rollout)
		d.log.Infof("Scale down new rs '%s' on abort (%v)", d.newRS.Name, abortScaleDownDelaySeconds)

		// if the newRS has scale down annotation, check if it should be scaled down now
		if scaleDownAtStr, ok := d.newRS.Annotations[v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey]; ok {
			d.log.Infof("New rs '%s' has scaledown deadline annotation: %s", d.newRS.Name, scaleDownAtStr)
			scaleDownAtTime, err := time.Parse(time.RFC3339, scaleDownAtStr)
			if err != nil {
				d.log.Warnf("Unable to read scaleDownAt label on rs '%s'", d.newRS.Name)
			} else {
				now := metav1.Now()
				scaleDownAt := metav1.NewTime(scaleDownAtTime)
				if scaleDownAt.After(now.Time) {
					d.log.Infof("RS '%s' has not reached the scaleDownTime", d.newRS.Name)
					remainingTime := scaleDownAt.Sub(now.Time)
					if remainingTime < d.resyncPeriod {
						d.enqueueRolloutAfter(d.rollout, remainingTime)
						return false, nil
					}
				} else {
					d.log.Infof("RS '%s' has reached the scaleDownTime", d.newRS.Name)
					newReplicasCount = int32(0)
				}
			}
		} else if abortScaleDownDelaySeconds != nil {
			err = d.addScaleDownDelay(d.newRS, *abortScaleDownDelaySeconds)
			if err != nil {
				return false, err
			}
		}
	}

	scaled, _, err := d.ScaleReplicaSetAndRecordEvent(d.newRS, newReplicasCount)
	return scaled, err
}

// shouldDelayScaleDownOnAbort returns if we are aborted and we should delay scaledown of canary/preview
func (d *replicasetDeployer) shouldDelayScaleDownOnAbort() bool {
	return d.pauseContext.IsAborted() && defaults.GetAbortScaleDownDelaySecondsOrDefault(d.rollout) != nil
}

// ReconcileOtherReplicaSets reconciles "other" ReplicaSets.
// Other ReplicaSets are ReplicaSets are neither the new or stable (allRSs - newRS - stableRS)
func (d *replicasetDeployer) ReconcileOtherReplicaSets(scaleDownOldReplicaSets func([]*appsv1.ReplicaSet) (bool, error)) (bool, error) {
	otherRSs := controller.FilterActiveReplicaSets(d.otherRSs)
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

// CleanupUnhealthyReplicas will scale down old replica sets with unhealthy replicas, so that all unhealthy replicas will be deleted.
func (d *replicasetDeployer) CleanupUnhealthyReplicas(oldRSs []*appsv1.ReplicaSet) ([]*appsv1.ReplicaSet, int32, error) {
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
	if *(rs.Spec.Replicas) == newScale && !annotations.ReplicasAnnotationsNeedUpdate(rs, defaults.GetReplicasOrDefault(d.rollout.Spec.Replicas)) {
		return false, rs, nil
	}
	var scalingOperation string
	if *(rs.Spec.Replicas) < newScale {
		scalingOperation = "up"
	} else {
		scalingOperation = "down"
	}
	scaled, newRS, err := d.scaleReplicaSet(rs, newScale, d.rollout, scalingOperation)
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

func (d *replicasetDeployer) ScaleDownDelayHelper(rs *appsv1.ReplicaSet, annotationedRSs int32, rolloutReplicas int32) (int32, int32, error) {
	desiredReplicaCount := int32(0)
	scaleDownRevisionLimit := GetScaleDownRevisionLimit(d.rollout)
	if !replicasetutil.HasScaleDownDeadline(rs) && *rs.Spec.Replicas > 0 {
		// This ReplicaSet is scaled up but does not have a scale down deadline. Add one.
		if annotationedRSs < scaleDownRevisionLimit {
			annotationedRSs++
			desiredReplicaCount = *rs.Spec.Replicas
			scaleDownDelaySeconds := defaults.GetScaleDownDelaySecondsOrDefault(d.rollout)
			err := d.addScaleDownDelay(rs, scaleDownDelaySeconds)
			if err != nil {
				return annotationedRSs, desiredReplicaCount, err
			}
			d.enqueueRolloutAfter(d.rollout, scaleDownDelaySeconds)
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
					d.enqueueRolloutAfter(d.rollout, *remainingTime)
				}
				desiredReplicaCount = rolloutReplicas
			}
		}
	}

	return annotationedRSs, desiredReplicaCount, nil
}

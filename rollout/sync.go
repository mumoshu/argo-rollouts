package rollout

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	patchtypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/annotations"
	"github.com/argoproj/argo-rollouts/utils/conditions"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	"github.com/argoproj/argo-rollouts/utils/diff"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
	"github.com/argoproj/argo-rollouts/utils/record"
	replicasetutil "github.com/argoproj/argo-rollouts/utils/replicaset"
	rolloututil "github.com/argoproj/argo-rollouts/utils/rollout"
)

func (c *rolloutContext) setRolloutRevision(revision string) error {
	if annotations.SetRolloutRevision(c.rollout, revision) {
		updatedRollout, err := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(c.rollout.Namespace).Update(context.TODO(), c.rollout, metav1.UpdateOptions{})
		if err != nil {
			c.log.WithError(err).Error("Error: updating rollout revision")
			return err
		}
		c.rollout = updatedRollout.DeepCopy()
		if err := c.refResolver.Resolve(c.rollout); err != nil {
			return err
		}
		c.newRollout = updatedRollout
		c.recorder.Eventf(c.rollout, record.EventOptions{EventReason: conditions.RolloutUpdatedReason}, conditions.RolloutUpdatedMessage, revision)
	}
	return nil
}

// syncReplicasOnly is responsible for reconciling rollouts on scaling events.
func (c *rolloutContext) syncReplicasOnly(isScaling bool) error {
	c.Deployer = c.newDeployer()

	c.log.Infof("Syncing replicas only (userPaused %v, isScaling: %v)", c.rollout.Spec.Paused, isScaling)
	_, err := c.GetAllReplicaSetsAndSyncRevision(false)
	if err != nil {
		return err
	}

	// NOTE: it is possible for newRS to be nil (e.g. when template and replicas changed at same time)
	if c.rollout.Spec.Strategy.BlueGreen != nil {
		previewSvc, activeSvc, err := c.getPreviewAndActiveServices()
		// Keep existing analysis runs if the rollout is paused
		c.SetCurrentAnalysisRuns(c.currentArs)
		if err != nil {
			return nil
		}
		err = c.podRestarter.Reconcile(c)
		if err != nil {
			return err
		}
		if err := c.ReconcileBlueGreen(activeSvc); err != nil {
			// If we get an error while trying to scale, the rollout will be requeued
			// so we can abort this resync
			return err
		}
		return c.syncRolloutStatusBlueGreen(previewSvc, activeSvc)
	}
	// The controller wants to use the rolloutCanary method to reconcile the rollout if the rollout is not paused.
	// If there are no scaling events, the rollout should only sync its status
	if c.rollout.Spec.Strategy.Canary != nil {
		err = c.podRestarter.Reconcile(c)
		if err != nil {
			return err
		}

		if isScaling {
			if _, err := c.ReconcileCanary(); err != nil {
				// If we get an error while trying to scale, the rollout will be requeued
				// so we can abort this resync
				return err
			}
		}
		// Reconciling AnalysisRuns to manage Background AnalysisRun if necessary
		err = c.reconcileAnalysisRuns()
		if err != nil {
			return err
		}

		// reconcileCanaryPause will ensure we will requeue this rollout at the appropriate time
		// if we are at a pause step with a duration.
		c.reconcileCanaryPause()
		err = c.reconcileStableAndCanaryService()
		if err != nil {
			return err
		}

		err = c.reconcileTrafficRouting()
		if err != nil {
			return err
		}

		return c.syncRolloutStatusCanary()
	}
	return fmt.Errorf("no rollout strategy provided")
}

// isScalingEvent checks whether the provided rollout has been updated with a scaling event
// by looking at the desired-replicas annotation in the active replica sets of the rollout.
//
// rsList should come from getReplicaSetsForRollout(r).
func (c *rolloutContext) isScalingEvent() (bool, error) {
	c.Deployer = c.newDeployer()

	_, err := c.GetAllReplicaSetsAndSyncRevision(false)
	if err != nil {
		return false, err
	}

	for _, rs := range controller.FilterActiveReplicaSets(c.allRSs) {
		desired, ok := annotations.GetDesiredReplicasAnnotation(rs)
		if !ok {
			continue
		}
		if desired != defaults.GetReplicasOrDefault(c.rollout.Spec.Replicas) {
			return true, nil
		}
	}
	return false, nil
}

// calculateStatus calculates the common fields for all rollouts by looking into the provided replica sets.
func (c *rolloutContext) calculateBaseStatus() v1alpha1.RolloutStatus {
	prevStatus := c.rollout.Status

	prevCond := conditions.GetRolloutCondition(prevStatus, v1alpha1.InvalidSpec)
	err := c.getRolloutValidationErrors()
	if err == nil && prevCond != nil {
		conditions.RemoveRolloutCondition(&prevStatus, v1alpha1.InvalidSpec)
	}

	var currentPodHash string
	if c.newRS == nil {
		// newRS potentially might be nil when called by syncReplicasOnly(). For this
		// to happen, the user would have had to simultaneously change the number of replicas, and
		// the pod template spec at the same time.
		currentPodHash = controller.ComputeHash(&c.rollout.Spec.Template, c.rollout.Status.CollisionCount)
		c.log.Infof("Assuming %s for new replicaset pod hash", currentPodHash)
	} else {
		currentPodHash = c.newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	}

	newStatus := c.newStatus
	newStatus.CurrentPodHash = currentPodHash
	newStatus.Replicas = replicasetutil.GetActualReplicaCountForReplicaSets(c.allRSs)
	newStatus.UpdatedReplicas = replicasetutil.GetActualReplicaCountForReplicaSets([]*appsv1.ReplicaSet{c.newRS})
	newStatus.ReadyReplicas = replicasetutil.GetReadyReplicaCountForReplicaSets(c.allRSs)
	newStatus.CollisionCount = c.rollout.Status.CollisionCount
	newStatus.Conditions = prevStatus.Conditions
	newStatus.RestartedAt = c.newStatus.RestartedAt
	newStatus.PromoteFull = (newStatus.CurrentPodHash != newStatus.StableRS) && prevStatus.PromoteFull
	return newStatus
}

// checkPausedConditions checks if the given rollout is paused or not and adds an appropriate condition.
// These conditions are needed so that we won't accidentally report lack of progress for resumed rollouts
// that were paused for longer than progressDeadlineSeconds.
func (c *rolloutContext) checkPausedConditions() error {
	// Progressing condition
	progCond := conditions.GetRolloutCondition(c.rollout.Status, v1alpha1.RolloutProgressing)
	progCondPaused := progCond != nil && progCond.Reason == conditions.RolloutPausedReason

	isPaused := len(c.rollout.Status.PauseConditions) > 0 || c.rollout.Spec.Paused
	abortCondExists := progCond != nil && progCond.Reason == conditions.RolloutAbortedReason

	var updatedConditions []*v1alpha1.RolloutCondition

	if (isPaused != progCondPaused) && !abortCondExists {
		if isPaused {
			updatedConditions = append(updatedConditions, conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionUnknown, conditions.RolloutPausedReason, conditions.RolloutPausedMessage))
		} else {
			updatedConditions = append(updatedConditions, conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionUnknown, conditions.RolloutResumedReason, conditions.RolloutResumedMessage))
		}
	}

	if !c.rollout.Status.Abort && abortCondExists {
		updatedConditions = append(updatedConditions, conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionUnknown, conditions.RolloutRetryReason, conditions.RolloutRetryMessage))
	}

	pauseCond := conditions.GetRolloutCondition(c.rollout.Status, v1alpha1.RolloutPaused)
	pausedCondTrue := pauseCond != nil && pauseCond.Status == corev1.ConditionTrue

	if (isPaused != pausedCondTrue) && !abortCondExists {
		condStatus := corev1.ConditionFalse
		if isPaused {
			condStatus = corev1.ConditionTrue
		}
		updatedConditions = append(updatedConditions, conditions.NewRolloutCondition(v1alpha1.RolloutPaused, condStatus, conditions.RolloutPausedReason, conditions.RolloutPausedMessage))
	}

	if len(updatedConditions) == 0 {
		return nil
	}

	newStatus := c.rollout.Status.DeepCopy()
	err := c.patchCondition(c.rollout, newStatus, updatedConditions...)
	return err
}

func (c *rolloutContext) patchCondition(r *v1alpha1.Rollout, newStatus *v1alpha1.RolloutStatus, conditionList ...*v1alpha1.RolloutCondition) error {
	ctx := context.TODO()
	for _, condition := range conditionList {
		conditions.SetRolloutCondition(newStatus, *condition)
	}
	newStatus.ObservedGeneration = strconv.Itoa(int(c.rollout.Generation))
	newStatus.Phase, newStatus.Message = rolloututil.CalculateRolloutPhase(r.Spec, *newStatus)

	logCtx := logutil.WithVersionFields(c.log, r)
	patch, modified, err := diff.CreateTwoWayMergePatch(
		&v1alpha1.Rollout{
			Status: r.Status,
		},
		&v1alpha1.Rollout{
			Status: *newStatus,
		}, v1alpha1.Rollout{})
	if err != nil {
		logCtx.Errorf("Error constructing app status patch: %v", err)
		return err
	}
	if !modified {
		logCtx.Info("No status changes. Skipping patch")
		return nil
	}
	newRollout, err := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(r.Namespace).Patch(ctx, r.Name, patchtypes.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if err != nil {
		logCtx.Warnf("Error patching rollout: %v", err)
		return err
	}
	logCtx.Infof("Patched conditions: %s", string(patch))
	c.newRollout = newRollout
	return nil
}

// isIndefiniteStep returns whether or not the rollout is at an Experiment or Analysis or Pause step which should
// not affect the progressDeadlineSeconds
func isIndefiniteStep(r *v1alpha1.Rollout) bool {
	currentStep, _ := replicasetutil.GetCurrentCanaryStep(r)
	if currentStep != nil && (currentStep.Experiment != nil || currentStep.Analysis != nil || currentStep.Pause != nil) {
		return true
	}
	return false
}

func (c *rolloutContext) calculateRolloutConditions(newStatus v1alpha1.RolloutStatus) v1alpha1.RolloutStatus {
	isPaused := len(c.rollout.Status.PauseConditions) > 0 || c.rollout.Spec.Paused
	isAborted := c.pauseContext.IsAborted()

	completeCond := conditions.GetRolloutCondition(c.rollout.Status, v1alpha1.RolloutCompleted)
	if !isPaused && conditions.RolloutComplete(c.rollout, &newStatus) {
		updateCompletedCond := conditions.NewRolloutCondition(v1alpha1.RolloutCompleted, corev1.ConditionTrue, conditions.RolloutCompletedReason, conditions.RolloutCompletedReason)
		conditions.SetRolloutCondition(&newStatus, *updateCompletedCond)
	} else {
		if completeCond != nil {
			updateCompletedCond := conditions.NewRolloutCondition(v1alpha1.RolloutCompleted, corev1.ConditionFalse, conditions.RolloutCompletedReason, conditions.RolloutCompletedReason)
			conditions.SetRolloutCondition(&newStatus, *updateCompletedCond)
		}
	}

	if isAborted {
		revision, _ := replicasetutil.Revision(c.rollout)
		message := fmt.Sprintf(conditions.RolloutAbortedMessage, revision)
		if c.pauseContext.abortMessage != "" {
			message = fmt.Sprintf("%s: %s", message, c.pauseContext.abortMessage)
		}
		condition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionFalse, conditions.RolloutAbortedReason, message)
		if conditions.SetRolloutCondition(&newStatus, *condition) {
			c.recorder.Warnf(c.rollout, record.EventOptions{EventReason: conditions.RolloutAbortedReason}, message)
		}
	}

	// If there is only one replica set that is active then that means we are not running
	// a new rollout and this is a resync where we don't need to estimate any progress.
	// In such a case, we should simply not estimate any progress for this rollout.
	currentCond := conditions.GetRolloutCondition(c.rollout.Status, v1alpha1.RolloutProgressing)

	isCompleteRollout := newStatus.Replicas == newStatus.AvailableReplicas && currentCond != nil && currentCond.Reason == conditions.NewRSAvailableReason && currentCond.Type != v1alpha1.RolloutProgressing
	// Check for progress. Only do this if the latest rollout hasn't completed yet and it is not aborted
	if !isCompleteRollout && !isAborted {
		switch {
		case conditions.RolloutComplete(c.rollout, &newStatus):
			// Update the rollout conditions with a message for the new replica set that
			// was successfully deployed. If the condition already exists, we ignore this update.
			rsName := ""
			if c.newRS != nil {
				rsName = c.newRS.Name
			}
			msg := fmt.Sprintf(conditions.ReplicaSetCompletedMessage, rsName)
			progressingCondition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionTrue, conditions.NewRSAvailableReason, msg)
			conditions.SetRolloutCondition(&newStatus, *progressingCondition)
		case conditions.RolloutProgressing(c.rollout, &newStatus):
			// If there is any progress made, continue by not checking if the rollout failed. This
			// behavior emulates the rolling updater progressDeadline check.
			msg := fmt.Sprintf(conditions.RolloutProgressingMessage, c.rollout.Name)
			if c.newRS != nil {
				msg = fmt.Sprintf(conditions.ReplicaSetProgressingMessage, c.newRS.Name)
			}
			condition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionTrue, conditions.ReplicaSetUpdatedReason, msg)
			// Update the current Progressing condition or add a new one if it doesn't exist.
			// If a Progressing condition with status=true already exists, we should update
			// everything but lastTransitionTime. SetRolloutCondition already does that but
			// it also is not updating conditions when the reason of the new condition is the
			// same as the old. The Progressing condition is a special case because we want to
			// update with the same reason and change just lastUpdateTime iff we notice any
			// progress. That's why we handle it here.
			if currentCond != nil {
				if currentCond.Status == corev1.ConditionTrue {
					condition.LastTransitionTime = currentCond.LastTransitionTime
				}
				conditions.RemoveRolloutCondition(&newStatus, v1alpha1.RolloutProgressing)
			}
			conditions.SetRolloutCondition(&newStatus, *condition)
		case !isIndefiniteStep(c.rollout) && conditions.RolloutTimedOut(c.rollout, &newStatus):
			// Update the rollout with a timeout condition. If the condition already exists,
			// we ignore this update.
			msg := fmt.Sprintf(conditions.RolloutTimeOutMessage, c.rollout.Name)
			if c.newRS != nil {
				msg = fmt.Sprintf(conditions.ReplicaSetTimeOutMessage, c.newRS.Name)
			}

			condition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionFalse, conditions.TimedOutReason, msg)
			condChanged := conditions.SetRolloutCondition(&newStatus, *condition)

			// If condition is changed and ProgressDeadlineAbort is set, abort the update
			if condChanged {
				if c.rollout.Spec.ProgressDeadlineAbort {
					c.pauseContext.AddAbort(msg)
					c.recorder.Warnf(c.rollout, record.EventOptions{EventReason: conditions.RolloutAbortedReason}, msg)
				}
			} else {
				// Although condition is unchanged, ProgressDeadlineAbort can be set after
				// an existing update timeout. In this case if update is not aborted, we need to abort.
				if c.rollout.Spec.ProgressDeadlineAbort && c.pauseContext != nil && !c.pauseContext.IsAborted() {
					c.pauseContext.AddAbort(msg)
					c.recorder.Warnf(c.rollout, record.EventOptions{EventReason: conditions.RolloutAbortedReason}, msg)
				}
			}
		}
	}

	activeRS, _ := replicasetutil.GetReplicaSetByTemplateHash(c.allRSs, newStatus.BlueGreen.ActiveSelector)
	if c.rollout.Spec.Strategy.BlueGreen != nil && activeRS != nil && annotations.IsSaturated(c.rollout, activeRS) {
		availability := conditions.NewRolloutCondition(v1alpha1.RolloutAvailable, corev1.ConditionTrue, conditions.AvailableReason, conditions.AvailableMessage)
		conditions.SetRolloutCondition(&newStatus, *availability)
	} else if c.rollout.Spec.Strategy.Canary != nil && replicasetutil.GetAvailableReplicaCountForReplicaSets(c.allRSs) >= defaults.GetReplicasOrDefault(c.rollout.Spec.Replicas) {
		availability := conditions.NewRolloutCondition(v1alpha1.RolloutAvailable, corev1.ConditionTrue, conditions.AvailableReason, conditions.AvailableMessage)
		conditions.SetRolloutCondition(&newStatus, *availability)
	} else {
		noAvailability := conditions.NewRolloutCondition(v1alpha1.RolloutAvailable, corev1.ConditionFalse, conditions.AvailableReason, conditions.NotAvailableMessage)
		conditions.SetRolloutCondition(&newStatus, *noAvailability)
	}

	// Move failure conditions of all replica sets in rollout conditions. For now,
	// only one failure condition is returned from getReplicaFailures.
	if replicaFailureCond := c.getReplicaFailures(c.allRSs, c.newRS); len(replicaFailureCond) > 0 {
		// There will be only one ReplicaFailure condition on the replica set.
		conditions.SetRolloutCondition(&newStatus, replicaFailureCond[0])
	} else {
		conditions.RemoveRolloutCondition(&newStatus, v1alpha1.RolloutReplicaFailure)
	}
	return newStatus
}

// persistRolloutStatus persists updates to rollout status. If no changes were made, it is a no-op
func (c *rolloutContext) persistRolloutStatus(newStatus *v1alpha1.RolloutStatus) error {
	ctx := context.TODO()
	logCtx := logutil.WithVersionFields(c.log, c.rollout)

	prevStatus := c.rollout.Status
	c.pauseContext.CalculatePauseStatus(newStatus)
	if c.rollout.Spec.TemplateResolvedFromRef {
		workloadRefObservation, _ := annotations.GetWorkloadGenerationAnnotation(c.rollout)
		currentWorkloadObservedGeneration, _ := strconv.ParseInt(newStatus.WorkloadObservedGeneration, 10, 32)
		if workloadRefObservation != int32(currentWorkloadObservedGeneration) {
			newStatus.WorkloadObservedGeneration = strconv.Itoa(int(workloadRefObservation))
		}
	} else {
		newStatus.WorkloadObservedGeneration = ""
	}

	newStatus.ObservedGeneration = strconv.Itoa(int(c.rollout.Generation))
	newStatus.Phase, newStatus.Message = rolloututil.CalculateRolloutPhase(c.rollout.Spec, *newStatus)

	patch, modified, err := diff.CreateTwoWayMergePatch(
		&v1alpha1.Rollout{
			Status: prevStatus,
		},
		&v1alpha1.Rollout{
			Status: *newStatus,
		}, v1alpha1.Rollout{})
	if err != nil {
		logCtx.Errorf("Error constructing app status patch: %v", err)
		return err
	}
	if !modified {
		logCtx.Info("No status changes. Skipping patch")
		c.requeueStuckRollout(*newStatus)
		return nil
	}

	newRollout, err := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(c.rollout.Namespace).Patch(ctx, c.rollout.Name, patchtypes.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if err != nil {
		logCtx.Warningf("Error updating rollout: %v", err)
		return err
	}

	c.sendStateChangeEvents(&prevStatus, newStatus)
	logCtx.Infof("Patched: %s", patch)
	c.newRollout = newRollout
	return nil
}

// sendStateChangeEvents emit rollout events on significant state changes
func (c *rolloutContext) sendStateChangeEvents(prevStatus, newStatus *v1alpha1.RolloutStatus) {
	prevPaused := len(prevStatus.PauseConditions) > 0
	currPaused := len(newStatus.PauseConditions) > 0
	currAborted := newStatus.AbortedAt != nil
	if prevPaused != currPaused {
		if currPaused {
			c.recorder.Eventf(c.rollout, record.EventOptions{EventReason: conditions.RolloutPausedReason}, conditions.RolloutPausedMessage+fmt.Sprintf(" (%s)", newStatus.PauseConditions[0].Reason))
		} else if !currAborted {
			// we check currAborted, because an abort will also clear status.pauseConditions
			// which should not be mistaken as a RolloutResumed
			c.recorder.Eventf(c.rollout, record.EventOptions{EventReason: conditions.RolloutResumedReason}, conditions.RolloutResumedMessage)
		}
	}
}

// used for unit testing
var nowFn = func() time.Time { return time.Now() }

// requeueStuckRollout checks whether the provided rollout needs to be synced for a progress
// check. It returns the time after the rollout will be requeued for the progress check, 0 if it
// will be requeued now, or -1 if it does not need to be requeued.
func (c *rolloutContext) requeueStuckRollout(newStatus v1alpha1.RolloutStatus) time.Duration {
	currentCond := conditions.GetRolloutCondition(c.rollout.Status, v1alpha1.RolloutProgressing)
	// Can't estimate progress if there is no deadline in the spec or progressing condition in the current status.
	if currentCond == nil {
		return time.Duration(-1)
	}
	// No need to estimate progress if the rollout is complete or already timed out.
	isPaused := len(c.rollout.Status.PauseConditions) > 0 || c.rollout.Spec.Paused
	if conditions.RolloutComplete(c.rollout, &newStatus) || currentCond.Reason == conditions.TimedOutReason || isPaused || c.rollout.Status.Abort || isIndefiniteStep(c.rollout) {
		return time.Duration(-1)
	}
	// If there is no sign of progress at this point then there is a high chance that the
	// rollout is stuck. We should resync this rollout at some point in the future[1]
	// and check whether it has timed out. We definitely need this, otherwise we depend on the
	// controller resync interval. See https://github.com/kubernetes/kubernetes/issues/34458.
	//
	// [1] ProgressingCondition.LastUpdatedTime + progressDeadlineSeconds - time.Now()
	//
	// For example, if a Rollout updated its Progressing condition 3 minutes ago and has a
	// deadline of 10 minutes, it would need to be resynced for a progress check after 7 minutes.
	//
	// lastUpdated: 			00:00:00
	// now: 					00:03:00
	// progressDeadlineSeconds: 600 (10 minutes)
	//
	// lastUpdated + progressDeadlineSeconds - now => 00:00:00 + 00:10:00 - 00:03:00 => 07:00
	progressDeadlineSeconds := defaults.GetProgressDeadlineSecondsOrDefault(c.rollout)
	after := currentCond.LastUpdateTime.Time.Add(time.Duration(progressDeadlineSeconds) * time.Second).Sub(nowFn())
	// If the remaining time is less than a second, then requeue the deployment immediately.
	// Make it ratelimited so we stay on the safe side, eventually the Deployment should
	// transition either to a Complete or to a TimedOut condition.
	if after < time.Second {
		c.log.Infof("Queueing up Rollout for a progress check now")
		c.enqueueRollout(c.rollout)
		return time.Duration(0)
	}
	c.log.Infof("Queueing up rollout for a progress after %ds", int(after.Seconds()))
	// Add a second to avoid milliseconds skew in AddAfter.
	// See https://github.com/kubernetes/kubernetes/issues/39785#issuecomment-279959133 for more info.
	c.enqueueRolloutAfter(c.rollout, after+time.Second)
	return after
}

// getReplicaFailures will convert replica failure conditions from replica sets
// to rollout conditions.
func (c *rolloutContext) getReplicaFailures(allRSs []*appsv1.ReplicaSet, newRS *appsv1.ReplicaSet) []v1alpha1.RolloutCondition {
	var errorConditions []v1alpha1.RolloutCondition
	if newRS != nil {
		for _, c := range newRS.Status.Conditions {
			if c.Type != appsv1.ReplicaSetReplicaFailure {
				continue
			}
			errorConditions = append(errorConditions, conditions.ReplicaSetToRolloutCondition(c))
		}
	}

	// Return failures for the new replica set over failures from old replica sets.
	if len(errorConditions) > 0 {
		return errorConditions
	}

	for i := range allRSs {
		rs := allRSs[i]
		if rs == nil {
			continue
		}

		for _, c := range rs.Status.Conditions {
			if c.Type != appsv1.ReplicaSetReplicaFailure {
				continue
			}
			errorConditions = append(errorConditions, conditions.ReplicaSetToRolloutCondition(c))
		}
	}
	return errorConditions
}

// resetRolloutStatus will reset the rollout status as if it is in a beginning of a new update
func (c *rolloutContext) resetRolloutStatus(newStatus *v1alpha1.RolloutStatus) {
	c.pauseContext.ClearPauseConditions()
	c.pauseContext.RemoveAbort()
	c.SetRestartedAt()
	newStatus.PromoteFull = false
	newStatus.BlueGreen.PrePromotionAnalysisRunStatus = nil
	newStatus.BlueGreen.PostPromotionAnalysisRunStatus = nil
	newStatus.BlueGreen.ScaleUpPreviewCheckPoint = false
	newStatus.Canary.CurrentStepAnalysisRunStatus = nil
	newStatus.Canary.CurrentBackgroundAnalysisRunStatus = nil
	newStatus.CurrentStepIndex = replicasetutil.ResetCurrentStepIndex(c.rollout)
}

// shouldFullPromote returns a reason string explaining why a rollout should fully promote, marking
// the desired ReplicaSet as stable. Returns empty string if the rollout is in middle of update
func (c *rolloutContext) shouldFullPromote(newStatus v1alpha1.RolloutStatus) string {
	// NOTE: the order of these checks are significant
	if c.stableRS == nil {
		return "Initial deploy"
	} else if c.rollout.Spec.Strategy.Canary != nil {
		if c.pauseContext.IsAborted() {
			return ""
		}
		if c.newRS == nil || c.newRS.Status.AvailableReplicas != defaults.GetReplicasOrDefault(c.rollout.Spec.Replicas) {
			return ""
		}
		if c.rollout.Status.PromoteFull {
			return "Full promotion requested"
		}
		_, currentStepIndex := replicasetutil.GetCurrentCanaryStep(c.rollout)
		stepCount := len(c.rollout.Spec.Strategy.Canary.Steps)
		completedAllSteps := stepCount == 0 || (currentStepIndex != nil && *currentStepIndex == int32(stepCount))
		if completedAllSteps {
			return fmt.Sprintf("Completed all %d canary steps", stepCount)
		}
	} else if c.rollout.Spec.Strategy.BlueGreen != nil {
		if newStatus.BlueGreen.ActiveSelector == "" {
			// corner case - initial deployments won't update the active selector until stable is set.
			// We must allow current to be marked stable, so that active can be marked to current, and
			// subsequently stable marked to current too. (chicken and egg problem)
			return "Initial deploy"
		}
		if newStatus.BlueGreen.ActiveSelector != newStatus.CurrentPodHash {
			// active selector still pointing to previous RS, don't update stable yet
			return ""
		}
		if !c.areTargetsVerified() {
			// active selector is pointing to desired RS, but we have not verify the target group yet
			return ""
		}
		if c.rollout.Status.PromoteFull {
			return "Full promotion requested"
		}
		if c.pauseContext.IsAborted() {
			return ""
		}
		if c.rollout.Spec.Strategy.BlueGreen.PostPromotionAnalysis != nil {
			// corner case - we fast-track the StableRS to be updated to CurrentPodHash when we are
			// moving to a ReplicaSet within scaleDownDelay and wish to skip analysis.
			if replicasetutil.HasScaleDownDeadline(c.newRS) {
				return fmt.Sprintf("Rollback to '%s' within scaleDownDelay", c.newRS.Name)
			}
			currentPostPromotionAnalysisRun := c.currentArs.BlueGreenPostPromotion
			if currentPostPromotionAnalysisRun == nil || currentPostPromotionAnalysisRun.Status.Phase != v1alpha1.AnalysisPhaseSuccessful {
				// we have yet to start post-promotion analysis or post-promotion was not successful
				return ""
			}
		}
		return "Completed blue-green update"
	}
	return ""
}

// promoteStable will take appropriate action once we have promoted the current ReplicaSet as stable
// e.g. reset status conditions, emit Kubernetes events, start scaleDownDelay, etc...
func (c *rolloutContext) promoteStable(newStatus *v1alpha1.RolloutStatus, reason string) error {
	c.pauseContext.ClearPauseConditions()
	c.pauseContext.RemoveAbort()
	newStatus.PromoteFull = false
	newStatus.BlueGreen.ScaleUpPreviewCheckPoint = false
	if c.rollout.Spec.Strategy.Canary != nil {
		stepCount := int32(len(c.rollout.Spec.Strategy.Canary.Steps))
		if stepCount > 0 {
			newStatus.CurrentStepIndex = &stepCount
		} else {
			newStatus.CurrentStepIndex = nil
		}
	}
	previousStableHash := newStatus.StableRS
	if previousStableHash != newStatus.CurrentPodHash {
		// only emit this event when we switched stable
		newStatus.StableRS = newStatus.CurrentPodHash
		revision, _ := replicasetutil.Revision(c.rollout)
		c.recorder.Eventf(c.rollout, record.EventOptions{EventReason: conditions.RolloutCompletedReason},
			conditions.RolloutCompletedMessage, revision, newStatus.CurrentPodHash, reason)
	}
	return nil
}

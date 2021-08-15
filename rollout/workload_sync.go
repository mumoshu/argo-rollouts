package rollout

import (
	"context"
	"fmt"
	"strconv"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/annotations"
	"github.com/argoproj/argo-rollouts/utils/conditions"
	"github.com/argoproj/argo-rollouts/utils/record"
	replicasetutil "github.com/argoproj/argo-rollouts/utils/replicaset"
	rolloututil "github.com/argoproj/argo-rollouts/utils/rollout"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/controller"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
	"k8s.io/utils/pointer"
)

// syncReplicasOnly is responsible for reconciling rollouts on scaling events.
func (c *workloadRolloutContext) syncReplicasOnly() error {
	c.log.Infof("Syncing replicas only (userPaused %v)", c.rollout.Spec.Paused)
	_, err := c.getAllWorkloadsAndSyncRevision(false)
	if err != nil {
		return err
	}

	// TODO Add support for Blue Green
	if c.rollout.Spec.Strategy.BlueGreen != nil {
		return nil
	}

	// The controller wants to use the rolloutCanary method to reconcile the rollout if the rollout is not paused.
	// If there are no scaling events, the rollout should only sync its status
	if c.rollout.Spec.Strategy.Canary != nil {
		// Reconciling AnalysisRuns to manage Background AnalysisRun if necessary
		err = c.reconcileAnalysisRuns()
		if err != nil {
			return err
		}

		// reconcileCanaryPause will ensure we will requeue this rollout at the appropriate time
		// if we are at a pause step with a duration.
		c.reconcileCanaryPause()

		err = c.reconcileTrafficRouting()
		if err != nil {
			return err
		}

		return c.syncRolloutStatusCanary()
	}
	return fmt.Errorf("no rollout strategy provided")
}

func (c *workloadRolloutContext) reconcileAnalysisRuns() error {
	return nil
}

func (c *workloadRolloutContext) reconcileTrafficRouting() error {
	return nil
}

func (c *workloadRolloutContext) reconcileExperiments() error {
	return nil
}

func (c *workloadRolloutContext) completedCurrentCanaryStep() bool {
	if c.rollout.Spec.Paused {
		return false
	}
	currentStep, _ := replicasetutil.GetCurrentCanaryStep(c.rollout)
	if currentStep == nil {
		return false
	}
	switch {
	case currentStep.Pause != nil:
		return c.pauseContext.CompletedCanaryPauseStep(*currentStep.Pause)
	case currentStep.SetCanaryScale != nil:
		log.Error("SetCanaryScale is not supported for Workload")
		return false
	case currentStep.SetWeight != nil:
		if c.weightVerified != nil && !*c.weightVerified {
			return false
		}
		return true
	case currentStep.Experiment != nil:
		experiment := c.currentEx
		return experiment != nil && experiment.Status.Phase == v1alpha1.AnalysisPhaseSuccessful
	case currentStep.Analysis != nil:
		currentStepAr := c.currentArs.CanaryStep
		analysisExistsAndCompleted := currentStepAr != nil && currentStepAr.Status.Phase.Completed()
		return analysisExistsAndCompleted && currentStepAr.Status.Phase == v1alpha1.AnalysisPhaseSuccessful
	}
	return false
}

func (c *workloadRolloutContext) syncRolloutStatusCanary() error {
	newStatus := c.calculateBaseStatus()

	currentStep, currentStepIndex := replicasetutil.GetCurrentCanaryStep(c.rollout)
	newStatus.StableRS = c.rollout.Status.StableRS
	newStatus.CurrentStepHash = conditions.ComputeStepHash(c.rollout)
	stepCount := int32(len(c.rollout.Spec.Strategy.Canary.Steps))

	if WorkloadTemplateOrStepsChanged(c.rollout, c.newWorkload) {
		c.resetRolloutStatus(&newStatus)
		if c.newWorkload != nil && c.rollout.Status.StableRS == GetWorkloadTemplateHash(c.newWorkload) {
			if stepCount > 0 {
				// If we get here, we detected that we've moved back to the stable ReplicaSet
				c.recorder.Eventf(c.rollout, record.EventOptions{EventReason: "SkipSteps"}, "Rollback to stable")
				newStatus.CurrentStepIndex = &stepCount
			}
		}
		newStatus = c.calculateRolloutConditions(newStatus)
		return c.persistRolloutStatus(&newStatus)
	}

	if c.rollout.Status.PromoteFull {
		c.pauseContext.ClearPauseConditions()
		c.pauseContext.RemoveAbort()
		if stepCount > 0 {
			currentStepIndex = &stepCount
		}
	}

	if reason := c.shouldFullPromote(newStatus); reason != "" {
		err := c.promoteStable(&newStatus, reason)
		if err != nil {
			return err
		}
		newStatus = c.calculateRolloutConditions(newStatus)
		return c.persistRolloutStatus(&newStatus)
	}

	if c.pauseContext.IsAborted() {
		if stepCount > int32(0) {
			if newStatus.StableRS == newStatus.CurrentPodHash {
				newStatus.CurrentStepIndex = &stepCount
			} else {
				newStatus.CurrentStepIndex = pointer.Int32Ptr(0)
			}
		}
		newStatus = c.calculateRolloutConditions(newStatus)
		return c.persistRolloutStatus(&newStatus)
	}

	if c.completedCurrentCanaryStep() {
		stepStr := rolloututil.CanaryStepString(*currentStep)
		*currentStepIndex++
		newStatus.Canary.CurrentStepAnalysisRunStatus = nil

		c.recorder.Eventf(c.rollout, record.EventOptions{EventReason: conditions.RolloutStepCompletedReason}, conditions.RolloutStepCompletedMessage, int(*currentStepIndex), stepCount, stepStr)
		c.pauseContext.RemovePauseCondition(v1alpha1.PauseReasonCanaryPauseStep)
	}

	newStatus.CurrentStepIndex = currentStepIndex
	newStatus = c.calculateRolloutConditions(newStatus)
	return c.persistRolloutStatus(&newStatus)
}

func (c *workloadRolloutContext) calculateRolloutConditions(newStatus v1alpha1.RolloutStatus) v1alpha1.RolloutStatus {
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

	isCompleteRollout := currentCond != nil
	// Check for progress. Only do this if the latest rollout hasn't completed yet and it is not aborted
	if !isCompleteRollout && !isAborted {
		switch {
		case conditions.RolloutComplete(c.rollout, &newStatus):
			// Update the rollout conditions with a message for the new replica set that
			// was successfully deployed. If the condition already exists, we ignore this update.
			rsName := ""
			if c.newWorkload != nil {
				rsName = c.newWorkload.Name
			}
			msg := fmt.Sprintf(conditions.ReplicaSetCompletedMessage, rsName)
			progressingCondition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionTrue, conditions.NewRSAvailableReason, msg)
			conditions.SetRolloutCondition(&newStatus, *progressingCondition)
		case conditions.RolloutProgressing(c.rollout, &newStatus):
			// If there is any progress made, continue by not checking if the rollout failed. This
			// behavior emulates the rolling updater progressDeadline check.
			msg := fmt.Sprintf(conditions.RolloutProgressingMessage, c.rollout.Name)
			if c.newWorkload != nil {
				msg = fmt.Sprintf(conditions.ReplicaSetProgressingMessage, c.newWorkload.Name)
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
			if c.newWorkload != nil {
				msg = fmt.Sprintf(conditions.ReplicaSetTimeOutMessage, c.newWorkload.Name)
			}
			condition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionFalse, conditions.TimedOutReason, msg)
			conditions.SetRolloutCondition(&newStatus, *condition)
		}
	}

	return newStatus
}

// shouldFullPromote returns a reason string explaining why a rollout should fully promote, marking
// the desired Workload as stable. Returns empty string if the rollout is in middle of update
func (c *workloadRolloutContext) shouldFullPromote(newStatus v1alpha1.RolloutStatus) string {
	// NOTE: the order of these checks are significant
	if c.stableWorkload == nil {
		return "Initial deploy"
	} else if c.rollout.Spec.Strategy.Canary != nil {
		if c.pauseContext.IsAborted() {
			return ""
		}
		if c.newWorkload == nil || c.newWorkload.Status.Ready == nil || !*c.newWorkload.Status.Ready {
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
		// TODO Add support for Blue Green
		return ""
	}
	return ""
}

// promoteStable will take appropriate action once we have promoted the current Workload as stable
// e.g. reset status conditions, emit Kubernetes events, start scaleDownDelay, etc...
func (c *workloadRolloutContext) promoteStable(newStatus *v1alpha1.RolloutStatus, reason string) error {
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
		// TODO ScaleDown delay
	}
	return nil
}

// calculateStatus calculates the common fields for all rollouts by looking into the provided replica sets.
func (c *workloadRolloutContext) calculateBaseStatus() v1alpha1.RolloutStatus {
	prevStatus := c.rollout.Status

	prevCond := conditions.GetRolloutCondition(prevStatus, v1alpha1.InvalidSpec)
	err := c.getRolloutValidationErrors()
	if err == nil && prevCond != nil {
		conditions.RemoveRolloutCondition(&prevStatus, v1alpha1.InvalidSpec)
	}

	var currentTemplateHash string
	if c.newWorkload == nil {
		// newWorkload potentially might be nil when called by syncReplicasOnly(). For this
		// to happen, the user would have had to simultaneously change the number of replicas, and
		// the pod template spec at the same time.
		currentTemplateHash = controller.ComputeHash(&c.rollout.Spec.Template, c.rollout.Status.CollisionCount)
		c.log.Infof("Assuming %s for new workload template hash", currentTemplateHash)
	} else {
		currentTemplateHash = c.newWorkload.Labels[v1alpha1.WorkloadTemplateHashLabelKey]
	}

	newStatus := c.newStatus
	newStatus.CurrentPodHash = currentTemplateHash
	newStatus.CollisionCount = c.rollout.Status.CollisionCount
	newStatus.Conditions = prevStatus.Conditions
	newStatus.RestartedAt = c.newStatus.RestartedAt
	newStatus.PromoteFull = (newStatus.CurrentPodHash != newStatus.StableRS) && prevStatus.PromoteFull
	return newStatus
}

// getAllReplicaSetsAndSyncRevision returns all the replica sets for the provided rollout (new and all old), with new RS's and rollout's revision updated.
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
func (c *workloadRolloutContext) getAllWorkloadsAndSyncRevision(createIfNotExisted bool) (*v1alpha1.Workload, error) {
	// Get new replica set with the updated revision number
	newWorkload, err := c.syncWorkloadRevision()
	if err != nil {
		return nil, err
	}
	if newWorkload == nil && createIfNotExisted {
		newWorkload, err = c.createDesiredWorkload()
		if err != nil {
			return nil, err
		}
	}
	return newWorkload, nil
}

// maxRevision finds the highest revision in the workloads
func maxRevision(allRSs []*v1alpha1.Workload) int64 {
	max := int64(0)
	for _, rs := range allRSs {
		if v, err := replicasetutil.Revision(rs); err != nil {
			// Skip the replica sets when it failed to parse their revision information
			log.WithError(err).Info("Couldn't parse revision, rollout controller will skip it when reconciling revisions.")
		} else if v > max {
			max = v
		}
	}
	return max
}

// Returns a replica set that matches the intent of the given rollout. Returns nil if the new replica set doesn't exist yet.
// 1. Get existing new RS (the RS that the given rollout targets, whose pod template is the same as rollout's).
// 2. If there's existing new RS, update its revision number if it's smaller than (maxOldRevision + 1), where maxOldRevision is the max revision number among all old RSes.
func (c *workloadRolloutContext) syncWorkloadRevision() (*v1alpha1.Workload, error) {
	if c.newWorkload == nil {
		return nil, nil
	}
	ctx := context.TODO()

	// Calculate the max revision number among all old RSes
	maxOldRevision := maxRevision(c.otherWorkloads)
	// Calculate revision number for this new replica set
	newRevision := strconv.FormatInt(maxOldRevision+1, 10)

	// Latest replica set exists. We need to sync its annotations (includes copying all but
	// annotationsToSkip from the parent rollout,
	// and also update the revision annotation in the rollout with the
	// latest revision.
	wlCopy := c.newWorkload.DeepCopy()

	// Set existing new replica set's annotation
	annotationsUpdated := annotations.SetNewWorkloadAnnotations(c.rollout, wlCopy, newRevision, true)

	if annotationsUpdated {
		return c.rolloutContext.argoprojclientset.ArgoprojV1alpha1().Workloads(wlCopy.ObjectMeta.Namespace).Update(ctx, wlCopy, metav1.UpdateOptions{})
	}

	// Should use the revision in existingNewRS's annotation, since it set by before
	if err := c.setRolloutRevision(wlCopy.Annotations[annotations.RevisionAnnotation]); err != nil {
		return nil, err
	}

	// If no other Progressing condition has been recorded and we need to estimate the progress
	// of this rollout then it is likely that old users started caring about progress. In that
	// case we need to take into account the first time we noticed their new replica set.
	cond := conditions.GetRolloutCondition(c.rollout.Status, v1alpha1.RolloutProgressing)
	if cond == nil {
		msg := fmt.Sprintf(conditions.FoundNewRSMessage, wlCopy.Name)
		condition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionTrue, conditions.FoundNewRSReason, msg)
		conditions.SetRolloutCondition(&c.rollout.Status, *condition)
		updatedRollout, err := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(c.rollout.Namespace).UpdateStatus(ctx, c.rollout, metav1.UpdateOptions{})
		if err != nil {
			c.log.WithError(err).Error("Error: updating rollout revision")
			return nil, err
		}
		c.rollout = updatedRollout
		c.newRollout = updatedRollout
		c.log.Infof("Initialized Progressing condition: %v", condition)
	}
	return wlCopy, nil
}

func (c *workloadRolloutContext) createDesiredWorkload() (*v1alpha1.Workload, error) {
	ctx := context.TODO()
	// Calculate the max revision number among all old RSes
	maxOldRevision := maxRevision(c.otherWorkloads)
	// Calculate revision number for this new replica set
	newRevision := strconv.FormatInt(maxOldRevision+1, 10)

	// new ReplicaSet does not exist, create one.
	newRSTemplate := *c.rollout.Spec.Template.DeepCopy()
	// Add default anti-affinity rule if antiAffinity bool set and RSTemplate meets requirements
	newRSTemplate.Spec.Affinity = replicasetutil.GenerateReplicaSetAffinity(*c.rollout)
	templateHash := ComputeHash(&c.rollout.Spec.WorkloadTemplate, c.rollout.Status.CollisionCount)
	newRSTemplate.Labels = labelsutil.CloneAndAddLabel(c.rollout.Spec.WorkloadTemplate.Labels, v1alpha1.WorkloadTemplateHashLabelKey, templateHash)

	// Create new Workload
	newWorkload := &v1alpha1.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.rollout.Name + "-" + templateHash,
			Namespace:       c.rollout.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(c.rollout, controllerKind)},
			Labels:          newRSTemplate.Labels,
		},
		Spec: v1alpha1.WorkloadSpec{
			ControllerName: c.rollout.Spec.WorkloadTemplate.Spec.ControllerName,
			Variables:      c.rollout.Spec.WorkloadTemplate.Spec.Variables,
		},
	}
	// Set new replica set's annotation
	// annotations.SetNewReplicaSetAnnotations(c.rollout, newRS, newRevision, false)

	// Add support for ephemeral metadata?

	// Create the new ReplicaSet. If it already exists, then we need to check for possible
	// hash collisions. If there is any other error, we need to report it in the status of
	// the Rollout.
	alreadyExists := false
	createdWorkload, err := c.rolloutContext.argoprojclientset.ArgoprojV1alpha1().Workloads(c.rollout.Namespace).Create(ctx, newWorkload, metav1.CreateOptions{})
	switch {
	// We may end up hitting this due to a slow cache or a fast resync of the Rollout.
	case errors.IsAlreadyExists(err):
		// TODO Add support for PodTemplateEqualIgnoreHash equivalent
		return nil, err
	case err != nil:
		msg := fmt.Sprintf(conditions.FailedRSCreateMessage, newWorkload.Name, err)
		c.recorder.Warnf(c.rollout, record.EventOptions{EventReason: conditions.FailedRSCreateReason}, msg)
		newStatus := c.rollout.Status.DeepCopy()
		cond := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionFalse, conditions.FailedRSCreateReason, msg)
		patchErr := c.patchCondition(c.rollout, newStatus, cond)
		if patchErr != nil {
			c.log.Warnf("Error Patching Rollout: %s", patchErr.Error())
		}
		return nil, err
	default:
		c.log.Infof("Created ReplicaSet %s", createdWorkload.Name)
	}

	if err := c.setRolloutRevision(newRevision); err != nil {
		return nil, err
	}

	if !alreadyExists {
		revision, _ := replicasetutil.Revision(createdWorkload)
		c.recorder.Eventf(c.rollout, record.EventOptions{EventReason: conditions.NewReplicaSetReason}, conditions.NewReplicaSetDetailedMessage, createdWorkload.Name, revision)

		msg := fmt.Sprintf(conditions.NewReplicaSetMessage, createdWorkload.Name)
		condition := conditions.NewRolloutCondition(v1alpha1.RolloutProgressing, corev1.ConditionTrue, conditions.NewReplicaSetReason, msg)
		conditions.SetRolloutCondition(&c.rollout.Status, *condition)
		updatedRollout, err := c.argoprojclientset.ArgoprojV1alpha1().Rollouts(c.rollout.Namespace).UpdateStatus(ctx, c.rollout, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
		c.rollout = updatedRollout.DeepCopy()
		if err := c.refResolver.Resolve(c.rollout); err != nil {
			return nil, err
		}
		c.newRollout = updatedRollout
		c.log.Infof("Set rollout condition: %v", condition)
	}
	return createdWorkload, err
}

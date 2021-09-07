package rollout

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/conditions"
	"github.com/argoproj/argo-rollouts/utils/record"
	replicasetutil "github.com/argoproj/argo-rollouts/utils/replicaset"
	rolloututil "github.com/argoproj/argo-rollouts/utils/rollout"
)

func (c *rolloutContext) rolloutCanary() error {
	c.Deployer = c.newDeployer()

	var err error
	if replicasetutil.PodTemplateOrStepsChanged(c.rollout, c.newRS) {
		c.newRS, err = c.GetAllReplicaSetsAndSyncRevision(false)
		if err != nil {
			return err
		}
		return c.syncRolloutStatusCanary()
	}

	c.newRS, err = c.GetAllReplicaSetsAndSyncRevision(true)
	if err != nil {
		return err
	}

	err = c.podRestarter.Reconcile(c)
	if err != nil {
		return err
	}

	err = c.ReconcileEphemeralMetadata()
	if err != nil {
		return err
	}

	if err := c.ReconcileRevisionHistoryLimit(); err != nil {
		return err
	}

	if err := c.reconcileStableAndCanaryService(); err != nil {
		return err
	}

	if err := c.reconcileTrafficRouting(); err != nil {
		return err
	}

	err = c.reconcileExperiments()
	if err != nil {
		return err
	}

	err = c.reconcileAnalysisRuns()
	if c.pauseContext.HasAddPause() {
		c.log.Info("Detected pause due to inconclusive AnalysisRun")
		return c.syncRolloutStatusCanary()
	}
	if err != nil {
		return err
	}

	noScalingOccurred, err := c.ReconcileCanary()
	if err != nil {
		return err
	}
	if noScalingOccurred {
		c.log.Info("Not finished reconciling ReplicaSets")
		return c.syncRolloutStatusCanary()
	}

	stillReconciling := c.reconcileCanaryPause()
	if stillReconciling {
		c.log.Infof("Not finished reconciling Canary Pause")
		return c.syncRolloutStatusCanary()
	}

	return c.syncRolloutStatusCanary()
}

func (c *rolloutContext) reconcileCanaryPause() bool {
	if c.rollout.Spec.Paused {
		return false
	}
	if c.rollout.Status.PromoteFull {
		return false
	}
	totalSteps := len(c.rollout.Spec.Strategy.Canary.Steps)
	if totalSteps == 0 {
		c.log.Info("Rollout does not have any steps")
		return false
	}
	currentStep, currentStepIndex := replicasetutil.GetCurrentCanaryStep(c.rollout)

	if totalSteps <= int(*currentStepIndex) {
		c.log.Info("No Steps remain in the canary steps")
		return false
	}

	if currentStep.Pause == nil {
		return false
	}
	c.log.Infof("Reconciling canary pause step (stepIndex: %d/%d)", *currentStepIndex, totalSteps)
	cond := getPauseCondition(c.rollout, v1alpha1.PauseReasonCanaryPauseStep)
	if cond == nil {
		// When the pause condition is null, that means the rollout is in an not paused state.
		// As a result, the controller needs to detect whether a rollout was unpaused or the
		// rollout needs to be paused for the first time. If the ControllerPause is false,
		// the controller has not paused the rollout yet and needs to do so before it
		// can proceed.
		if !c.rollout.Status.ControllerPause {
			c.pauseContext.AddPauseCondition(v1alpha1.PauseReasonCanaryPauseStep)
		}
		return true
	}
	if currentStep.Pause.Duration == nil {
		return true
	}
	c.checkEnqueueRolloutDuringWait(cond.StartTime, currentStep.Pause.DurationSeconds())
	return true
}

func (c *rolloutContext) completedCurrentCanaryStep() bool {
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
		return c.AtDesiredReplicaCountsForCanary()
	case currentStep.SetWeight != nil:
		if !c.AtDesiredReplicaCountsForCanary() {
			return false
		}
		if !c.areTargetsVerified() {
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

func (c *rolloutContext) syncRolloutStatusCanary() error {
	newStatus := c.calculateBaseStatus()
	newStatus.AvailableReplicas = replicasetutil.GetAvailableReplicaCountForReplicaSets(c.allRSs)
	newStatus.HPAReplicas = replicasetutil.GetActualReplicaCountForReplicaSets(c.allRSs)
	newStatus.Selector = metav1.FormatLabelSelector(c.rollout.Spec.Selector)

	currentStep, currentStepIndex := replicasetutil.GetCurrentCanaryStep(c.rollout)
	newStatus.StableRS = c.rollout.Status.StableRS
	newStatus.CurrentStepHash = conditions.ComputeStepHash(c.rollout)
	stepCount := int32(len(c.rollout.Spec.Strategy.Canary.Steps))

	if replicasetutil.PodTemplateOrStepsChanged(c.rollout, c.newRS) {
		c.resetRolloutStatus(&newStatus)
		if c.newRS != nil && c.rollout.Status.StableRS == replicasetutil.GetPodTemplateHash(c.newRS) {
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

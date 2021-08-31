package rollout

import (
	"math"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	replicasetutil "github.com/argoproj/argo-rollouts/utils/replicaset"
	serviceutil "github.com/argoproj/argo-rollouts/utils/service"
)

// rolloutBlueGreen implements the logic for rolling a new replica set.
func (c *rolloutContext) rolloutBlueGreen() error {
	previewSvc, activeSvc, err := c.getPreviewAndActiveServices()
	if err != nil {
		return err
	}

	c.Deployer = c.newDeployer()

	c.newRS, err = c.GetAllReplicaSetsAndSyncRevision(true)
	if err != nil {
		return err
	}

	// This must happen right after the new replicaset is created
	err = c.reconcilePreviewService(previewSvc)
	if err != nil {
		return err
	}

	if replicasetutil.CheckPodSpecChange(c.rollout, c.newRS) {
		return c.syncRolloutStatusBlueGreen(previewSvc, activeSvc)
	}

	err = c.podRestarter.Reconcile(c)
	if err != nil {
		return err
	}

	err = c.ReconcileBlueGreen(activeSvc)
	if err != nil {
		return err
	}

	c.reconcileBlueGreenPause(activeSvc, previewSvc)

	err = c.reconcileActiveService(activeSvc)
	if err != nil {
		return err
	}

	err = c.awsVerifyTargetGroups(activeSvc)
	if err != nil {
		return err
	}

	err = c.reconcileAnalysisRuns()
	if err != nil {
		return err
	}

	err = c.reconcileEphemeralMetadata()
	if err != nil {
		return err
	}

	return c.syncRolloutStatusBlueGreen(previewSvc, activeSvc)
}

// isBlueGreenFastTracked returns true if we should skip the pause step because update has been fast tracked
func (c *rolloutContext) isBlueGreenFastTracked(activeSvc *corev1.Service) bool {
	if replicasetutil.HasScaleDownDeadline(c.newRS) {
		c.log.Infof("Detected scale down annotation for ReplicaSet '%s' and will skip pause", c.newRS.Name)
		return true
	}
	if c.rollout.Status.PromoteFull {
		return true
	}
	if _, ok := activeSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]; !ok {
		return true
	}
	if activeSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey] == c.newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey] {
		return true
	}
	return false
}

func (c *rolloutContext) reconcileBlueGreenPause(activeSvc, previewSvc *corev1.Service) {
	if c.rollout.Status.Abort {
		return
	}

	if !replicasetutil.ReadyForPause(c.rollout, c.newRS, c.allRSs) {
		c.log.Infof("New RS '%s' is not ready to pause", c.newRS.Name)
		return
	}
	if c.rollout.Spec.Paused {
		c.log.Info("rollout has been paused by user")
		return
	}
	if c.isBlueGreenFastTracked(activeSvc) {
		c.log.Debug("skipping pause: fast-tracked update")
		c.pauseContext.RemovePauseCondition(v1alpha1.PauseReasonBlueGreenPause)
		return
	}
	newRSPodHash := c.newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	if activeSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey] == newRSPodHash {
		c.log.Debug("skipping pause: desired ReplicaSet already active")
		c.pauseContext.RemovePauseCondition(v1alpha1.PauseReasonBlueGreenPause)
		return
	}
	if c.rollout.Status.BlueGreen.ScaleUpPreviewCheckPoint {
		c.log.Debug("skipping pause: scaleUpPreviewCheckPoint passed")
		c.pauseContext.RemovePauseCondition(v1alpha1.PauseReasonBlueGreenPause)
		return
	}
	if !needsBlueGreenControllerPause(c.rollout) {
		c.pauseContext.RemovePauseCondition(v1alpha1.PauseReasonBlueGreenPause)
		return
	}

	// if we get here, the controller should manage the pause/resume
	c.log.Infof("reconciling pause (autoPromotionSeconds: %d)", c.rollout.Spec.Strategy.BlueGreen.AutoPromotionSeconds)
	if !c.completedPrePromotionAnalysis() {
		c.log.Infof("not ready for pause: prePromotionAnalysis incomplete")
		return
	}
	pauseCond := getPauseCondition(c.rollout, v1alpha1.PauseReasonBlueGreenPause)
	if pauseCond != nil {
		// We are currently paused. Check if we completed our pause duration
		if !c.pauseContext.CompletedBlueGreenPause() {
			c.log.Info("pause incomplete")
			if c.rollout.Spec.Strategy.BlueGreen.AutoPromotionSeconds > 0 {
				c.checkEnqueueRolloutDuringWait(pauseCond.StartTime, c.rollout.Spec.Strategy.BlueGreen.AutoPromotionSeconds)
			}
		} else {
			c.log.Infof("pause completed")
			c.pauseContext.RemovePauseCondition(v1alpha1.PauseReasonBlueGreenPause)
		}
	} else {
		// no pause condition exists. If Status.ControllerPause is true, the user manually resumed
		// the rollout. e.g. `kubectl argo rollouts promote ROLLOUT`
		if !c.rollout.Status.ControllerPause {
			c.log.Info("pausing")
			c.pauseContext.AddPauseCondition(v1alpha1.PauseReasonBlueGreenPause)
		}
	}
}

// needsBlueGreenControllerPause indicates if the controller should manage the pause status of the blue-green rollout
func needsBlueGreenControllerPause(ro *v1alpha1.Rollout) bool {
	if ro.Spec.Strategy.BlueGreen.AutoPromotionEnabled != nil {
		if !*ro.Spec.Strategy.BlueGreen.AutoPromotionEnabled {
			return true
		}
	}
	return ro.Spec.Strategy.BlueGreen.AutoPromotionSeconds > 0
}

func GetScaleDownRevisionLimit(ro *v1alpha1.Rollout) int32 {
	if ro.Spec.Strategy.BlueGreen != nil {
		if ro.Spec.Strategy.BlueGreen.ScaleDownDelayRevisionLimit != nil {
			return *ro.Spec.Strategy.BlueGreen.ScaleDownDelayRevisionLimit
		}
	}
	if ro.Spec.Strategy.Canary != nil {
		if ro.Spec.Strategy.Canary.ScaleDownDelayRevisionLimit != nil {
			return *ro.Spec.Strategy.Canary.ScaleDownDelayRevisionLimit
		}
	}
	return math.MaxInt32
}

func (c *rolloutContext) syncRolloutStatusBlueGreen(previewSvc *corev1.Service, activeSvc *corev1.Service) error {
	newStatus := c.calculateBaseStatus()
	newStatus.StableRS = c.rollout.Status.StableRS

	if replicasetutil.CheckPodSpecChange(c.rollout, c.newRS) {
		c.resetRolloutStatus(&newStatus)
	}
	if c.rollout.Status.PromoteFull {
		c.pauseContext.ClearPauseConditions()
		c.pauseContext.RemoveAbort()
	}

	previewSelector := serviceutil.GetRolloutSelectorLabel(previewSvc)
	if previewSelector != c.rollout.Status.BlueGreen.PreviewSelector {
		c.log.Infof("Updating preview selector (%s -> %s)", c.rollout.Status.BlueGreen.PreviewSelector, previewSelector)
	}
	newStatus.BlueGreen.PreviewSelector = previewSelector

	activeSelector := serviceutil.GetRolloutSelectorLabel(activeSvc)
	if activeSelector != c.rollout.Status.BlueGreen.ActiveSelector {
		c.log.Infof("Updating active selector (%s -> %s)", c.rollout.Status.BlueGreen.ActiveSelector, activeSelector)
	}
	newStatus.BlueGreen.ActiveSelector = activeSelector

	if reason := c.shouldFullPromote(newStatus); reason != "" {
		c.promoteStable(&newStatus, reason)
	} else {
		newStatus.BlueGreen.ScaleUpPreviewCheckPoint = c.calculateScaleUpPreviewCheckPoint(newStatus)
	}

	activeRS, _ := replicasetutil.GetReplicaSetByTemplateHash(c.allRSs, newStatus.BlueGreen.ActiveSelector)
	if activeRS != nil {
		newStatus.HPAReplicas = activeRS.Status.Replicas
		newStatus.Selector = metav1.FormatLabelSelector(activeRS.Spec.Selector)
		newStatus.AvailableReplicas = activeRS.Status.AvailableReplicas
		newStatus.ReadyReplicas = activeRS.Status.ReadyReplicas
	} else {
		// when we do not have an active replicaset, accounting is done on the default rollout selector
		newStatus.HPAReplicas = replicasetutil.GetActualReplicaCountForReplicaSets(c.allRSs)
		newStatus.Selector = metav1.FormatLabelSelector(c.rollout.Spec.Selector)
		newStatus.AvailableReplicas = replicasetutil.GetAvailableReplicaCountForReplicaSets(c.allRSs)
		// NOTE: setting ready replicas is skipped since it's already performed in c.calculateBaseStatus() and is redundant
		// newStatus.ReadyReplicas = replicasetutil.GetReadyReplicaCountForReplicaSets(c.allRSs)
	}

	newStatus = c.calculateRolloutConditions(newStatus)
	return c.persistRolloutStatus(&newStatus)
}

// calculateScaleUpPreviewCheckPoint calculates the correct value of status.blueGreen.scaleUpPreviewCheckPoint
// which is used by the blueGreen.previewReplicaCount feature. scaleUpPreviewCheckPoint is a single
// direction trip-wire, initialized to false, and gets flipped true as soon as the preview replicas
// matches scaleUpPreviewCheckPoint and prePromotionAnalysis (if used) completes. It get reset to
// false when the pod template changes, or the rollout fully promotes (stableRS == newRS)
func (c *rolloutContext) calculateScaleUpPreviewCheckPoint(newStatus v1alpha1.RolloutStatus) bool {
	if c.rollout.Spec.Strategy.BlueGreen.PreviewReplicaCount == nil {
		// previewReplicaCount feature is not being used
		return false
	}

	// Once the ScaleUpPreviewCheckPoint is set to true, the rollout should keep that value until
	// the newRS becomes the new stableRS or there is a template change.
	prevValue := c.rollout.Status.BlueGreen.ScaleUpPreviewCheckPoint
	if prevValue {
		return true
	}
	if !c.completedPrePromotionAnalysis() || !c.pauseContext.CompletedBlueGreenPause() {
		// do not set the checkpoint unless prePromotionAnalysis was successful and we completed our pause
		return false
	}
	previewCountAvailable := *c.rollout.Spec.Strategy.BlueGreen.PreviewReplicaCount == replicasetutil.GetAvailableReplicaCountForReplicaSets([]*appsv1.ReplicaSet{c.newRS})
	if prevValue != previewCountAvailable {
		c.log.Infof("setting scaleUpPreviewCheckPoint to %v: preview replica count availability is %v", previewCountAvailable, previewCountAvailable)
	}
	return previewCountAvailable
}

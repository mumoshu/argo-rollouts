package rollout

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	analysisutil "github.com/argoproj/argo-rollouts/utils/analysis"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	experimentutil "github.com/argoproj/argo-rollouts/utils/experiment"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
	"github.com/argoproj/argo-rollouts/utils/replicaset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/validation/field"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
)

type workloadRolloutContext struct {
	rolloutContext

	newWorkload    *v1alpha1.Workload
	stableWorkload *v1alpha1.Workload
	otherWorkloads []*v1alpha1.Workload
}

func (c *workloadRolloutContext) reconcileRollout(r *v1alpha1.Rollout) (*v1alpha1.Rollout, error) {
	// Get Rollout Validation errors
	err := c.getRolloutValidationErrors()
	if err != nil {
		if vErr, ok := err.(*field.Error); ok {
			// We want to frequently requeue rollouts with InvalidSpec errors, because the error
			// condition might be timing related (e.g. the Rollout was applied before the Service).
			c.enqueueRolloutAfter(c.rollout, 20*time.Second)
			return nil, c.createInvalidRolloutCondition(vErr, c.rollout)
		}
		return nil, err
	}

	err = c.checkPausedConditions()
	if err != nil {
		return nil, err
	}

	if getPauseCondition(c.rollout, v1alpha1.PauseReasonInconclusiveAnalysis) != nil || c.rollout.Spec.Paused {
		return nil, c.syncReplicasOnly()
	}

	// TODO Add support for BlueGreen rollout

	// Due to the rollout validation before this, when we get here strategy is canary
	return nil, c.rolloutCanary()
}

func (c *workloadRolloutContext) rolloutCanary() error {
	var err error
	if WorkloadTemplateOrStepsChanged(c.rollout, c.newWorkload) {
		c.newWorkload, err = c.getAllWorkloadsAndSyncRevision(false)
		if err != nil {
			return err
		}
		return c.syncRolloutStatusCanary()
	}

	c.newWorkload, err = c.getAllWorkloadsAndSyncRevision(true)
	if err != nil {
		return err
	}

	// TODO reconcileEphemeralMetadata()

	if err := c.reconcileRevisionHistoryLimit(c.otherWorkloads); err != nil {
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

	stillReconciling := c.reconcileCanaryPause()
	if stillReconciling {
		c.log.Infof("Not finished reconciling Canary Pause")
		return c.syncRolloutStatusCanary()
	}

	return c.syncRolloutStatusCanary()
}

func (c *workloadRolloutContext) reconcileRevisionHistoryLimit(oldWorkloads []*v1alpha1.Workload) error {
	ctx := context.TODO()
	revHistoryLimit := defaults.GetRevisionHistoryLimitOrDefault(c.rollout)

	// Avoid deleting replica set with deletion timestamp set
	aliveFilter := func(rs *v1alpha1.Workload) bool {
		return rs != nil && rs.ObjectMeta.DeletionTimestamp == nil
	}
	cleanableWorkloads := filterWorkloads(oldWorkloads, aliveFilter)

	diff := int32(len(cleanableWorkloads)) - revHistoryLimit
	if diff <= 0 {
		return nil
	}
	c.log.Infof("Cleaning up %d old workloads from revision history limit %d", len(cleanableWorkloads), revHistoryLimit)

	sort.Sort(workloadsByCreationTimestamp(cleanableWorkloads))
	wlTemplateHashToArList := analysisutil.SortAnalysisRunByPodHash(c.otherArs, v1alpha1.WorkloadTemplateHashLabelKey)
	wlTemplateHashToExList := experimentutil.SortExperimentsByPodHash(c.otherExs, v1alpha1.WorkloadTemplateHashLabelKey)
	c.log.Info("Looking to cleanup old workloads")
	for i := int32(0); i < diff; i++ {
		wl := cleanableWorkloads[i]
		c.log.Infof("Trying to workload %q", wl.Name)
		if err := c.rolloutContext.argoprojclientset.ArgoprojV1alpha1().Workloads(wl.Namespace).Delete(ctx, wl.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			// Return error instead of aggregating and continuing DELETEs on the theory
			// that we may be overloading the api server.
			return err
		}
		if wlTemplateHash, ok := wl.Labels[v1alpha1.WorkloadTemplateHashLabelKey]; ok {
			if ars, ok := wlTemplateHashToArList[wlTemplateHash]; ok {
				c.log.Infof("Cleaning up associated analysis runs with Workload '%s'", wl.Name)
				err := c.deleteAnalysisRuns(ars)
				if err != nil {
					return err
				}
			}
			if exs, ok := wlTemplateHashToExList[wlTemplateHash]; ok {
				c.log.Infof("Cleaning up associated experiments with Workload '%s'", wl.Name)
				err := c.deleteExperiments(exs)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

type filterWorkloadFunc func(rs *v1alpha1.Workload) bool

// FilterReplicaSets returns replica sets that are filtered by filterFn (all returned ones should match filterFn).
func filterWorkloads(workloads []*v1alpha1.Workload, filterFn filterWorkloadFunc) []*v1alpha1.Workload {
	var filtered []*v1alpha1.Workload
	for i := range workloads {
		if filterFn(workloads[i]) {
			filtered = append(filtered, workloads[i])
		}
	}
	return filtered
}

type workloadsByCreationTimestamp []*v1alpha1.Workload

func (o workloadsByCreationTimestamp) Len() int      { return len(o) }
func (o workloadsByCreationTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o workloadsByCreationTimestamp) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// WorkloadTemplateOrStepsChanged detects if there is a change in either the workload template, or canary steps
func WorkloadTemplateOrStepsChanged(rollout *v1alpha1.Rollout, newWorkload *v1alpha1.Workload) bool {
	if replicaset.CheckStepHashChange(rollout) {
		return true
	}
	if CheckWorkloadSpecChange(rollout, newWorkload) {
		return true
	}
	return false
}

// checkPodSpecChange indicates if the rollout spec has changed indicating that the rollout needs to reset the
// currentStepIndex to zero. If there is no previous pod spec to compare to the function defaults to false
func CheckWorkloadSpecChange(rollout *v1alpha1.Rollout, newWorkload *v1alpha1.Workload) bool {
	if rollout.Status.CurrentPodHash == "" {
		return false
	}
	podHash := ComputeHash(&rollout.Spec.WorkloadTemplate.Spec, rollout.Status.CollisionCount)
	if newWorkload != nil {
		podHash = GetWorkloadTemplateHash(newWorkload)
	}
	if rollout.Status.CurrentPodHash != podHash {
		logCtx := logutil.WithRollout(rollout)
		logCtx.Infof("Pod template change detected (new: %s, old: %s)", podHash, rollout.Status.CurrentPodHash)
		return true
	}
	return false
}

// GetPodTemplateHash returns the rollouts-pod-template-hash value from a ReplicaSet's labels
func GetWorkloadTemplateHash(rs *v1alpha1.Workload) string {
	if rs.Labels == nil {
		return ""
	}
	return rs.Labels[v1alpha1.WorkloadTemplateHashLabelKey]
}

// ComputeHash returns a hash value calculated from pod template and
// a collisionCount to avoid hash collision. The hash will be safe encoded to
// avoid bad words.
func ComputeHash(templateSpec interface{}, collisionCount *int32) string {
	templateSpecHasher := fnv.New32a()
	hashutil.DeepHashObject(templateSpecHasher, templateSpec)

	// Add collisionCount in the hash if it exists.
	if collisionCount != nil {
		collisionCountBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(collisionCountBytes, uint32(*collisionCount))
		templateSpecHasher.Write(collisionCountBytes)
	}

	return rand.SafeEncodeString(fmt.Sprint(templateSpecHasher.Sum32()))
}

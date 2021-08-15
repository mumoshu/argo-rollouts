package workloadbalancer

import (
	"context"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/pointer"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	clientset "github.com/argoproj/argo-rollouts/pkg/client/clientset/versioned"
	"github.com/argoproj/argo-rollouts/utils/diff"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
	"github.com/argoproj/argo-rollouts/utils/record"
	patchtypes "k8s.io/apimachinery/pkg/types"
)

const (
	// Type holds this controller type
	Type = "WorkloadBalancer"
)

// ReconcilerConfig describes static configuration data for the ALB Ingress reconciler
type ReconcilerConfig struct {
	Rollout        *v1alpha1.Rollout
	Client         clientset.Interface
	Recorder       record.EventRecorder
	ControllerKind schema.GroupVersionKind
}

// Reconciler holds required fields to reconcile ALB Ingress resources
type Reconciler struct {
	cfg ReconcilerConfig
	log *logrus.Entry

	canaryHash, stableHash string
}

// NewReconciler returns a reconciler struct that brings the ALB Ingress into the desired state
func NewReconciler(cfg ReconcilerConfig) (*Reconciler, error) {
	r := Reconciler{
		cfg: cfg,
		log: logutil.WithRollout(cfg.Rollout),
	}
	return &r, nil
}

// Type indicates this reconciler is an ALB ingress reconciler
func (r *Reconciler) Type() string {
	return Type
}

// SetWeight modifies ALB Ingress resources to reach desired state
func (r *Reconciler) SetWeight(desiredWeight int32) error {
	ctx := context.TODO()
	rollout := r.cfg.Rollout

	wbCfg := rollout.Spec.Strategy.Canary.TrafficRouting.WorkloadBalancer
	wbName := wbCfg.Name

	wb, err := r.getWorkloadBalancer(ctx, wbName)
	if k8serrors.IsNotFound(err) {
		// Create new WorkloadBalancer
		wbMeta := &metav1.ObjectMeta{}
		wbMeta.Name = wbName
		wbMeta.Namespace = rollout.Namespace
		wbMeta.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(rollout, r.cfg.ControllerKind),
		}
		wbMeta.Labels = rollout.Spec.Strategy.Canary.TrafficRouting.WorkloadBalancer.Template.PodTemplateMetadata.Labels
		wbMeta.Annotations = rollout.Spec.Strategy.Canary.TrafficRouting.WorkloadBalancer.Template.PodTemplateMetadata.Annotations

		wbSpec := rollout.Spec.Strategy.Canary.TrafficRouting.WorkloadBalancer.Template.Spec.DeepCopy()

		wb = &v1alpha1.WorkloadBalancer{
			ObjectMeta: *wbMeta,
			Spec:       *wbSpec,
		}

		wb, err = r.createWorkloadBalancer(ctx, wb)
		if err == nil {
			r.cfg.Recorder.Eventf(r.cfg.Rollout, record.EventOptions{EventReason: "WorkloadBalancerCreated"}, "WorkloadBalancer `%s` created", wbName)
		} else {
			r.cfg.Recorder.Eventf(r.cfg.Rollout, record.EventOptions{EventReason: "WorkloadBalancerNotCreated"}, "WorkloadBalancer `%s` failed creation: %v", wbName, err)
		}
	}

	if err != nil {
		return err
	}

	var readyWorkloads []v1alpha1.Workload

	if r.canaryHash == "" && r.stableHash == "" {
		var RolloutNameKey = "rollout-name"

		wbs, err := r.listWorkloads(ctx, metav1.AddLabelToSelector(&metav1.LabelSelector{}, RolloutNameKey, rollout.Name))
		if err != nil {
			return err
		}

		sort.SliceStable(wbs, func(i, j int) bool {
			return wbs[i].CreationTimestamp.Time.After(wbs[j].CreationTimestamp.Time)
		})

		for _, wb := range wbs {
			if wb.Status.Ready != nil && *wb.Status.Ready {
				readyWorkloads = append(readyWorkloads, wb)
			}
		}
	} else {
		stable, err := r.getWorkloadByHash(ctx, r.stableHash)
		if err != nil {
			return err
		}

		canary, err := r.getWorkloadByHash(ctx, r.canaryHash)
		if err != nil {
			return err
		}

		readyWorkloads = append(readyWorkloads, *canary, *stable)
	}

	if len(readyWorkloads) == 0 {
		return nil
	}

	var desiredBackends []v1alpha1.WorkloadBalancerBackend

	if len(readyWorkloads) == 1 {
		desiredBackends = append(desiredBackends, v1alpha1.WorkloadBalancerBackend{
			Name:   readyWorkloads[0].Name,
			Weight: pointer.Int32Ptr(100),
		})
	} else {
		desiredBackends = append(desiredBackends, v1alpha1.WorkloadBalancerBackend{
			Name:   readyWorkloads[0].Name,
			Weight: &desiredWeight,
		})
		desiredBackends = append(desiredBackends, v1alpha1.WorkloadBalancerBackend{
			Name:   readyWorkloads[1].Name,
			Weight: pointer.Int32Ptr(100 - desiredWeight),
		})
	}

	updated := wb.DeepCopy()
	updated.Spec.Backends = desiredBackends

	if err := r.patchWorkloadBalancer(ctx, wb, updated); err != nil {
		return nil
	}

	return nil
}

func (r *Reconciler) VerifyWeight(desiredWeight int32) (bool, error) {
	ctx := context.TODO()
	rollout := r.cfg.Rollout
	wbName := rollout.Spec.Strategy.Canary.TrafficRouting.WorkloadBalancer.Name
	wb, err := r.getWorkloadBalancer(ctx, wbName)
	if err != nil {
		return false, err
	}

	if wb.Generation != wb.Status.ObservedGeneration {
		return false, nil
	}

	return true, nil
}

// UpdateHash informs a traffic routing reconciler about new canary/stable workload hashes
func (r *Reconciler) UpdateHash(canaryHash, stableHash string) error {
	r.canaryHash = canaryHash
	r.stableHash = stableHash
	return nil
}

func (r *Reconciler) getWorkloadByHash(ctx context.Context, v string) (*v1alpha1.Workload, error) {
	ws, err := r.cfg.Client.ArgoprojV1alpha1().Workloads(r.cfg.Rollout.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(metav1.AddLabelToSelector(&metav1.LabelSelector{}, v1alpha1.WorkloadTemplateHashLabelKey, v)),
	})
	if err != nil {
		return nil, err
	}
	if len(ws.Items) == 0 {
		return nil, fmt.Errorf("Workload labeled with %s=%s does not exist", v1alpha1.WorkloadTemplateHashLabelKey, v)
	}

	return &ws.Items[0], nil
}

func (r *Reconciler) listWorkloads(ctx context.Context, selector *metav1.LabelSelector) ([]v1alpha1.Workload, error) {
	lis, err := r.cfg.Client.ArgoprojV1alpha1().Workloads(r.cfg.Rollout.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(selector),
	})
	if err != nil {
		return nil, err
	}

	return lis.Items, nil
}

func (r *Reconciler) getWorkloadBalancer(ctx context.Context, name string) (*v1alpha1.WorkloadBalancer, error) {
	return r.cfg.Client.ArgoprojV1alpha1().WorkloadBalancers(r.cfg.Rollout.Namespace).Get(ctx, name, metav1.GetOptions{})
}

func (r *Reconciler) createWorkloadBalancer(ctx context.Context, wb *v1alpha1.WorkloadBalancer) (*v1alpha1.WorkloadBalancer, error) {
	return r.cfg.Client.ArgoprojV1alpha1().WorkloadBalancers(r.cfg.Rollout.Namespace).Create(ctx, wb, metav1.CreateOptions{})
}

func (r *Reconciler) patchWorkloadBalancer(ctx context.Context, existing *v1alpha1.WorkloadBalancer, desired *v1alpha1.WorkloadBalancer) error {
	patch, modified, err := diff.CreateTwoWayMergePatch(
		v1alpha1.WorkloadBalancer{
			Spec: existing.Spec,
		},
		v1alpha1.WorkloadBalancer{
			Spec: desired.Spec,
		},
		v1alpha1.WorkloadBalancer{},
	)
	if err != nil {
		panic(err)
	}
	if !modified {
		r.log.Infof("WorkloadBalancer `%s` was not modified", existing.Name)
		return nil
	}
	_, err = r.cfg.Client.ArgoprojV1alpha1().WorkloadBalancers(r.cfg.Rollout.Namespace).Patch(ctx, existing.Name, patchtypes.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

package envoygateway

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ProxyManager helps manipulate EnvoyGateway CRDs dynamically.
type ProxyManager struct {
	client client.Client
}

// NewProxyManager creates a new proxy manager.
func NewProxyManager(c client.Client) *ProxyManager {
	return &ProxyManager{client: c}
}

// ScaleProxy scales the given resource to the desired number of replicas.
// It assumes the resource is an EnvoyProxy CRD from Envoy Gateway.
func (p *ProxyManager) ScaleProxy(ctx context.Context, ref *corev1.ObjectReference, replicas int32) error {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return fmt.Errorf("invalid APIVersion in reference: %w", err)
	}

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    ref.Kind,
	})

	if err := p.client.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}, u); err != nil {
		return fmt.Errorf("failed to get envoy proxy crd: %w", err)
	}

	patch := client.MergeFrom(u.DeepCopy())

	// For EnvoyGateway EnvoyProxy, we set the replicas field at spec.provider.kubernetes.envoyDeployment.replicas
	if err := unstructured.SetNestedField(u.Object, int64(replicas), "spec", "provider", "kubernetes", "envoyDeployment", "replicas"); err != nil {
		return fmt.Errorf("failed to set replicas: %w", err)
	}

	if err := p.client.Patch(ctx, u, patch); err != nil {
		return fmt.Errorf("failed to patch envoy proxy crd: %w", err)
	}

	return nil
}

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// EdgeHAConfig defines the configuration for High Availability during edge node upgrades.
type EdgeHAConfig struct {
	// NodeSelector defines which nodes are considered "edge nodes".
	// +optional
	NodeSelector *metav1.LabelSelector `json:"nodeSelector,omitempty"`

	// CloudflareSecretRef refers to the secret containing Cloudflare API tokens.
	// +optional
	CloudflareSecretRef *corev1.SecretKeySelector `json:"cloudflareSecretRef,omitempty"`

	// CloudflareZoneID is the zone ID where the DNS record lives.
	// +optional
	CloudflareZoneID string `json:"cloudflareZoneID,omitempty"`

	// DNSRecordName is the DNS A record to shift during the upgrade (e.g. api.example.com).
	// +optional
	DNSRecordName string `json:"dnsRecordName,omitempty"`

	// EnvoyProxyRef refers to the EnvoyProxy resource to patch.
	// +optional
	EnvoyProxyRef *corev1.ObjectReference `json:"envoyProxyRef,omitempty"`
}

// TalosUpgradeSpec defines the desired state of TalosUpgrade
type TalosUpgradeSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// TalosVersion is the desired Talos OS version (e.g. "v1.7.5").
	// +required
	TalosVersion string `json:"talosVersion"`

	// KubernetesVersion is the desired Kubernetes version (e.g. "v1.30.2").
	// +required
	KubernetesVersion string `json:"kubernetesVersion"`

	// EdgeHAConfig defines the configuration for High Availability during edge node upgrades.
	// +optional
	EdgeHAConfig *EdgeHAConfig `json:"edgeHAConfig,omitempty"`
}

// TalosUpgradeStatus defines the observed state of TalosUpgrade.
type TalosUpgradeStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Phase indicates the current state evaluated by the operator.
	// +optional
	Phase string `json:"phase,omitempty"`

	// EdgeUpgradeState tracks the state of the Edge Node HA workflow.
	// +optional
	EdgeUpgradeState string `json:"edgeUpgradeState,omitempty"`

	// TempWorkerIP stores the IP of the worker node temporarily running Envoy.
	// +optional
	TempWorkerIP string `json:"tempWorkerIP,omitempty"`

	// OriginalEdgeIP stores the original IP of the edge node.
	// +optional
	OriginalEdgeIP string `json:"originalEdgeIP,omitempty"`

	// Message is a human-readable message about the current state.
	// +optional
	Message string `json:"message,omitempty"`

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the TalosUpgrade resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Talos Version",type=string,JSONPath=".spec.talosVersion"
// +kubebuilder:printcolumn:name="K8s Version",type=string,JSONPath=".spec.kubernetesVersion"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// TalosUpgrade is the Schema for the talosupgrades API
type TalosUpgrade struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TalosUpgrade
	// +required
	Spec TalosUpgradeSpec `json:"spec"`

	// status defines the observed state of TalosUpgrade
	// +optional
	Status TalosUpgradeStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TalosUpgradeList contains a list of TalosUpgrade
type TalosUpgradeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TalosUpgrade `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TalosUpgrade{}, &TalosUpgradeList{})
		return nil
	})
}

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

package controller

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	upgradev1alpha1 "github.com/misha-artemiev/talops/api/v1alpha1"
	"github.com/misha-artemiev/talops/internal/cloudflare"
	"github.com/misha-artemiev/talops/internal/envoygateway"
)

// parseTalosVersion extracts the version string from the OSImage field (e.g. "Talos (v1.7.5)" -> "v1.7.5")
func parseTalosVersion(osImage string) string {
	start := strings.Index(osImage, "(")
	end := strings.Index(osImage, ")")
	if start != -1 && end != -1 && end > start {
		return osImage[start+1 : end]
	}
	return strings.TrimSpace(osImage)
}

func transitionState(upgrade *upgradev1alpha1.TalosUpgrade, newState, message string) {
	upgrade.Status.NodeUpgradeState = newState
	upgrade.Status.Message = message
	now := metav1.Now()
	upgrade.Status.LastTransitionTime = &now
}

func checkTimeout(upgrade *upgradev1alpha1.TalosUpgrade, timeout time.Duration) bool {
	if upgrade.Status.LastTransitionTime == nil {
		return false
	}
	return time.Since(upgrade.Status.LastTransitionTime.Time) > timeout
}

// State machine constants for Edge HA Upgrade
const (
	PhaseUpToDate                    = "UpToDate"
	StatePending                     = ""
	StateDeployingTempProxy          = "DeployingTempProxy"
	StateWaitingDNSPropagationToTemp = "WaitingDNSPropagationToTemp"
	StateUpgradingEdgeNode           = "UpgradingEdgeNode"
	StateWaitingDNSPropagationToEdge = "WaitingDNSPropagationToEdge"
	StateCleaningUp                  = "CleaningUp"
	StateDone                        = "Done"
)

// TalosUpgradeReconciler reconciles a TalosUpgrade object
type TalosUpgradeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=upgrade.noxbound.com,resources=talosupgrades,verbs=get;list;watch
// +kubebuilder:rbac:groups=upgrade.noxbound.com,resources=talosupgrades/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.envoyproxy.io,resources=envoyproxies,verbs=get;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *TalosUpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var upgrade upgradev1alpha1.TalosUpgrade
	if err := r.Get(ctx, req.NamespacedName, &upgrade); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		log.Error(err, "Failed to list nodes")
		return ctrl.Result{}, err
	}

	var mismatchedNodes []corev1.Node
	for _, node := range nodeList.Items {
		talosVer := parseTalosVersion(node.Status.NodeInfo.OSImage)
		needsTalos := talosVer != upgrade.Spec.TalosVersion
		needsKube := strings.TrimSpace(node.Status.NodeInfo.KubeletVersion) != upgrade.Spec.KubernetesVersion
		if needsTalos || needsKube {
			mismatchedNodes = append(mismatchedNodes, node)
		}
	}

	if len(mismatchedNodes) == 0 {
		if upgrade.Status.Phase != PhaseUpToDate || upgrade.Status.CurrentNode != "" {
			upgrade.Status.Phase = PhaseUpToDate
			upgrade.Status.CurrentNode = ""
			upgrade.Status.Message = "All nodes are running the desired versions"
			if err := r.Status().Update(ctx, &upgrade); err != nil {
				return ctrl.Result{}, err
			}
			log.Info("Updated TalosUpgrade status", "phase", PhaseUpToDate)
		}
		return ctrl.Result{}, nil
	}

	// Update Phase if necessary
	if upgrade.Status.Phase != "UpgradeNeeded" {
		upgrade.Status.Phase = "UpgradeNeeded"
		if err := r.Status().Update(ctx, &upgrade); err != nil {
			return ctrl.Result{}, err
		}
	}

	// If no node is currently being upgraded, pick the first one
	if upgrade.Status.CurrentNode == "" {
		upgrade.Status.CurrentNode = mismatchedNodes[0].Name
		upgrade.Status.NodeUpgradeState = StatePending
		upgrade.Status.Message = fmt.Sprintf("Selected node %s for upgrade", mismatchedNodes[0].Name)
		upgrade.Status.LastTransitionTime = nil
		if err := r.Status().Update(ctx, &upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Verify if the currently selected node is still mismatched
	var currentNodeObj *corev1.Node
	for i, n := range mismatchedNodes {
		if n.Name == upgrade.Status.CurrentNode {
			currentNodeObj = &mismatchedNodes[i]
			break
		}
	}

	if currentNodeObj == nil {
		// The node finished upgrading!
		log.Info("Node successfully upgraded", "node", upgrade.Status.CurrentNode)
		upgrade.Status.CurrentNode = ""
		upgrade.Status.NodeUpgradeState = ""
		upgrade.Status.Message = "Node upgraded successfully. Waiting to pick next node."
		upgrade.Status.LastTransitionTime = nil
		if err := r.Status().Update(ctx, &upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Determine if the node is an Edge Node
	isEdgeNode := false
	if upgrade.Spec.EdgeHAConfig != nil && upgrade.Spec.EdgeHAConfig.NodeSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(upgrade.Spec.EdgeHAConfig.NodeSelector)
		if err == nil && selector.Matches(labels.Set(currentNodeObj.Labels)) {
			isEdgeNode = true
		}
	}

	if isEdgeNode {
		return r.reconcileEdgeHA(ctx, &upgrade)
	}
	return r.reconcileStandardNode(ctx, &upgrade, currentNodeObj)
}

// reconcileEdgeHA handles the complex state machine for upgrading edge nodes with HA.
func (r *TalosUpgradeReconciler) reconcileEdgeHA(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade) (ctrl.Result, error) {
	// Initialize transition time if nil
	if upgrade.Status.LastTransitionTime == nil {
		now := metav1.Now()
		upgrade.Status.LastTransitionTime = &now
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
	}

	switch upgrade.Status.NodeUpgradeState {
	case StatePending:
		return r.handleEdgeStatePending(ctx, upgrade)
	case StateDeployingTempProxy:
		return r.handleEdgeStateDeployingTempProxy(ctx, upgrade)
	case StateWaitingDNSPropagationToTemp:
		return r.handleEdgeStateWaitingDNSPropagationToTemp(ctx, upgrade)
	case StateUpgradingEdgeNode:
		return r.handleEdgeStateUpgradingEdgeNode(ctx, upgrade)
	case StateWaitingDNSPropagationToEdge:
		return r.handleEdgeStateWaitingDNSPropagationToEdge(ctx, upgrade)
	case StateCleaningUp:
		return r.handleEdgeStateCleaningUp(ctx, upgrade)
	case StateDone, "Error":
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, nil
	}
}

func (r *TalosUpgradeReconciler) handleEdgeStatePending(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Scaling Envoy Proxy to 2 replicas")
	proxyMgr := envoygateway.NewProxyManager(r.Client)
	if err := proxyMgr.ScaleProxy(ctx, upgrade.Spec.EdgeHAConfig.EnvoyProxyRef, 2); err != nil {
		return ctrl.Result{}, err
	}

	if upgrade.Status.OriginalEdgeIP == "" {
		ips, err := net.LookupHost(upgrade.Spec.EdgeHAConfig.DNSRecordName)
		if err == nil && len(ips) > 0 {
			upgrade.Status.OriginalEdgeIP = ips[0]
		}
	}

	transitionState(upgrade, StateDeployingTempProxy, "Scaled EnvoyProxy to 2 replicas. Waiting for pods to be Ready.")
	if err := r.Status().Update(ctx, upgrade); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *TalosUpgradeReconciler) handleEdgeStateDeployingTempProxy(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Checking if temporary Envoy proxy is ready on worker node")

	if checkTimeout(upgrade, 10*time.Minute) {
		transitionState(upgrade, "Error", "Timeout waiting for temporary proxy to become ready.")
		_ = r.Status().Update(ctx, upgrade) // Error state updated, ignoring failure to update error state.
		return ctrl.Result{}, nil
	}

	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		return ctrl.Result{}, err
	}

	var tempWorkerIP string
	selector, err := metav1.LabelSelectorAsSelector(upgrade.Spec.EdgeHAConfig.NodeSelector)
	if err != nil {
		return ctrl.Result{}, err
	}

	for _, node := range nodeList.Items {
		if !selector.Matches(labels.Set(node.Labels)) {
			for _, addr := range node.Status.Addresses {
				if addr.Type == corev1.NodeExternalIP {
					tempWorkerIP = addr.Address
					break
				}
			}
			if tempWorkerIP == "" {
				for _, addr := range node.Status.Addresses {
					if addr.Type == corev1.NodeInternalIP {
						tempWorkerIP = addr.Address
						break
					}
				}
			}
			break
		}
	}

	if tempWorkerIP == "" {
		log.Info("No available worker node found")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	conn, err := net.DialTimeout("tcp", net.JoinHostPort(tempWorkerIP, "443"), 2*time.Second)
	if err != nil {
		log.Info("Proxy not yet answering on worker node", "ip", tempWorkerIP)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	_ = conn.Close()

	log.Info("Proxy is ready! Updating Cloudflare DNS", "ip", tempWorkerIP)
	var cfSecret corev1.Secret
	secretKey := client.ObjectKey{
		Namespace: upgrade.Namespace,
		Name:      upgrade.Spec.EdgeHAConfig.CloudflareSecretRef.Name,
	}
	if err := r.Get(ctx, secretKey, &cfSecret); err != nil {
		return ctrl.Result{}, err
	}

	cfClient, err := cloudflare.NewClient(string(cfSecret.Data[upgrade.Spec.EdgeHAConfig.CloudflareSecretRef.Key]))
	if err != nil {
		return ctrl.Result{}, err
	}

	err = cfClient.UpdateARecordIP(ctx, upgrade.Spec.EdgeHAConfig.CloudflareZoneID, upgrade.Spec.EdgeHAConfig.DNSRecordName, tempWorkerIP)
	if err != nil {
		log.Error(err, "Failed to update Cloudflare DNS")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	upgrade.Status.TempWorkerIP = tempWorkerIP
	transitionState(upgrade, StateWaitingDNSPropagationToTemp, "Cloudflare A record updated to worker IP. Waiting for DNS propagation.")
	if err := r.Status().Update(ctx, upgrade); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *TalosUpgradeReconciler) handleEdgeStateWaitingDNSPropagationToTemp(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Polling DNS to check if traffic shifted to TempWorkerIP")

	if checkTimeout(upgrade, 15*time.Minute) {
		transitionState(upgrade, "Error", "Timeout waiting for DNS to propagate.")
		_ = r.Status().Update(ctx, upgrade)
		return ctrl.Result{}, nil
	}

	ips, err := net.LookupHost(upgrade.Spec.EdgeHAConfig.DNSRecordName)
	if err != nil {
		log.Error(err, "DNS lookup failed")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if !slices.Contains(ips, upgrade.Status.TempWorkerIP) {
		log.Info("DNS not yet propagated", "expected", upgrade.Status.TempWorkerIP, "got", ips)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	transitionState(upgrade, StateUpgradingEdgeNode, "DNS propagated. Initiating edge node upgrade.")
	if err := r.Status().Update(ctx, upgrade); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TalosUpgradeReconciler) handleEdgeStateUpgradingEdgeNode(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Executing Edge Node cordon, drain, and Talos upgrade")
	// TODO: Implement actual Talos upgrade API call.

	if checkTimeout(upgrade, 30*time.Minute) {
		transitionState(upgrade, "Error", "Timeout waiting for Edge Node upgrade.")
		_ = r.Status().Update(ctx, upgrade)
		return ctrl.Result{}, nil
	}

	// Revert DNS
	var cfSecret corev1.Secret
	secretKey := client.ObjectKey{
		Namespace: upgrade.Namespace,
		Name:      upgrade.Spec.EdgeHAConfig.CloudflareSecretRef.Name,
	}
	if err := r.Get(ctx, secretKey, &cfSecret); err != nil {
		return ctrl.Result{}, err
	}

	cfClient, err := cloudflare.NewClient(string(cfSecret.Data[upgrade.Spec.EdgeHAConfig.CloudflareSecretRef.Key]))
	if err != nil {
		return ctrl.Result{}, err
	}

	err = cfClient.UpdateARecordIP(ctx, upgrade.Spec.EdgeHAConfig.CloudflareZoneID, upgrade.Spec.EdgeHAConfig.DNSRecordName, upgrade.Status.OriginalEdgeIP)
	if err != nil {
		log.Error(err, "Failed to revert Cloudflare DNS")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	transitionState(upgrade, StateWaitingDNSPropagationToEdge, "Edge node upgraded. Reverting Cloudflare A record to original IP.")
	if err := r.Status().Update(ctx, upgrade); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *TalosUpgradeReconciler) handleEdgeStateWaitingDNSPropagationToEdge(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Polling DNS to check if traffic shifted back to OriginalEdgeIP")

	if checkTimeout(upgrade, 15*time.Minute) {
		transitionState(upgrade, "Error", "Timeout waiting for DNS to propagate back.")
		_ = r.Status().Update(ctx, upgrade)
		return ctrl.Result{}, nil
	}

	ips, err := net.LookupHost(upgrade.Spec.EdgeHAConfig.DNSRecordName)
	if err != nil {
		log.Error(err, "DNS lookup failed")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if !slices.Contains(ips, upgrade.Status.OriginalEdgeIP) {
		log.Info("DNS not yet propagated back", "expected", upgrade.Status.OriginalEdgeIP, "got", ips)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	transitionState(upgrade, StateCleaningUp, "DNS propagated back. Cleaning up temporary proxy.")
	if err := r.Status().Update(ctx, upgrade); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TalosUpgradeReconciler) handleEdgeStateCleaningUp(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Scaling Envoy Proxy back to 1 replica")
	proxyMgr := envoygateway.NewProxyManager(r.Client)
	if err := proxyMgr.ScaleProxy(ctx, upgrade.Spec.EdgeHAConfig.EnvoyProxyRef, 1); err != nil {
		return ctrl.Result{}, err
	}
	transitionState(upgrade, StateDone, "Edge Node HA upgrade complete.")
	upgrade.Status.Phase = PhaseUpToDate
	if err := r.Status().Update(ctx, upgrade); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// State machine constants for Standard Node Upgrade
const (
	StateStandardPending   = "StandardPending"
	StateStandardUpgrading = "StandardUpgrading"
)

func (r *TalosUpgradeReconciler) reconcileStandardNode(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade, node *corev1.Node) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if upgrade.Status.LastTransitionTime == nil {
		now := metav1.Now()
		upgrade.Status.LastTransitionTime = &now
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
	}

	// If NodeUpgradeState is empty or Pending (used generically)
	if upgrade.Status.NodeUpgradeState == "" || upgrade.Status.NodeUpgradeState == StatePending {
		upgrade.Status.NodeUpgradeState = StateStandardPending
	}

	switch upgrade.Status.NodeUpgradeState {
	case StateStandardPending:
		log.Info("Executing standard node cordon, drain, and Talos upgrade", "node", node.Name)
		// TODO: Implement actual Talos upgrade API call.

		transitionState(upgrade, StateStandardUpgrading, fmt.Sprintf("Upgrading node %s.", node.Name))
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil

	case StateStandardUpgrading:
		log.Info("Waiting for node to report updated versions", "node", node.Name)

		if checkTimeout(upgrade, 30*time.Minute) {
			transitionState(upgrade, "Error", fmt.Sprintf("Timeout waiting for node %s to upgrade.", node.Name))
			_ = r.Status().Update(ctx, upgrade)
			return ctrl.Result{}, nil
		}

		// The orchestrator automatically clears CurrentNode when the versions match,
		// so if we are here, it hasn't matched yet. Just wait.
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil

	case "Error":
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TalosUpgradeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&upgradev1alpha1.TalosUpgrade{}).
		Named("talosupgrade").
		Complete(r)
}

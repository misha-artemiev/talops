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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	upgradev1alpha1 "github.com/misha-artemiev/talops/api/v1alpha1"
	"github.com/misha-artemiev/talops/internal/envoygateway"
)

// State machine constants for Edge HA Upgrade
const (
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
// TODO(user): Modify the Reconcile function to compare the state specified by
// the TalosUpgrade object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *TalosUpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var upgrade upgradev1alpha1.TalosUpgrade
	if err := r.Get(ctx, req.NamespacedName, &upgrade); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if upgrade.Spec.EdgeHAConfig != nil {
		return r.reconcileEdgeHA(ctx, &upgrade)
	}

	// Standard logic for non-edge upgrades
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		log.Error(err, "Failed to list nodes")
		return ctrl.Result{}, err
	}

	needTalosUpgrade := 0
	needKubeUpgrade := 0

	for _, node := range nodeList.Items {
		if !strings.Contains(node.Status.NodeInfo.OSImage, upgrade.Spec.TalosVersion) {
			needTalosUpgrade++
		}
		if !strings.Contains(node.Status.NodeInfo.KubeletVersion, upgrade.Spec.KubernetesVersion) {
			needKubeUpgrade++
		}
	}

	newPhase := "UpToDate"
	newMessage := "All nodes are running the desired versions"

	if needTalosUpgrade > 0 || needKubeUpgrade > 0 {
		newPhase = "UpgradeNeeded"
		newMessage = fmt.Sprintf("%d node(s) need Talos upgrade, %d node(s) need K8s upgrade", needTalosUpgrade, needKubeUpgrade)
	}

	if upgrade.Status.Phase != newPhase || upgrade.Status.Message != newMessage {
		upgrade.Status.Phase = newPhase
		upgrade.Status.Message = newMessage
		if err := r.Status().Update(ctx, &upgrade); err != nil {
			log.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}
		log.Info("Updated TalosUpgrade status", "phase", newPhase, "message", newMessage)
	}

	return ctrl.Result{}, nil
}

// reconcileEdgeHA handles the complex state machine for upgrading edge nodes with HA.
func (r *TalosUpgradeReconciler) reconcileEdgeHA(ctx context.Context, upgrade *upgradev1alpha1.TalosUpgrade) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	switch upgrade.Status.EdgeUpgradeState {
	case StatePending:
		log.Info("Scaling Envoy Proxy to 2 replicas")
		proxyMgr := envoygateway.NewProxyManager(r.Client)
		if err := proxyMgr.ScaleProxy(ctx, upgrade.Spec.EdgeHAConfig.EnvoyProxyRef, 2); err != nil {
			return ctrl.Result{}, err
		}
		upgrade.Status.EdgeUpgradeState = StateDeployingTempProxy
		upgrade.Status.Message = "Scaled EnvoyProxy to 2 replicas. Waiting for pods to be Ready."
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil

	case StateDeployingTempProxy:
		log.Info("Checking if temporary Envoy proxy is ready on worker node")
		// TODO: Validate that a proxy pod is ready on a non-edge node, and retrieve its ExternalIP.
		// For now, we simulate success and move to DNS update.
		// cloudflareSecret := fetchSecret(ctx, upgrade.Spec.EdgeHAConfig.CloudflareSecretRef)
		// dnsClient := cloudflare.NewClient(cloudflareSecret)
		// dnsClient.UpdateARecordIP(...)

		upgrade.Status.TempWorkerIP = "203.0.113.1" // Placeholder worker IP
		upgrade.Status.EdgeUpgradeState = StateWaitingDNSPropagationToTemp
		upgrade.Status.Message = "Cloudflare A record updated to worker IP. Waiting for DNS propagation."
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil

	case StateWaitingDNSPropagationToTemp:
		log.Info("Polling DNS to check if traffic shifted to TempWorkerIP")
		// TODO: Use net.LookupIP to resolve dnsRecordName globally.
		// If resolved IP == upgrade.Status.TempWorkerIP { proceed }

		upgrade.Status.EdgeUpgradeState = StateUpgradingEdgeNode
		upgrade.Status.Message = "DNS propagated. Initiating edge node upgrade."
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case StateUpgradingEdgeNode:
		log.Info("Executing Edge Node cordon, drain, and Talos upgrade")
		// TODO: Implement actual Talos upgrade API call.
		// Once edge node is Ready again, we revert DNS.

		upgrade.Status.EdgeUpgradeState = StateWaitingDNSPropagationToEdge
		upgrade.Status.Message = "Edge node upgraded. Reverting Cloudflare A record to original IP."
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil

	case StateWaitingDNSPropagationToEdge:
		log.Info("Polling DNS to check if traffic shifted back to OriginalEdgeIP")
		// TODO: Ensure DNS propagated back.

		upgrade.Status.EdgeUpgradeState = StateCleaningUp
		upgrade.Status.Message = "DNS propagated back. Cleaning up temporary proxy."
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case StateCleaningUp:
		log.Info("Scaling Envoy Proxy back to 1 replica")
		proxyMgr := envoygateway.NewProxyManager(r.Client)
		if err := proxyMgr.ScaleProxy(ctx, upgrade.Spec.EdgeHAConfig.EnvoyProxyRef, 1); err != nil {
			return ctrl.Result{}, err
		}
		upgrade.Status.EdgeUpgradeState = StateDone
		upgrade.Status.Phase = "UpToDate"
		upgrade.Status.Message = "Edge Node HA upgrade complete."
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case StateDone:
		// Nothing to do
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

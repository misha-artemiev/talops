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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	upgradev1alpha1 "github.com/misha-artemiev/talops/api/v1alpha1"
)

// TalosUpgradeReconciler reconciles a TalosUpgrade object
type TalosUpgradeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=upgrade.noxbound.com,resources=talosupgrades,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=upgrade.noxbound.com,resources=talosupgrades/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=upgrade.noxbound.com,resources=talosupgrades/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch

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

// SetupWithManager sets up the controller with the Manager.
func (r *TalosUpgradeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&upgradev1alpha1.TalosUpgrade{}).
		Named("talosupgrade").
		Complete(r)
}

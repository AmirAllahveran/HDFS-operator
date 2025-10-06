/*
Copyright 2023 AmirAllahveran.

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

package controllers

import (
	"context"
	"fmt"

	hdfsv1alpha1 "github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// HDFSClusterReconciler reconciles a HDFSCluster object
type HDFSClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

const (
	hdfsClusterFinalizer = "hdfs.aut.tech/finalizer"
)

//+kubebuilder:rbac:groups=hdfs.aut.tech,resources=hdfsclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hdfs.aut.tech,resources=hdfsclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hdfs.aut.tech,resources=hdfsclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="policy",resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile

func (r *HDFSClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("hdfscluster", req.NamespacedName)

	// Fetch the HDFS custom resource
	var hdfs hdfsv1alpha1.HDFSCluster
	err := r.Get(ctx, req.NamespacedName, &hdfs)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("resource deleted before reconciliation")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "failed to fetch HDFSCluster")
		return ctrl.Result{}, err
	}

	if hdfs.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&hdfs, hdfsClusterFinalizer) {
			controllerutil.AddFinalizer(&hdfs, hdfsClusterFinalizer)
			if err := r.Update(ctx, &hdfs); err != nil {
				logger.Error(err, "failed to add finalizer")
				return ctrl.Result{}, err
			}
			logger.Info("finalizer registered")
		}
	} else {
		if controllerutil.ContainsFinalizer(&hdfs, hdfsClusterFinalizer) {
			if err := r.cleanupCluster(ctx, &hdfs, logger); err != nil {
				logger.Error(err, "cleanup failed")
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&hdfs, hdfsClusterFinalizer)
			if err := r.Update(ctx, &hdfs); err != nil {
				logger.Error(err, "failed to remove finalizer")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if err := r.createOrUpdateComponents(ctx, &hdfs, logger); err != nil {
		logger.Error(err, "failed to reconcile dependent resources")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *HDFSClusterReconciler) cleanupCluster(ctx context.Context, hdfs *hdfsv1alpha1.HDFSCluster, logger logr.Logger) error {
	cleanupObjects := []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-cluster-config", Namespace: hdfs.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-datanode-script", Namespace: hdfs.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-zookeeper-script", Namespace: hdfs.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-ha-namenode-script", Namespace: hdfs.Namespace}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-namenode", Namespace: hdfs.Namespace}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-datanode", Namespace: hdfs.Namespace}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-journalnode", Namespace: hdfs.Namespace}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-zookeeper", Namespace: hdfs.Namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-hadoop", Namespace: hdfs.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-namenode", Namespace: hdfs.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-datanode", Namespace: hdfs.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-journalnode", Namespace: hdfs.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-zookeeper", Namespace: hdfs.Namespace}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: hdfs.Name + "-journalnode", Namespace: hdfs.Namespace}},
	}

	var errs []error
	for _, obj := range cleanupObjects {
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			logger.Error(err, "failed to cleanup dependent resource", "name", obj.GetName())
			errs = append(errs, err)
		}
	}

	aggregated := utilerrors.NewAggregate(errs)
	if aggregated != nil {
		r.recordWarning(hdfs, "CleanupFailed", aggregated.Error())
	}

	return aggregated
}

func (r *HDFSClusterReconciler) createOrUpdateComponents(ctx context.Context, hdfs *hdfsv1alpha1.HDFSCluster, logger logr.Logger) error {

	if hdfs.Spec.NameNode.Replicas == 2 {
		if hdfs.Spec.JournalNode == nil || hdfs.Spec.Zookeeper == nil {
			err := fmt.Errorf("journalNode and zookeeper specs are required when running NameNode in HA mode")
			r.recordWarning(hdfs, "MissingHAMembers", err.Error())
			return err
		}
	}

	if err := r.createOrUpdateConfigmap(ctx, hdfs, logger); err != nil {
		logger.Error(err, "reconcile configmap failed")
		return err
	}
	if hdfs.Spec.NameNode.Replicas == 2 {
		if err := r.createOrUpdateJournalNode(ctx, hdfs); err != nil {
			logger.Error(err, "reconcile journalnode failed")
			return err
		}
		if err := r.createOrUpdateZookeeper(ctx, hdfs); err != nil {
			logger.Error(err, "reconcile zookeeper failed")
			return err
		}
	}
	if err := r.createOrUpdateNameNode(ctx, hdfs, logger); err != nil {
		logger.Error(err, "reconcile namenode failed")
		return err
	}
	if err := r.createOrUpdateDataNode(ctx, hdfs); err != nil {
		logger.Error(err, "reconcile datanode failed")
		return err
	}
	if err := r.createHadoop(ctx, hdfs); err != nil {
		logger.Error(err, "reconcile hadoop client failed")
		return err
	}

	return nil
}

func (r *HDFSClusterReconciler) recordWarning(hdfs *hdfsv1alpha1.HDFSCluster, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(hdfs, corev1.EventTypeWarning, reason, message)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *HDFSClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hdfsv1alpha1.HDFSCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Complete(r)
}

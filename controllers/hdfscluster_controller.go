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
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/log"

	//"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	hdfsv1alpha1 "github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HDFSClusterReconciler reconciles a HDFSCluster object
type HDFSClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=hdfs.aut.tech,resources=hdfsclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hdfs.aut.tech,resources=hdfsclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hdfs.aut.tech,resources=hdfsclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *HDFSClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var kubeConfig *string
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeHome := filepath.Join(homedir.HomeDir(), ".kube", "config")
		kubeConfig = &kubeHome
		config, err = clientcmd.BuildConfigFromFlags("", *kubeConfig)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Error(err, "")
	}

	// Fetch the HDFS custom resource
	var hdfs hdfsv1alpha1.HDFSCluster
	err = r.Get(ctx, req.NamespacedName, &hdfs)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, it could have been deleted
			return ctrl.Result{}, nil
		}
		// Error occurred during fetching the object
		return ctrl.Result{}, err
	}
	// Check if the resource is marked for deletion
	//if !hdfs.ObjectMeta.DeletionTimestamp.IsZero() {
	//	// Perform cleanup tasks, e.g., delete associated components
	//	// and remove finalizers
	//	return r.handleDeletion(ctx, &hdfs, logger)
	//}

	// Check if the required components exist
	exists, err := r.componentsExist(ctx, &hdfs)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !exists {
		// Create the required components
		err = r.createComponents(ctx, clientSet, &hdfs, logger)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	//else {
	//	// Update the existing components if necessary
	//	err = r.updateComponents(ctx, &hdfs, logger)
	//	if err != nil {
	//		return ctrl.Result{}, err
	//	}
	//}

	return ctrl.Result{}, nil
}

func (r *HDFSClusterReconciler) createComponents(ctx context.Context, clientSet kubernetes.Interface, hdfs *hdfsv1alpha1.HDFSCluster, logger logr.Logger) error {
	configMap, err := r.DesiredClusterConfigMap(hdfs)
	if err != nil {
		logger.Error(err, "Failed to create desired ConfigMap")
		return err
	}

	// Create the ConfigMap
	logger.Info("Creating ConfigMap", "Namespace", configMap.Namespace, "Name", configMap.Name)
	_, err = clientSet.CoreV1().ConfigMaps(configMap.Namespace).Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil {
		logger.Error(err, "Failed to create ConfigMap", "Namespace", configMap.Namespace, "Name", configMap.Name)
		return err
	}

	return nil
}

func (r *HDFSClusterReconciler) componentsExist(ctx context.Context, hdfs *hdfsv1alpha1.HDFSCluster) (bool, error) {
	return false, nil
	//// Check if NameNode exists
	//nameNodeExists, err := r.nameNodeExists(ctx, hdfs)
	//if err != nil {
	//	return false, err
	//}
	//
	//// Check if DataNodes exist
	//dataNodesExist, err := r.dataNodesExist(ctx, hdfs)
	//if err != nil {
	//	return false, err
	//}

	//if hdfs.Spec.NameNode.Replicas == "1" {
	// Single NameNode mode
	//return nameNodeExists && dataNodesExist, nil
	//}
	//else if hdfs.Spec.NameNode.Replicas == "2" {
	//	// High Availability NameNode mode
	//	// Check if JournalNodes exist
	//	journalNodesExist, err := r.journalNodesExist(ctx, hdfs)
	//	if err != nil {
	//		return false, err
	//	}
	//
	//	// Check if Zookeeper components exist
	//	zookeeperExists, err := r.zookeeperExists(ctx, hdfs)
	//	if err != nil {
	//		return false, err
	//	}
	//
	//	return nameNodeExists && dataNodesExist && journalNodesExist && zookeeperExists, nil
	//} else {
	//	return false, fmt.Errorf("invalid NameNode replica count: %s", hdfs.Spec.NameNode.Replicas)
	//}
}

//const myHdfsFinalizer = "hdfs.finalizers.aut.com"
//
//func (r *HDFSClusterReconciler) handleDeletion(ctx context.Context, hdfs *hdfsv1alpha1.HDFSCluster, log logr.Logger) (ctrl.Result, error) {
//	if controllerutil.ContainsFinalizer(hdfs, myHdfsFinalizer) {
//		// Perform cleanup actions
//		if err := r.cleanupResources(ctx, hdfs); err != nil {
//			log.Error(err, "failed to clean up resources")
//			return ctrl.Result{}, err
//		}
//
//		// Remove finalizer after successful cleanup
//		controllerutil.RemoveFinalizer(hdfs, myHdfsFinalizer)
//		err := r.Update(ctx, hdfs)
//		if err != nil {
//			return ctrl.Result{}, err
//		}
//	}
//
//	log.Info("resource has been deleted")
//	return ctrl.Result{}, nil
//}
//
//func (r *HDFSClusterReconciler) cleanupResources(ctx context.Context, myHdfs *hdfsv1alpha1.HDFSCluster) error {
//	// Implement the cleanup logic here
//	// e.g., delete related resources, such as StatefulSets, Deployments, Pods, Services, etc.
//
//	// Note: Be cautious not to delete resources that are owned by other resources
//	// or are expected to be garbage-collected automatically.
//
//	return nil
//}

// SetupWithManager sets up the controller with the Manager.
func (r *HDFSClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hdfsv1alpha1.HDFSCluster{}).
		Watches(&source.Kind{Type: &hdfsv1alpha1.HDFSCluster{}},
			&handler.EnqueueRequestForOwner{IsController: true, OwnerType: &hdfsv1alpha1.HDFSCluster{}}).
		Complete(r)
}

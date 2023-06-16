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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"strings"
)

// HDFSClusterReconciler reconciles a HDFSCluster object
type HDFSClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

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
	logger := log.FromContext(ctx)

	//var kubeConfig *string
	//config, err := rest.InClusterConfig()
	//if err != nil {
	//	kubeHome := filepath.Join(homedir.HomeDir(), ".kube", "config")
	//	kubeConfig = &kubeHome
	//	config, err = clientcmd.BuildConfigFromFlags("", *kubeConfig)
	//	if err != nil {
	//		return ctrl.Result{}, err
	//	}
	//}
	//
	//clientSet, err := kubernetes.NewForConfig(config)
	//if err != nil {
	//	logger.Error(err, "")
	//}

	// Fetch the HDFS custom resource
	var hdfs hdfsv1alpha1.HDFSCluster
	err := r.Get(ctx, req.NamespacedName, &hdfs)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Object not found, it could have been deleted")
			return ctrl.Result{}, nil
		}
		logger.Info("Error occurred during fetching the object")
		return ctrl.Result{}, err
	}

	requestArray := strings.Split(fmt.Sprint(req), "/")
	requestName := requestArray[1]

	if requestName == hdfs.Name {
		//logger.Info("create Or Update Components")
		err = r.createOrUpdateComponents(ctx, &hdfs, logger)
		if err != nil {
			logger.Info("Error occurred during create Or Update Components")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *HDFSClusterReconciler) createOrUpdateComponents(ctx context.Context, hdfs *hdfsv1alpha1.HDFSCluster, logger logr.Logger) error {

	//logger.Info("createOrUpdateConfigmap", "name", hdfs.Name)
	err := r.createOrUpdateConfigmap(ctx, hdfs, logger)
	if err != nil {
		logger.Info("Error occurred during createOrUpdateConfigmap")
		return err
	}
	if hdfs.Spec.NameNode.Replicas == 2 {
		//logger.Info("createOrUpdateJournalNode", "name", hdfs.Name)
		err := r.createOrUpdateJournalNode(ctx, hdfs)
		if err != nil {
			logger.Info("Error occurred during createOrUpdateJournalNode")
			return err
		}
		//logger.Info("createOrUpdateZookeeper", "name", hdfs.Name)
		err = r.createOrUpdateZookeeper(ctx, hdfs)
		if err != nil {
			logger.Info("Error occurred during createOrUpdateZookeeper")
			return err
		}
	}
	//logger.Info("createOrUpdateNameNode", "name", hdfs.Name)
	err = r.createOrUpdateNameNode(ctx, hdfs, logger)
	if err != nil {
		logger.Info("Error occurred during createOrUpdateNameNode")
		return err
	}
	//logger.Info("createOrUpdateDataNode", "name", hdfs.Name)
	err = r.createOrUpdateDataNode(ctx, hdfs)
	if err != nil {
		logger.Info("Error occurred during createOrUpdateDataNode")
		return err
	}
	//logger.Info("createHadoop", "name", hdfs.Name)
	err = r.createHadoop(ctx, hdfs)
	if err != nil {
		logger.Info("Error occurred during createHadoop")
		return err
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HDFSClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hdfsv1alpha1.HDFSCluster{}).
		Complete(r)
}

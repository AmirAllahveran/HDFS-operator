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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// log is for logging in this package.
var hdfsclusterlog = logf.Log.WithName("hdfscluster-resource")

func (r *HDFSCluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

//+kubebuilder:webhook:path=/mutate-hdfs-aut-tech-v1alpha1-hdfscluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=hdfs.aut.tech,resources=hdfsclusters,verbs=create;update,versions=v1alpha1,name=mhdfscluster.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &HDFSCluster{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *HDFSCluster) Default() {
	hdfsclusterlog.Info("default", "name", r.Name)

	// TODO(user): fill in your defaulting logic.
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-hdfs-aut-tech-v1alpha1-hdfscluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=hdfs.aut.tech,resources=hdfsclusters,verbs=create;update,versions=v1alpha1,name=vhdfscluster.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HDFSCluster{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *HDFSCluster) ValidateCreate() error {
	hdfsclusterlog.Info("validate create", "name", r.Name)

	// TODO(user): fill in your validation logic upon object creation.
	return nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *HDFSCluster) ValidateUpdate(old runtime.Object) error {
	hdfsclusterlog.Info("validate update", "name", r.Name)

	// TODO(user): fill in your validation logic upon object update.
	return nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *HDFSCluster) ValidateDelete() error {
	hdfsclusterlog.Info("validate delete", "name", r.Name)

	// TODO(user): fill in your validation logic upon object deletion.
	return nil
}

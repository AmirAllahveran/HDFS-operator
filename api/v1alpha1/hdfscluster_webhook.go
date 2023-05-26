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
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"regexp"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"strconv"
)

// log is for logging in this package.
var hdfsclusterlog = logf.Log.WithName("hdfscluster-resource")

func (h *HDFSCluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(h).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-hdfs-aut-tech-v1alpha1-hdfscluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=hdfs.aut.tech,resources=hdfsclusters,verbs=create;update,versions=v1alpha1,name=mhdfscluster.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &HDFSCluster{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (h *HDFSCluster) Default() {
	hdfsclusterlog.Info("default", "name", h.Name)
	if h.Spec.ClusterConfig.DfsReplication == 0 {
		h.Spec.ClusterConfig.DfsReplication = 1
	}
	//if h.Spec.NameNode.Replicas == 1 {
	//	if _, ok := h.Spec.ClusterConfig.CustomHadoopConfig.CoreSite[""]; ok {
	//
	//	}
	//}
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-hdfs-aut-tech-v1alpha1-hdfscluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=hdfs.aut.tech,resources=hdfsclusters,verbs=create;update,versions=v1alpha1,name=vhdfscluster.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HDFSCluster{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (h *HDFSCluster) ValidateCreate() error {
	hdfsclusterlog.Info("validate create", "name", h.Name)

	match, err := isNumberMatchesPattern(h.Spec.DataNode.Replicas, "^[123]$")
	if err != nil {
		return err
	}
	if !match {
		return errors.New("invalid Datanode Replica count. It should be valid in this regex ^[123]$")
	}

	match, err = isNumberMatchesPattern(h.Spec.NameNode.Replicas, "^[12]$")
	if err != nil {
		return err
	}
	if !match {
		return errors.New("invalid Namenode Replica count. It should be valid in this regex ^[12]$")
	}

	if h.Spec.NameNode.Replicas == 2 {
		match, err := isNumberMatchesPattern(h.Spec.Zookeeper.Replicas, "^[13]$")
		if err != nil {
			return err
		}
		if !match {
			return errors.New("invalid Zookeeper Replica count. It should be valid in this regex ^[123]$")
		}

		match, err = isNumberMatchesPattern(h.Spec.JournalNode.Replicas, "^[13]$")
		if err != nil {
			return err
		}
		if !match {
			return errors.New("invalid JournalNode Replica count. It should be valid in this regex ^[123]$")
		}
	}

	return nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (h *HDFSCluster) ValidateUpdate(old runtime.Object) error {
	hdfsclusterlog.Info("validate update", "name", h.Name)

	// TODO(user): fill in your validation logic upon object update.
	return nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (h *HDFSCluster) ValidateDelete() error {
	hdfsclusterlog.Info("validate delete", "name", h.Name)

	// TODO(user): fill in your validation logic upon object deletion.
	return nil
}

func isNumberMatchesPattern(num int, pattern string) (bool, error) {
	// Convert the number to a string
	strNum := strconv.Itoa(num)

	// Compile the pattern
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, fmt.Errorf("invalid pattern: %v", err)
	}

	// Match the number against the pattern
	isMatch := re.MatchString(strNum)

	return isMatch, nil
}

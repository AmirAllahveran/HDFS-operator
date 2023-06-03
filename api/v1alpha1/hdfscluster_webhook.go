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
	if h.Spec.ClusterConfig.CoreSite == nil {
		h.Spec.ClusterConfig.CoreSite = make(map[string]string)
	}
	if h.Spec.ClusterConfig.HdfsSite == nil {
		h.Spec.ClusterConfig.HdfsSite = make(map[string]string)
	}
	if h.Spec.NameNode.Replicas == 1 {
		if _, ok := h.Spec.ClusterConfig.CoreSite["fs.defaultFS"]; !ok {
			h.Spec.ClusterConfig.CoreSite["fs.defaultFS"] = "hdfs://" + h.Name + "-namenode." +
				h.Namespace + ".svc.cluster.local:8020"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.replication"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.replication"] = "1"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"] = "false"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.namenode.name.dir"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.namenode.name.dir"] = "/data/hadoop/namenode"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.datanode.data.dir"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.datanode.data.dir"] = "/data/hadoop/datanode"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.permissions.enabled"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.permissions.enabled"] = "true"
		}
	} else {
		if _, ok := h.Spec.ClusterConfig.CoreSite["fs.defaultFS"]; !ok {
			h.Spec.ClusterConfig.CoreSite["fs.defaultFS"] = "hdfs://hdfs-k8s"
		}
		if _, ok := h.Spec.ClusterConfig.CoreSite["ha.zookeeper.quorum"]; !ok {
			zookeeperQuorum := h.Name + "-zookeeper-0." + h.Name + "-zookeeper." + h.Namespace + ".svc.cluster.local:2181"
			if h.Spec.Zookeeper.Replicas == 3 {
				zookeeperQuorum = h.Name + "-zookeeper-0." + h.Name + "-zookeeper." + h.Namespace + ".svc.cluster.local:2181," +
					h.Name + "-zookeeper-1." + h.Name + "-zookeeper." + h.Namespace + ".svc.cluster.local:2181," +
					h.Name + "-zookeeper-2." + h.Name + "-zookeeper." + h.Namespace + ".svc.cluster.local:2181"
			}
			h.Spec.ClusterConfig.CoreSite["ha.zookeeper.quorum"] = zookeeperQuorum
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.nameservices"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.nameservices"] = "hdfs-k8s"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.ha.namenodes.hdfs-k8s"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.ha.namenodes.hdfs-k8s"] = "nn0,nn1"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.namenode.rpc-address.hdfs-k8s.nn0"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.namenode.rpc-address.hdfs-k8s.nn0"] = h.Name + "-namenode-0." + h.Name +
				"-namenode." + h.Namespace + ".svc.cluster.local:8020"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.namenode.rpc-address.hdfs-k8s.nn1"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.namenode.rpc-address.hdfs-k8s.nn1"] = h.Name + "-namenode-1." + h.Name +
				"-namenode." + h.Namespace + ".svc.cluster.local:8020"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.namenode.shared.edits.dir"]; !ok {
			qjournal := h.Name + "-journalnode-0." + h.Name + "-journalnode." + h.Namespace + ".svc.cluster.local:8485"
			if h.Spec.JournalNode.Replicas == 3 {
				qjournal = h.Name + "-journalnode-0." + h.Name + "-journalnode." + h.Namespace + ".svc.cluster.local:8485;" +
					h.Name + "-journalnode-1." + h.Name + "-journalnode." + h.Namespace + ".svc.cluster.local:8485;" +
					h.Name + "-journalnode-2." + h.Name + "-journalnode." + h.Namespace + ".svc.cluster.local:8485"
			}
			h.Spec.ClusterConfig.HdfsSite["dfs.namenode.shared.edits.dir"] = "qjournal://" + qjournal + "/hdfs-k8s"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.client.failover.proxy.provider.hdfs-k8s"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.client.failover.proxy.provider.hdfs-k8s"] = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"] = "false"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.namenode.name.dir"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.namenode.name.dir"] = "/data/hadoop/namenode"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.datanode.data.dir"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.datanode.data.dir"] = "/data/hadoop/datanode"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.journalnode.edits.dir"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.journalnode.edits.dir"] = "/data/hadoop/journalnode"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.replication"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.replication"] = "1"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.permissions.enabled"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.permissions.enabled"] = "true"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.ha.fencing.methods"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.ha.fencing.methods"] = "shell(/bin/true)"
		}
		if _, ok := h.Spec.ClusterConfig.HdfsSite["dfs.ha.automatic-failover.enabled"]; !ok {
			h.Spec.ClusterConfig.HdfsSite["dfs.ha.automatic-failover.enabled"] = "true"
		}

	}
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-hdfs-aut-tech-v1alpha1-hdfscluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=hdfs.aut.tech,resources=hdfsclusters,verbs=create;update,versions=v1alpha1,name=vhdfscluster.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HDFSCluster{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (h *HDFSCluster) ValidateCreate() error {
	hdfsclusterlog.Info("validate create", "name", h.Name)

	return validateNode(h)
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (h *HDFSCluster) ValidateUpdate(old runtime.Object) error {
	hdfsclusterlog.Info("validate update", "name", h.Name)

	return validateNode(h)
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (h *HDFSCluster) ValidateDelete() error {
	hdfsclusterlog.Info("validate delete", "name", h.Name)

	return nil
}

func validateNode(h *HDFSCluster) error {
	match := isValid(h.Spec.DataNode, "^[123]$")
	if !match {
		return errors.New("invalid DataNode")
	}

	match = isValid(h.Spec.NameNode, "^[12]$")
	if !match {
		return errors.New("invalid NameNode")
	}

	if h.Spec.NameNode.Replicas == 2 {
		match := isValid(*h.Spec.Zookeeper, "^[13]$")
		if !match {
			return errors.New("invalid Zookeeper")
		}

		match = isValid(*h.Spec.JournalNode, "^[13]$")
		if !match {
			return errors.New("invalid JournalNode")
		}
	}
	return nil
}

func isValid(node Node, pattern string) bool {
	// Convert the number to a string
	strNum := strconv.Itoa(node.Replicas)

	// Compile the pattern
	re, _ := regexp.Compile(pattern)

	// Match the number against the pattern
	isMatch := re.MatchString(strNum)
	isValid := validateResources(&node.Resources)

	return isMatch && isValid
}

func validateResources(r *Resources) bool {
	pattern := `^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$`

	compile := regexp.MustCompile(pattern)

	if r.Cpu != "" && !compile.MatchString(r.Cpu) {
		return false
	}
	if r.Memory != "" && !compile.MatchString(r.Memory) {
		return false
	}
	if !compile.MatchString(r.Storage) {
		return false
	}
	return true
}

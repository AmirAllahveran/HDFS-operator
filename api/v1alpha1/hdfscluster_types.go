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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// HDFSClusterSpec defines the desired state of HDFSCluster
type HDFSClusterSpec struct {
	ClusterConfig ClusterConfig `json:"clusterConfig,omitempty"`
	NameNode      Node          `json:"nameNode"`
	DataNode      Node          `json:"dataNode"`
	JournalNode   Node          `json:"journalNode,omitempty"`
	Zookeeper     Node          `json:"zookeeper,omitempty"`
}

type Node struct {
	Replicas int `json:"replicas"`
	// +kubebuilder:validation:Optional
	Resources Resources `json:"resources"`
}

//type DataNode struct {
//	Replicas int `json:"replicas"`
//	// +kubebuilder:validation:Optional
//	Resources Resources `json:"resources"`
//}
//
//type Zookeeper struct {
//	Replicas int `json:"replicas"`
//	// +kubebuilder:validation:Optional
//	Resources Resources `json:"resources"`
//}

//type JournalNode struct {
//	Replicas int `json:"replicas"`
//	// +kubebuilder:validation:Optional
//	Resources Resources `json:"resources"`
//}

type ClusterConfig struct {
	CoreSite map[string]string `json:"coreSite,omitempty"`
	HdfsSite map[string]string `json:"hdfsSite,omitempty"`
}

type Resources struct {
	Memory  string `json:"memory,omitempty"`
	Cpu     string `json:"cpu,omitempty"`
	Storage string `json:"storage"`
}

// HDFSClusterStatus defines the observed state of HDFSCluster
type HDFSClusterStatus struct {
	CreationTime string `json:"creationTime"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// HDFSCluster is the Schema for the hdfsclusters API
type HDFSCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HDFSClusterSpec   `json:"spec,omitempty"`
	Status HDFSClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// HDFSClusterList contains a list of HDFSCluster
type HDFSClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HDFSCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HDFSCluster{}, &HDFSClusterList{})
}

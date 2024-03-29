//go:build !ignore_autogenerated
// +build !ignore_autogenerated

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

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ClusterConfig) DeepCopyInto(out *ClusterConfig) {
	*out = *in
	if in.CoreSite != nil {
		in, out := &in.CoreSite, &out.CoreSite
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.HdfsSite != nil {
		in, out := &in.HdfsSite, &out.HdfsSite
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ClusterConfig.
func (in *ClusterConfig) DeepCopy() *ClusterConfig {
	if in == nil {
		return nil
	}
	out := new(ClusterConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HDFSCluster) DeepCopyInto(out *HDFSCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	out.Status = in.Status
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HDFSCluster.
func (in *HDFSCluster) DeepCopy() *HDFSCluster {
	if in == nil {
		return nil
	}
	out := new(HDFSCluster)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *HDFSCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HDFSClusterList) DeepCopyInto(out *HDFSClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]HDFSCluster, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HDFSClusterList.
func (in *HDFSClusterList) DeepCopy() *HDFSClusterList {
	if in == nil {
		return nil
	}
	out := new(HDFSClusterList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *HDFSClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HDFSClusterSpec) DeepCopyInto(out *HDFSClusterSpec) {
	*out = *in
	in.ClusterConfig.DeepCopyInto(&out.ClusterConfig)
	out.NameNode = in.NameNode
	out.DataNode = in.DataNode
	if in.JournalNode != nil {
		in, out := &in.JournalNode, &out.JournalNode
		*out = new(Node)
		**out = **in
	}
	if in.Zookeeper != nil {
		in, out := &in.Zookeeper, &out.Zookeeper
		*out = new(Node)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HDFSClusterSpec.
func (in *HDFSClusterSpec) DeepCopy() *HDFSClusterSpec {
	if in == nil {
		return nil
	}
	out := new(HDFSClusterSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HDFSClusterStatus) DeepCopyInto(out *HDFSClusterStatus) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HDFSClusterStatus.
func (in *HDFSClusterStatus) DeepCopy() *HDFSClusterStatus {
	if in == nil {
		return nil
	}
	out := new(HDFSClusterStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Node) DeepCopyInto(out *Node) {
	*out = *in
	out.Resources = in.Resources
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Node.
func (in *Node) DeepCopy() *Node {
	if in == nil {
		return nil
	}
	out := new(Node)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Resources) DeepCopyInto(out *Resources) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Resources.
func (in *Resources) DeepCopy() *Resources {
	if in == nil {
		return nil
	}
	out := new(Resources)
	in.DeepCopyInto(out)
	return out
}

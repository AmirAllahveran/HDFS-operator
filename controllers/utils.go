package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/go-xmlfmt/xmlfmt"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
)

func stringToInt32(s string) *int32 {
	i, _ := strconv.ParseInt(s, 10, 32)
	result := int32(i)
	return &result
}

func int32Ptr(i int32) *int32 { return &i }

func int64Ptr(i int64) *int64 { return &i }

func (r *HDFSClusterReconciler) ScaleDownAndUpStatefulSet(ctx context.Context, name, namespace string) error {
	existingStatefulSet := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, existingStatefulSet)
	if err != nil {
		return err
	}

	// Remember the original replica count
	originalReplicaCount := *existingStatefulSet.Spec.Replicas

	// Scale down to zero
	zero := int32(0)
	existingStatefulSet.Spec.Replicas = &zero

	// Update the StatefulSet with the new replica count
	if err := r.Update(ctx, existingStatefulSet); err != nil {
		return err
	}

	// Scale back up to the original count
	existingStatefulSet.Spec.Replicas = &originalReplicaCount
	// Update the StatefulSet with the new replica count
	if err := r.Update(ctx, existingStatefulSet); err != nil {
		return err
	}

	return nil
}

func (r *HDFSClusterReconciler) ScaleDownAndUpDeployment(ctx context.Context, name, namespace string) error {
	existingDeployment := &appsv1.Deployment{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, existingDeployment)
	if err != nil {
		return err
	}

	// Remember the original replica count
	originalReplicaCount := *existingDeployment.Spec.Replicas

	// Scale down to zero
	zero := int32(0)
	existingDeployment.Spec.Replicas = &zero

	// Update the StatefulSet with the new replica count
	if err := r.Update(ctx, existingDeployment); err != nil {
		return err
	}

	// Scale back up to the original count
	existingDeployment.Spec.Replicas = &originalReplicaCount
	// Update the StatefulSet with the new replica count
	if err := r.Update(ctx, existingDeployment); err != nil {
		return err
	}

	return nil
}

func mapToXml(properties map[string]string) string {
	var res string
	for key, value := range properties {
		property := `<property>
	<name>` + key + `</name>
	<value>` + value + `</value>
</property>`
		res = res + property
	}

	res = xmlfmt.FormatXML(res, "", "  ")

	return res
}

func resourceRequirements(resources v1alpha1.Resources) (*v1.ResourceRequirements, error) {
	var err error

	req := v1.ResourceRequirements{
		Requests: v1.ResourceList{},
		Limits:   v1.ResourceList{},
	}

	if resources.Cpu != "" {
		req.Requests[v1.ResourceCPU], err = resource.ParseQuantity(resources.Cpu)
		if err != nil {
			return nil, err
		}
		req.Limits[v1.ResourceCPU] = req.Requests[v1.ResourceCPU]
	}

	if resources.Memory != "" {
		req.Requests[v1.ResourceMemory], err = resource.ParseQuantity(resources.Memory)
		if err != nil {
			return nil, err
		}
		req.Limits[v1.ResourceMemory] = req.Requests[v1.ResourceMemory]
	}

	return &req, nil
}

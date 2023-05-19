package controllers

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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

func (r *HDFSClusterReconciler) scaleStatefulSet(ctx context.Context, name, namespace string, replicas int32) error {
	existingStatefulSet := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, existingStatefulSet)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	existingStatefulSet.Spec.Replicas = &replicas
	if err := r.Update(ctx, existingStatefulSet); err != nil {
		return err
	}

	return nil
}

func (r *HDFSClusterReconciler) scaleDeployment(ctx context.Context, name, namespace string, replicas int32) error {
	existingDeployment := &appsv1.Deployment{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, existingDeployment)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	existingDeployment.Spec.Replicas = &replicas
	if err := r.Update(ctx, existingDeployment); err != nil {
		return err
	}

	return nil
}

package controllers

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"testing"
)

func TestStringToInt32(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want int32
	}{
		{"Test 1", "12345", 12345},
		{"Test 2", "0", 0},
		{"Test 3", "-12345", -12345},
		{"Test 4", "abcd", 0}, // parsing non-integer string should result in 0
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stringToInt32(tt.arg)
			if *got != tt.want {
				t.Errorf("stringToInt32() = %v, want %v", *got, tt.want)
			}
		})
	}
}

// TestInt32Ptr is the unit test function for int32Ptr
func TestInt32Ptr(t *testing.T) {
	testCases := []struct {
		input  int32
		output *int32
	}{
		{0, new(int32)},
		{1, new(int32)},
		{-1, new(int32)},
		{12345, new(int32)},
		{-12345, new(int32)},
		{2147483647, new(int32)},
		{-2147483648, new(int32)},
	}

	for _, tc := range testCases {
		// Set the expected value of the pointer
		*tc.output = tc.input

		// Call int32Ptr function and compare the result with the expected output
		got := int32Ptr(tc.input)
		if *got != *tc.output {
			t.Errorf("int32Ptr(%d) = %d; want %d", tc.input, *got, *tc.output)
		}
	}
}

func TestScaleDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	namespace := "default"
	name := "test-deployment"
	initialReplicas := int32(3)
	desiredReplicas := int32(2)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &initialReplicas,
		},
	}

	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).Build(),
		Scheme: scheme,
	}

	err := r.scaleDeployment(context.Background(), name, namespace, desiredReplicas)
	if err != nil {
		t.Fatalf("Failed to scale Deployment: %v", err)
	}

	updatedDeployment := &appsv1.Deployment{}
	err = r.Client.Get(context.Background(), client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, updatedDeployment)
	if err != nil {
		t.Fatalf("Failed to get updated Deployment: %v", err)
	}

	if *updatedDeployment.Spec.Replicas != desiredReplicas {
		t.Errorf("Deployment replicas mismatch: expected '%d', got '%d'", desiredReplicas, *updatedDeployment.Spec.Replicas)
	}
}

func TestScaleStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	namespace := "default"
	name := "test-deployment"
	initialReplicas := int32(3)
	desiredReplicas := int32(2)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &initialReplicas,
		},
	}

	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build(),
		Scheme: scheme,
	}

	err := r.scaleStatefulSet(context.Background(), name, namespace, desiredReplicas)
	if err != nil {
		t.Fatalf("Failed to scale Deployment: %v", err)
	}

	updatedSts := &appsv1.StatefulSet{}
	err = r.Client.Get(context.Background(), client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, updatedSts)
	if err != nil {
		t.Fatalf("Failed to get updated Deployment: %v", err)
	}

	if *updatedSts.Spec.Replicas != desiredReplicas {
		t.Errorf("Deployment replicas mismatch: expected '%d', got '%d'", desiredReplicas, *updatedSts.Spec.Replicas)
	}
}

func TestMapToXml(t *testing.T) {
	properties := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	res := mapToXml(properties)
	if !strings.Contains(res, "key1") || !strings.Contains(res, "value1") {
		t.Errorf("Expected 'key1' and 'value1' to be in the result, got %s", res)
	}

	if !strings.Contains(res, "key2") || !strings.Contains(res, "value2") {
		t.Errorf("Expected 'key2' and 'value2' to be in the result, got %s", res)
	}
}

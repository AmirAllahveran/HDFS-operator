package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

func TestHDFSClusterReconciler_ScaleDownAndUpDeployment(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Create a HDFSCluster object
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hdfs-cluster",
			Namespace: "default",
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}

	// Create a new Deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(3), // 3 replicas initially
		},
	}
	// Create the Deployment
	err := r.Create(context.Background(), deployment)
	if err != nil {
		t.Fatalf("failed to create test deployment: %v", err)
	}

	// Scale down and up the deployment
	err = r.ScaleDownAndUpDeployment(context.Background(), "test-deployment", "default")
	if err != nil {
		t.Fatalf("failed to scale down and up deployment: %v", err)
	}

	// Fetch the deployment to check the replicas
	err = r.Get(context.Background(), client.ObjectKey{
		Namespace: "default",
		Name:      "test-deployment",
	}, deployment)
	if err != nil {
		t.Fatalf("failed to get deployment: %v", err)
	}

	// Assert that the number of replicas is still 3
	if *deployment.Spec.Replicas != 3 {
		t.Errorf("unexpected number of replicas: got %v want %v", *deployment.Spec.Replicas, 3)
	}
}

func TestHDFSClusterReconciler_ScaleDownAndUpStatefulSet(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Create a HDFSCluster object
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hdfs-cluster",
			Namespace: "default",
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}

	// Create a new sts
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sts",
			Namespace: "default",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(3), // 3 replicas initially
		},
	}
	// Create the sts
	err := r.Create(context.Background(), sts)
	if err != nil {
		t.Fatalf("failed to create test sts: %v", err)
	}

	// Scale down and up the sts
	err = r.ScaleDownAndUpStatefulSet(context.Background(), "test-sts", "default")
	if err != nil {
		t.Fatalf("failed to scale down and up sts: %v", err)
	}

	// Fetch the sts to check the replicas
	err = r.Get(context.Background(), client.ObjectKey{
		Namespace: "default",
		Name:      "test-sts",
	}, sts)
	if err != nil {
		t.Fatalf("failed to get sts: %v", err)
	}

	// Assert that the number of replicas is still 3
	if *sts.Spec.Replicas != 3 {
		t.Errorf("unexpected number of replicas: got %v want %v", *sts.Spec.Replicas, 3)
	}
}

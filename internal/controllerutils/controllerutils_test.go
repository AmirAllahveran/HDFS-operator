package controllerutils

import (
	"context"
	"strings"
	"testing"

	hdfsv1alpha1 "github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestStringToInt32(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want int32
	}{
		{"valid", "12345", 12345},
		{"zero", "0", 0},
		{"negative", "-12345", -12345},
		{"invalid", "abcd", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StringToInt32(tt.arg)
			if *got != tt.want {
				t.Fatalf("StringToInt32() = %v, want %v", *got, tt.want)
			}
		})
	}
}

func TestInt32Ptr(t *testing.T) {
	inputs := []int32{0, 1, -1, 42}
	for _, input := range inputs {
		ptr := Int32Ptr(input)
		if *ptr != input {
			t.Fatalf("Int32Ptr(%d) = %d", input, *ptr)
		}
	}
}

func TestMapToXML(t *testing.T) {
	properties := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	res := MapToXML(properties)
	if !strings.Contains(res, "key1") || !strings.Contains(res, "value1") {
		t.Fatalf("expected key1/value1 in XML, got %s", res)
	}
}

func TestRestartDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = appsv1.AddToScheme(scheme)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{Replicas: Int32Ptr(3)},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).Build()

	if err := RestartDeployment(context.Background(), fakeClient, "test-deployment", "default"); err != nil {
		t.Fatalf("RestartDeployment returned error: %v", err)
	}

	fetched := &appsv1.Deployment{}
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Name: "test-deployment", Namespace: "default"}, fetched); err != nil {
		t.Fatalf("failed to fetch deployment: %v", err)
	}

	if fetched.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
		t.Fatalf("restart annotation not set")
	}
}

func TestRestartStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = appsv1.AddToScheme(scheme)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sts",
			Namespace: "default",
		},
		Spec: appsv1.StatefulSetSpec{Replicas: Int32Ptr(3)},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build()

	if err := RestartStatefulSet(context.Background(), fakeClient, "test-sts", "default"); err != nil {
		t.Fatalf("RestartStatefulSet returned error: %v", err)
	}

	fetched := &appsv1.StatefulSet{}
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Name: "test-sts", Namespace: "default"}, fetched); err != nil {
		t.Fatalf("failed to fetch statefulset: %v", err)
	}

	if fetched.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
		t.Fatalf("restart annotation not set")
	}
}

func TestResourceRequirements(t *testing.T) {
	resources := hdfsv1alpha1.Resources{
		Memory:  "1Gi",
		Cpu:     "500m",
		Storage: "10Gi",
	}

	expected := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("1Gi"),
			corev1.ResourceCPU:    resource.MustParse("500m"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("1Gi"),
			corev1.ResourceCPU:    resource.MustParse("500m"),
		},
	}

	result, err := ResourceRequirements(resources)
	if err != nil {
		t.Fatalf("ResourceRequirements returned error: %v", err)
	}

	if len(result.Requests) != len(expected.Requests) {
		t.Fatalf("unexpected number of resource requests")
	}
	if result.Requests[corev1.ResourceCPU] != expected.Requests[corev1.ResourceCPU] {
		t.Fatalf("unexpected cpu request")
	}
	if result.Requests[corev1.ResourceMemory] != expected.Requests[corev1.ResourceMemory] {
		t.Fatalf("unexpected memory request")
	}
}

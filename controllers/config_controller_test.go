package controllers

import (
	"context"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"testing"

	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestHDFSClusterReconciler_desiredClusterConfigMap(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Create a HDFSCluster object
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hdfs-cluster",
			Namespace: "default",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.Node{
				Replicas: 1,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}

	// Call desiredClusterConfigMap
	cm, err := r.desiredClusterConfigMap(hdfsCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check the name and namespace of the returned ConfigMap
	if cm.Name != hdfsCluster.Name+"-cluster-config" || cm.Namespace != hdfsCluster.Namespace {
		t.Fatalf("unexpected ConfigMap name or namespace: got %s/%s, want %s/%s",
			cm.Namespace, cm.Name, hdfsCluster.Namespace, hdfsCluster.Name)
	}

	// Check the labels of the returned ConfigMap
	if cm.Labels["app"] != hdfsCluster.Name {
		t.Fatalf("unexpected label: got %s, want %s",
			cm.Labels["app"], hdfsCluster.Name)
	}

	// Check that the ConfigMap data includes the expected keys
	if _, ok := cm.Data["core-site.xml"]; !ok {
		t.Fatal("expected key 'core-site.xml' not found in ConfigMap data")
	}
	if _, ok := cm.Data["hdfs-site.xml"]; !ok {
		t.Fatal("expected key 'hdfs-site.xml' not found in ConfigMap data")
	}

	// Check that the ConfigMap owner reference has been set correctly
	if len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].UID != hdfsCluster.UID {
		t.Fatal("unexpected owner reference in ConfigMap")
	}
}

func TestHDFSClusterReconciler_createOrUpdateConfigmap(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Create a HDFSCluster object
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hdfs-cluster",
			Namespace: "default",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.Node{
				Replicas: 1,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}
	logger := log.FromContext(context.Background())
	// Call createOrUpdateConfigmap - this should create a new ConfigMap
	if err := r.createOrUpdateConfigmap(context.Background(), hdfsCluster, logger); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify that the ConfigMap was created
	existingConfigMap := &corev1.ConfigMap{}
	if err := r.Get(context.Background(), client.ObjectKey{
		Namespace: hdfsCluster.Namespace,
		Name:      hdfsCluster.Name + "-cluster-config",
	}, existingConfigMap); err != nil {
		t.Fatalf("expected ConfigMap to be created but got error: %v", err)
	}

}

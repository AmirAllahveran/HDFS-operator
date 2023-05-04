package controllers

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
)

func TestHDFSClusterReconciler_createHadoop(t *testing.T) {
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

	err := r.createHadoop(context.Background(), hdfsCluster)
	if err != nil {
		t.Fatalf("Failed to create Hadoop: %v", err)
	}

	hadoopDeploymentList := &appsv1.DeploymentList{}
	err = r.List(context.Background(), hadoopDeploymentList, client.InNamespace(hdfsCluster.Namespace))
	if err != nil {
		t.Fatalf("Failed to list Deployments: %v", err)
	}

	if len(hadoopDeploymentList.Items) != 1 {
		t.Fatalf("Expected 1 Deployment, found %d", len(hadoopDeploymentList.Items))
	}

	hadoopDeployment := hadoopDeploymentList.Items[0]
	desiredHadoopDeployment, _ := r.desiredHadoopDeployment(hdfsCluster)
	if hadoopDeployment.Name != desiredHadoopDeployment.Name {
		t.Errorf("Hadoop Deployment name mismatch: expected '%s', got '%s'", desiredHadoopDeployment.Name, hadoopDeployment.Name)
	}
}

func TestDesiredHadoopDeployment(t *testing.T) {
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

	deployment, err := r.desiredHadoopDeployment(hdfsCluster)
	if err != nil {
		t.Fatalf("Failed to create desired Hadoop Deployment: %v", err)
	}

	expectedName := hdfsCluster.Name + "-hadoop"
	if deployment.Name != expectedName {
		t.Errorf("Deployment name mismatch: expected '%s', got '%s'", expectedName, deployment.Name)
	}

	if deployment.Namespace != hdfsCluster.Namespace {
		t.Errorf("Deployment namespace mismatch: expected '%s', got '%s'", hdfsCluster.Namespace, deployment.Namespace)
	}

	// Test other desired properties of the Hadoop Deployment
}

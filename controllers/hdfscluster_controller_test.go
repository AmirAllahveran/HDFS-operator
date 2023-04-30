package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestHDFSClusterReconciler_createOrUpdateComponents(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Create a HDFSCluster object
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hdfs-cluster",
			Namespace: "default",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			DataNode: v1alpha1.DataNode{
				Resources: v1alpha1.Resources{
					Storage: "10Gi",
				},
			},
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}
	ctx := context.Background()

	err := r.createOrUpdateComponents(ctx, hdfsCluster)
	require.NoError(t, err)

	// Call desiredClusterConfigMap
	desiredConfigMap, err := r.desiredClusterConfigMap(hdfsCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Check that the ConfigMap was created
	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desiredConfigMap), cm)
	require.NoError(t, err)
}

package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestHDFSClusterReconciler_desiredNameNodeService(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	//_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Initialize a mock HDFSCluster
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hdfs",
			Namespace: "default",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.NameNode{
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

	svc, err := r.desiredNameNodeService(hdfsCluster)
	assert.Nil(t, err)

	expectedName := hdfsCluster.Name + "-namenode"
	assert.Equal(t, expectedName, svc.Name)

	expectedNamespace := hdfsCluster.Namespace
	assert.Equal(t, expectedNamespace, svc.Namespace)

	expectedLabels := map[string]string{
		"cluster":   hdfsCluster.Name,
		"app":       "hdfsCluster",
		"component": "namenode",
	}
	assert.Equal(t, expectedLabels, svc.Labels)

	expectedPorts := []corev1.ServicePort{
		{
			Name:       "web",
			Port:       9870,
			TargetPort: intstr.FromString("9870"),
		},
		{
			Name:       "default",
			Port:       8020,
			TargetPort: intstr.FromString("8020"),
		},
	}
	assert.Equal(t, expectedPorts, svc.Spec.Ports)
}

func TestHDFSClusterReconciler_desiredNameNodeStatefulSet(t *testing.T) {
	// Use the same setup as before
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hdfs",
			Namespace: "default",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.NameNode{
				Replicas: 1,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
		},
	}

	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}

	sts, err := r.desiredSingleNameNodeStatefulSet(hdfsCluster)
	assert.Nil(t, err)

	expectedName := hdfsCluster.Name + "-namenode"
	assert.Equal(t, expectedName, sts.Name)

	expectedNamespace := hdfsCluster.Namespace
	assert.Equal(t, expectedNamespace, sts.Namespace)

	expectedLabels := map[string]string{
		"cluster":   hdfsCluster.Name,
		"app":       "hdfsCluster",
		"component": "namenode",
	}
	assert.Equal(t, expectedLabels, sts.Labels)

	assert.Equal(t, hdfsCluster.Name+"-namenode-service", sts.Spec.ServiceName)

	//expectedReplicas := stringToInt32(hdfsCluster.Spec.NameNode.Replicas)
	//assert.Equal(t, expectedReplicas, *sts.Spec.Replicas)
	// Check the number of replicas
	if *sts.Spec.Replicas != 1 {
		t.Errorf("expected 3 replicas, got: %d", *sts.Spec.Replicas)
	}

	// ... Continue with assertions for the rest of the StatefulSet fields

	// Now test for a case where SetControllerReference returns an error
	// Here we pass a nil scheme which will cause the SetControllerReference function to return an error.
	//r = &HDFSClusterReconciler{Scheme: nil}
	//sts, err = r.desiredNameNodeStatefulSet(hdfsCluster)
	//assert.Nil(t, sts)
	//assert.NotNil(t, err)
}

func TestHDFSClusterReconciler_createOrUpdateNameNode(t *testing.T) {
	// Use the same setup as before
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hdfs",
			Namespace: "default",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.NameNode{
				Replicas: 1,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
		},
	}

	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}

	ctx := context.Background()

	// Test when Service and StatefulSet do not exist
	err := r.createOrUpdateNameNode(ctx, hdfsCluster)
	assert.Nil(t, err)

	// Now the Service and StatefulSet should exist
	service := &corev1.Service{}
	err = r.Get(ctx, client.ObjectKey{
		Namespace: hdfsCluster.Namespace,
		Name:      hdfsCluster.Name + "-namenode",
	}, service)
	assert.Nil(t, err)

	statefulSet := &appsv1.StatefulSet{}
	err = r.Get(ctx, client.ObjectKey{
		Namespace: hdfsCluster.Namespace,
		Name:      hdfsCluster.Name + "-namenode",
	}, statefulSet)
	assert.Nil(t, err)

	// Test when Service and StatefulSet already exist
	err = r.createOrUpdateNameNode(ctx, hdfsCluster)
	assert.Nil(t, err)
}

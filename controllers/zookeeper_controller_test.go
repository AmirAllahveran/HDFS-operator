package controllers

import (
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestHDFSClusterReconciler_desiredZookeeperService(t *testing.T) {
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
				Replicas: "2",
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
			JournalNode: v1alpha1.JournalNode{
				Replicas: "1",
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
			Zookeeper: v1alpha1.Zookeeper{
				Replicas: "1",
				Resources: v1alpha1.Resources{
					Storage: "3Gi",
				},
			},
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}

	svc, err := r.desiredZookeeperService(hdfsCluster)
	assert.Nil(t, err)

	expectedName := hdfsCluster.Name + "-zookeeper"
	assert.Equal(t, expectedName, svc.Name)

	expectedNamespace := hdfsCluster.Namespace
	assert.Equal(t, expectedNamespace, svc.Namespace)

	expectedLabels := map[string]string{
		"cluster":   hdfsCluster.Name,
		"app":       "hdfsCluster",
		"component": "zookeeper",
	}
	assert.Equal(t, expectedLabels, svc.Labels)

	expectedPorts := []corev1.ServicePort{
		{
			Name:       "server",
			Port:       2888,
			TargetPort: intstr.FromString("server"),
		},
		{
			Name:       "leader-election",
			Port:       3888,
			TargetPort: intstr.FromString("leader-election"),
		},
	}
	assert.Equal(t, expectedPorts, svc.Spec.Ports)
}

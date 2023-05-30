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
			NameNode: v1alpha1.Node{
				Replicas: 2,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
			JournalNode: v1alpha1.Node{
				Replicas: 1,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
			Zookeeper: v1alpha1.Node{
				Replicas: 1,
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
			Name:       "client",
			Port:       2181,
			TargetPort: intstr.FromString("client"),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "server",
			Port:       2888,
			TargetPort: intstr.FromString("server"),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "leader-election",
			Port:       3888,
			TargetPort: intstr.FromString("leader-election"),
			Protocol:   corev1.ProtocolTCP,
		},
	}
	assert.Equal(t, expectedPorts, svc.Spec.Ports)
}

func TestHDFSClusterReconciler_desiredZookeeperConfigMap(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	//_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Initialize a mock HDFSCluster
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "test-namespace",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.Node{
				Replicas: 2,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
			JournalNode: v1alpha1.Node{
				Replicas: 1,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
			Zookeeper: v1alpha1.Node{
				Replicas: 1,
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

	// Executing the function to test
	configMap, err := r.desiredZookeeperConfigMap(hdfsCluster)

	// Assert that there's no error
	assert.Nil(t, err)

	// Assert that returned ConfigMap is not nil
	assert.NotNil(t, configMap)

	// Assert that the ConfigMap metadata is correctly set
	assert.Equal(t, "test-cluster-zookeeper-script", configMap.ObjectMeta.Name)
	assert.Equal(t, "test-namespace", configMap.ObjectMeta.Namespace)
	assert.Equal(t, hdfsCluster.Name, configMap.ObjectMeta.Labels["cluster"])
	assert.Equal(t, "hdfsCluster", configMap.ObjectMeta.Labels["app"])
	assert.Equal(t, "zookeeper", configMap.ObjectMeta.Labels["component"])

	// Assert that the Data in ConfigMap is as expected
	assert.Equal(t, "#!/bin/bash", configMap.Data["init-certs.sh"])
	_, setupPresent := configMap.Data["setup.sh"]
	assert.True(t, setupPresent, "setup.sh script should be present in the ConfigMap data")

	// Assert that the owner reference is set correctly
	assert.Equal(t, 1, len(configMap.OwnerReferences))
	assert.Equal(t, hdfsCluster.Name, configMap.OwnerReferences[0].Name)
}

func TestHDFSClusterReconciler_desiredZookeeperStatefulSet(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	//_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Initialize a mock HDFSCluster
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "test-namespace",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.Node{
				Replicas: 2,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
			JournalNode: v1alpha1.Node{
				Replicas: 1,
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
			Zookeeper: v1alpha1.Node{
				Replicas: 1,
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
	t.Run("valid cluster definition", func(t *testing.T) {
		statefulSet, err := r.desiredZookeeperStatefulSet(hdfsCluster)
		assert.NoError(t, err)

		// Verify the returned stateful set has the expected properties.
		assert.Equal(t, "test-cluster-zookeeper", statefulSet.Name)
		assert.Equal(t, "test-namespace", statefulSet.Namespace)
		assert.Equal(t, int32(3), *statefulSet.Spec.Replicas)

		// Verify ControllerReference
		ownerRefs := statefulSet.OwnerReferences
		assert.NotEmpty(t, ownerRefs)
		assert.Equal(t, "HDFSCluster", ownerRefs[0].Kind)
		assert.Equal(t, "test-cluster", ownerRefs[0].Name)
	})
}

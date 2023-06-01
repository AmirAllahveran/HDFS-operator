package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestDesiredJournalNode(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = v1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	hdfs := &v1alpha1.HDFSCluster{
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.Node{
				Replicas: 2,
			},
			JournalNode: v1alpha1.Node{
				Replicas: 3,
			},
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testCluster",
			Namespace: "testNamespace",
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfs).Build(),
		Scheme: s,
	}

	// Act
	pdb, err := r.desiredJournalNodePodDisruptionBudget(hdfs)

	// Assert
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expectedMinAvailable := hdfs.Spec.JournalNode.Replicas/2 + 1
	if pdb.Spec.MinAvailable.IntVal != int32(expectedMinAvailable) {
		t.Errorf("Expected MinAvailable to be %d but got %d", expectedMinAvailable, pdb.Spec.MinAvailable.IntVal)
	}

	expectedName := hdfs.Name + "-journalnode"
	if pdb.ObjectMeta.Name != expectedName {
		t.Errorf("Expected Name to be %s but got %s", expectedName, pdb.ObjectMeta.Name)
	}

	expectedNamespace := hdfs.Namespace
	if pdb.ObjectMeta.Namespace != expectedNamespace {
		t.Errorf("Expected Namespace to be %s but got %s", expectedNamespace, pdb.ObjectMeta.Namespace)
	}

	if pdb.Spec.Selector.MatchLabels["cluster"] != hdfs.Name || pdb.Spec.Selector.MatchLabels["app"] != "hdfsCluster" || pdb.Spec.Selector.MatchLabels["component"] != "journalnode" {
		t.Errorf("Unexpected labels in PodDisruptionBudget Selector")
	}
}

func TestHDFSClusterReconciler_desiredJournalNodeService(t *testing.T) {
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
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}

	svc, err := r.desiredJournalNodeService(hdfsCluster)
	assert.Nil(t, err)

	expectedName := hdfsCluster.Name + "-journalnode"
	assert.Equal(t, expectedName, svc.Name)

	expectedNamespace := hdfsCluster.Namespace
	assert.Equal(t, expectedNamespace, svc.Namespace)

	expectedLabels := map[string]string{
		"cluster":   hdfsCluster.Name,
		"app":       "hdfsCluster",
		"component": "journalnode",
	}
	assert.Equal(t, expectedLabels, svc.Labels)

	expectedPorts := []corev1.ServicePort{
		{
			Name:       "http",
			Port:       8480,
			TargetPort: intstr.FromString("http"),
		},
		{
			Name:       "rpc",
			Port:       8485,
			TargetPort: intstr.FromString("rpc"),
		},
	}
	assert.Equal(t, expectedPorts, svc.Spec.Ports)
}

func TestHDFSClusterReconciler_createOrUpdateJournalNode(t *testing.T) {
	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = v1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Create a HDFSCluster object
	hdfs := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hdfs-cluster",
			Namespace: "default",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			NameNode: v1alpha1.Node{
				Replicas: 2,
				Resources: v1alpha1.Resources{
					Storage: "10Gi",
				},
			},
			JournalNode: v1alpha1.Node{
				Replicas: 1,
				Resources: v1alpha1.Resources{
					Storage: "3G",
				},
			},
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfs).Build(),
		Scheme: s,
	}

	ctx := context.Background()
	// Act
	err := r.createOrUpdateJournalNode(ctx, hdfs)

	// Assert
	assert.NoError(t, err)

	// Check if the Service, PodDisruptionBudget, and StatefulSet were created
	service := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: hdfs.Name + "-journalnode", Namespace: hdfs.Namespace}, service)
	assert.NoError(t, err)

	pdb := &v1.PodDisruptionBudget{}
	err = r.Get(ctx, types.NamespacedName{Name: hdfs.Name + "-journalnode", Namespace: hdfs.Namespace}, pdb)
	assert.NoError(t, err)

	statefulSet := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: hdfs.Name + "-journalnode", Namespace: hdfs.Namespace}, statefulSet)
	assert.NoError(t, err)
}

func TestHDFSClusterReconciler_desiredJournalNodeStatefulSet(t *testing.T) {

	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	hdfs := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testCluster",
			Namespace: "testNamespace",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			JournalNode: v1alpha1.Node{
				Replicas: 3,
				Resources: v1alpha1.Resources{
					Storage: "10Gi",
				},
			},
		},
	}
	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfs).Build(),
		Scheme: s,
	}

	// Act
	sts, err := r.desiredJournalNodeStatefulSet(hdfs)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, sts)
	assert.Equal(t, hdfs.Name+"-journalnode", sts.Name)
	assert.Equal(t, hdfs.Namespace, sts.Namespace)

	// Check the volume claim templates
	expectedStorage := resource.MustParse(hdfs.Spec.JournalNode.Resources.Storage)
	if sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String() != expectedStorage.String() {
		t.Errorf("expected storage to be '%s', got: '%s'", hdfs.Spec.DataNode.Resources.Storage,
			sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String())
	}
}

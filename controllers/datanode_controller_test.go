package controllers

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"

	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDesiredDataNodeConfigMap(t *testing.T) {

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
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfsCluster).Build(),
		Scheme: s,
	}

	configMap, _ := r.desiredDataNodeConfigMap(hdfsCluster)

	expectedName := hdfsCluster.Name
	expectedNamespace := hdfsCluster.Namespace
	expectedLabels := map[string]string{
		"app": hdfsCluster.Name,
	}

	if configMap.Name != expectedName {
		t.Errorf("Expected name to be %s, but got %s", expectedName, configMap.Name)
	}

	if configMap.Namespace != expectedNamespace {
		t.Errorf("Expected namespace to be %s, but got %s", expectedNamespace, configMap.Namespace)
	}

	for key, value := range expectedLabels {
		if configMap.Labels[key] != value {
			t.Errorf("Expected label %s to be %s, but got %s", key, value, configMap.Labels[key])
		}
	}

	expectedData := map[string]string{
		"check-status.sh": `#!/usr/bin/env bash
set -o errexit
set -o errtrace
set -o nounset
set -o pipefail
set -o xtrace
_PORTS="50075 1006"
_URL_PATH="jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo"
_CLUSTER_ID=""
for _PORT in $_PORTS; do
  _CLUSTER_ID+=$(curl -s http://localhost:${_PORT}/$_URL_PATH |  \
      grep ClusterId) || true
done
echo $_CLUSTER_ID | grep -q -v null`,
	}

	for key, value := range expectedData {
		if configMap.Data[key] != value {
			t.Errorf("Expected data for key %s to be %s, but got %s", key, value, configMap.Data[key])
		}
	}

	// Check that the ConfigMap owner reference has been set correctly
	if len(configMap.OwnerReferences) != 1 || configMap.OwnerReferences[0].UID != hdfsCluster.UID {
		t.Fatal("unexpected owner reference in ConfigMap")
	}
}

func TestHDFSClusterReconciler_createOrUpdateDataNode(t *testing.T) {
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

	// Call the method under test
	err := r.createOrUpdateDataNode(context.Background(), hdfsCluster)
	if err != nil {
		t.Fatalf("Failed to create or update DataNode: %v", err)
	}

	// Check that the ConfigMap and StatefulSet were created or updated
	configMap := &corev1.ConfigMap{}
	err = r.Get(context.Background(), client.ObjectKey{
		Namespace: hdfsCluster.Namespace,
		Name:      hdfsCluster.Name + "-datanode-script",
	}, configMap)
	if err != nil {
		t.Errorf("Failed to get ConfigMap: %v", err)
	}

	statefulSet := &appsv1.StatefulSet{}
	err = r.Get(context.Background(), client.ObjectKey{
		Namespace: hdfsCluster.Namespace,
		Name:      hdfsCluster.Name + "-datanode",
	}, statefulSet)
	if err != nil {
		t.Errorf("Failed to get StatefulSet: %v", err)
	}
}

func TestHDFSClusterReconciler_desiredDataNodeStatefulSet(t *testing.T) {

	// Set up a fake client to mock API calls
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s) // Add your custom resource to the scheme

	// Initialize a mock HDFSCluster
	hdfs := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hdfs",
			Namespace: "default",
		},
		Spec: v1alpha1.HDFSClusterSpec{
			DataNode: v1alpha1.DataNode{
				Replicas: "3",
				Resources: v1alpha1.Resources{
					Storage: "1Gi",
				},
			},
		},
	}

	// Create a Reconciler instance with the fake client
	r := &HDFSClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(hdfs).Build(),
		Scheme: s,
	}

	sts, err := r.desiredDataNodeStatefulSet(hdfs)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Check the StatefulSet name
	if sts.Name != "test-hdfs-datanode" {
		t.Errorf("expected name to be 'test-hdfs-datanode', got: %s", sts.Name)
	}

	// Check the StatefulSet namespace
	if sts.Namespace != "default" {
		t.Errorf("expected namespace to be 'default', got: %s", sts.Namespace)
	}

	// Check the number of replicas
	if *sts.Spec.Replicas != 3 {
		t.Errorf("expected 3 replicas, got: %d", *sts.Spec.Replicas)
	}

	// Check the StatefulSet labels
	expectedLabels := map[string]string{
		"cluster":   "test-hdfs",
		"app":       "hdfsCluster",
		"component": "datanode",
	}
	if !reflect.DeepEqual(sts.Labels, expectedLabels) {
		t.Errorf("expected labels to be '%v', got: %v", expectedLabels, sts.Labels)
	}

	// Check the container image
	if sts.Spec.Template.Spec.Containers[0].Image != "uhopper/hadoop-datanode:2.7.2" {
		t.Errorf("expected image to be 'uhopper/hadoop-datanode:2.7.2', got: %s", sts.Spec.Template.Spec.Containers[0].Image)
	}

	// Check the volume claim templates
	expectedStorage := resource.MustParse(hdfs.Spec.DataNode.Resources.Storage)
	if sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String() != expectedStorage.String() {
		t.Errorf("expected storage to be '%s', got: '%s'", hdfs.Spec.DataNode.Resources.Storage,
			sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String())
	}
}

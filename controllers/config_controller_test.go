package controllers

import (
	"testing"

	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDesiredClusterConfigMap(t *testing.T) {
	reconciler := &HDFSClusterReconciler{}
	hdfsCluster := &v1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "test-namespace",
		},
	}

	configMap, err := reconciler.DesiredClusterConfigMap(hdfsCluster)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

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

	if _, ok := configMap.Data["core-site.xml"]; !ok {
		t.Errorf("Expected ConfigMap data to have key 'core-site.xml'")
	}

	if _, ok := configMap.Data["hdfs-site.xml"]; !ok {
		t.Errorf("Expected ConfigMap data to have key 'hdfs-site.xml'")
	}

	if configMap.OwnerReferences[0].Name != hdfsCluster.Name {
		t.Errorf("Expected ConfigMap to have owner reference to '%s', got '%s'", hdfsCluster.Name, configMap.OwnerReferences[0].Name)
	}
}

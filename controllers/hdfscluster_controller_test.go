package controllers

import (
	"context"
	hdfsv1alpha1 "github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"testing"
)

func TestCreateComponents(t *testing.T) {
	// Arrange
	logger := zap.New(zap.WriteTo(os.Stdout), zap.UseDevMode(true))
	clientSet := fake.NewSimpleClientset()
	hdfs := &hdfsv1alpha1.HDFSCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}
	reconciler := HDFSClusterReconciler{}
	ctx := context.Background()

	// Act
	err := reconciler.createComponents(ctx, clientSet, hdfs, logger)

	// Assert
	assert.Nil(t, err)

	cm, err := clientSet.CoreV1().ConfigMaps(hdfs.Namespace).Get(ctx, hdfs.Name, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.NotNil(t, cm)
	assert.Equal(t, hdfs.Name, cm.Name)
	assert.Equal(t, hdfs.Namespace, cm.Namespace)
}

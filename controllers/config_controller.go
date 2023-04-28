package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	hdfsv1alpha1 "github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *HDFSClusterReconciler) desiredClusterConfigMap(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.ConfigMap, error) {
	cmTemplate := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name,
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"app": hdfsCluster.Name,
			},
		},
		Data: map[string]string{
			"core-site.xml": `<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://NAMENODE_HOST:9000</value>
  </property>
</configuration>`,
			"hdfs-site.xml": `<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>` + hdfsCluster.Spec.ClusterConfig.DfsReplication + `</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///hdfs/namenode</value>
  </property>
</configuration>
`,
		},
	}
	if err := ctrl.SetControllerReference(hdfsCluster, cmTemplate, r.Scheme); err != nil {
		return cmTemplate, err
	}

	return cmTemplate, nil
}

//func (r *HDFSClusterReconciler) clusterConfigExists(ctx context.Context, hdfsCluster *v1alpha1.HDFSCluster) (bool, error) {
//	// Define the desired ConfigMap object
//	desiredConfigMap, _ := r.desiredClusterConfigMap(hdfsCluster)
//	if desiredConfigMap == nil {
//		return false, fmt.Errorf("desiredClusterConfigMap returned nil")
//	}
//
//	// Check if the ConfigMap already exists
//	existingConfigMap := &corev1.ConfigMap{}
//	err := r.Get(ctx, client.ObjectKeyFromObject(desiredConfigMap), existingConfigMap)
//
//	if err != nil {
//		if errors.IsNotFound(err) {
//			// ConfigMap was not found
//			return false, nil
//		}
//		// An error occurred
//		return false, err
//	}
//
//	// ConfigMap was found
//	return true, nil
//}

func (r *HDFSClusterReconciler) createOrUpdateConfigmap(ctx context.Context, hdfs *hdfsv1alpha1.HDFSCluster) error {
	// Define the desired NameNode Service object
	desiredConfigMap, _ := r.desiredClusterConfigMap(hdfs)

	// Check if the Service already exists
	existingConfigMap := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredConfigMap), existingConfigMap)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredConfigMap); err != nil {
			return err
		}
	} else {
		existingConfigMap.Data = desiredConfigMap.Data
		if err := r.Update(ctx, existingConfigMap); err != nil {
			return err
		}
	}

	return nil
}

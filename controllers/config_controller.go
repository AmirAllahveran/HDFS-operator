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
	"strconv"
	"time"
)

func (r *HDFSClusterReconciler) desiredClusterConfigMap(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.ConfigMap, error) {

	customCoreSite := ""
	for _, item := range hdfsCluster.Spec.ClusterConfig.CustomHadoopConfig.CoreSite {
		property := `  <property>
	<name>` + item.Name + `</name>
	<value>` + item.Value + `</value>
</property>`
		customCoreSite = customCoreSite + property + "\n"
	}

	customHdfsSite := ""
	for _, item := range hdfsCluster.Spec.ClusterConfig.CustomHadoopConfig.HdfsSite {
		property := `  <property>
	<name>` + item.Name + `</name>
	<value>` + item.Value + `</value>
</property>`
		customHdfsSite = customHdfsSite + property + "\n"
	}

	cmTemplate := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-cluster-config",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"app": hdfsCluster.Name,
			},
		},
		Data: map[string]string{
			"core-site.xml": `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://` + hdfsCluster.Name + "-namenode." + hdfsCluster.Namespace + `.svc.cluster.local:9000</value>
    <description>The default filesystem URI.</description>
  </property>
  <property>
    <name>io.file.buffer.size</name>
    <value>131072</value>
    <description>The size of buffer for use in sequence files.</description>
  </property>` + customCoreSite + `
</configuration>`,
			"hdfs-site.xml": `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
     <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
     <value>false</value>
     <description>Use IP address instead of hostname for communication between NameNode and DataNodes</description>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/data/hadoop/namenode</value>
    <description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/data/hadoop/datanode</value>
    <description>Comma-separated list of paths on the local filesystem of a DataNode where it stores its blocks.</description>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>` + hdfsCluster.Spec.ClusterConfig.DfsReplication + `</value>
    <description>Default block replication. The actual number of replications can be specified when the file is created.</description>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>true</value>
    <description>If "true", enable permission checking in HDFS. If "false", permission checking is turned off, but all other behavior is unchanged.</description>
  </property>` + customHdfsSite + `
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
		hdfs.Status.CreationTime = time.Now().String()
		errStatus := r.Status().Update(ctx, hdfs)
		if errStatus != nil {
			return errStatus
		}
	} else {
		dataNodeReplica, _ := strconv.ParseInt(hdfs.Spec.DataNode.Replicas, 10, 32)
		err = r.scaleStatefulSet(ctx, hdfs.Name+"-datanode", hdfs.Namespace, 0)
		err = r.scaleStatefulSet(ctx, hdfs.Name+"-namenode", hdfs.Namespace, 0)
		err = r.scaleDeployment(ctx, hdfs.Name+"-hadoop", hdfs.Namespace, 0)
		existingConfigMap.Data = desiredConfigMap.Data
		if err := r.Update(ctx, existingConfigMap); err != nil {
			return err
		}
		err = r.scaleDeployment(ctx, hdfs.Name+"-hadoop", hdfs.Namespace, 1)
		err = r.scaleStatefulSet(ctx, hdfs.Name+"-namenode", hdfs.Namespace, 1)
		err = r.scaleStatefulSet(ctx, hdfs.Name+"-datanode", hdfs.Namespace, int32(dataNodeReplica))
	}

	return nil
}

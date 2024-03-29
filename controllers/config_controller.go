package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"time"
)

func (r *HDFSClusterReconciler) desiredClusterConfigMap(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.ConfigMap, error) {
	coreSite := ""
	hdfsSite := ""
	if hdfsCluster.Spec.NameNode.Replicas == 1 {
		coreSite = configCoreSiteSingle(hdfsCluster)
		hdfsSite = configHdfsSiteSingle(hdfsCluster)
	} else {
		coreSite = configCoreSiteHA(hdfsCluster)
		hdfsSite = configHdfsSiteHA(hdfsCluster)
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
<configuration>` + coreSite + `</configuration>`,
			"hdfs-site.xml": `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>` + hdfsSite + `</configuration>`,
		},
	}
	if err := ctrl.SetControllerReference(hdfsCluster, cmTemplate, r.Scheme); err != nil {
		return cmTemplate, err
	}

	return cmTemplate, nil
}

func configCoreSiteSingle(hdfsCluster *v1alpha1.HDFSCluster) string {
	coreSite := make(map[string]string)
	coreSite["fs.defaultFS"] = "hdfs://" + hdfsCluster.Name + "-namenode-0." + hdfsCluster.Name + "-namenode." + hdfsCluster.Namespace + ".svc.cluster.local:8020"
	for key, val := range hdfsCluster.Spec.ClusterConfig.CoreSite {
		coreSite[key] = val
	}
	return mapToXml(coreSite)
}

func configCoreSiteHA(hdfsCluster *v1alpha1.HDFSCluster) string {
	zookeeperQuorum := hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:2181"
	if hdfsCluster.Spec.Zookeeper.Replicas == 3 {
		zookeeperQuorum = hdfsCluster.Name + "-zookeeper-0." + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:2181," +
			hdfsCluster.Name + "-zookeeper-1." + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:2181," +
			hdfsCluster.Name + "-zookeeper-2." + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:2181"
	}

	coreSite := make(map[string]string)
	coreSite["fs.defaultFS"] = "hdfs://hdfs-k8s"
	coreSite["ha.zookeeper.quorum"] = zookeeperQuorum

	for key, val := range hdfsCluster.Spec.ClusterConfig.CoreSite {
		coreSite[key] = val
	}
	return mapToXml(coreSite)
}

func configHdfsSiteSingle(hdfsCluster *v1alpha1.HDFSCluster) string {
	hdfsSite := make(map[string]string)
	hdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"] = "false"
	hdfsSite["dfs.namenode.name.dir"] = "/data/hadoop/namenode"
	hdfsSite["dfs.datanode.data.dir"] = "/data/hadoop/datanode"
	hdfsSite["dfs.replication"] = "1"
	hdfsSite["dfs.permissions.enabled"] = "true"
	for key, val := range hdfsCluster.Spec.ClusterConfig.HdfsSite {
		hdfsSite[key] = val
	}
	return mapToXml(hdfsSite)
}

func configHdfsSiteHA(hdfsCluster *v1alpha1.HDFSCluster) string {
	qjournal := hdfsCluster.Name + "-journalnode-0." + hdfsCluster.Name + "-journalnode." + hdfsCluster.Namespace + ".svc.cluster.local:8485"
	if hdfsCluster.Spec.JournalNode.Replicas == 3 {
		qjournal = hdfsCluster.Name + "-journalnode-0." + hdfsCluster.Name + "-journalnode." + hdfsCluster.Namespace + ".svc.cluster.local:8485;" +
			hdfsCluster.Name + "-journalnode-1." + hdfsCluster.Name + "-journalnode." + hdfsCluster.Namespace + ".svc.cluster.local:8485;" +
			hdfsCluster.Name + "-journalnode-2." + hdfsCluster.Name + "-journalnode." + hdfsCluster.Namespace + ".svc.cluster.local:8485"
	}

	hdfsSite := make(map[string]string)
	hdfsSite["dfs.nameservices"] = "hdfs-k8s"
	hdfsSite["dfs.ha.namenodes.hdfs-k8s"] = "nn0,nn1"
	hdfsSite["dfs.namenode.rpc-address.hdfs-k8s.nn0"] = hdfsCluster.Name + "-namenode-0." + hdfsCluster.Name +
		"-namenode." + hdfsCluster.Namespace + ".svc.cluster.local:8020"
	hdfsSite["dfs.namenode.rpc-address.hdfs-k8s.nn1"] = hdfsCluster.Name + "-namenode-1." + hdfsCluster.Name +
		"-namenode." + hdfsCluster.Namespace + ".svc.cluster.local:8020"
	hdfsSite["dfs.namenode.shared.edits.dir"] = "qjournal://" + qjournal + "/hdfs-k8s"
	hdfsSite["dfs.client.failover.proxy.provider.hdfs-k8s"] = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
	hdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"] = "false"
	hdfsSite["dfs.namenode.name.dir"] = "/data/hadoop/namenode"
	hdfsSite["dfs.datanode.data.dir"] = "/data/hadoop/datanode"
	hdfsSite["dfs.journalnode.edits.dir"] = "/data/hadoop/journalnode"
	hdfsSite["dfs.replication"] = "1"
	hdfsSite["dfs.permissions.enabled"] = "true"
	hdfsSite["dfs.ha.fencing.methods"] = "shell(/bin/true)"
	hdfsSite["dfs.ha.automatic-failover.enabled"] = "true"
	for key, val := range hdfsCluster.Spec.ClusterConfig.HdfsSite {
		hdfsSite[key] = val
	}
	return mapToXml(hdfsSite)
}

func (r *HDFSClusterReconciler) createOrUpdateConfigmap(ctx context.Context, hdfs *v1alpha1.HDFSCluster, logger logr.Logger) error {
	// Define the desired NameNode Service object
	desiredConfigMap, _ := r.desiredClusterConfigMap(hdfs)

	// Check if the Service already exists
	existingConfigMap := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredConfigMap), existingConfigMap)
	if err != nil && !errors.IsNotFound(err) {
		logger.Error(err, "Error occurred during Get configmap")
		return err
	}
	updateJN := false
	updateZK := false
	if hdfs.Spec.NameNode.Replicas == 2 {
		existingJournalNodeStatefulSet := &appsv1.StatefulSet{}
		errJN := r.Get(ctx, client.ObjectKey{
			Namespace: hdfs.Namespace,
			Name:      hdfs.Name + "-journalnode",
		}, existingJournalNodeStatefulSet)
		if errJN != nil && !errors.IsNotFound(errJN) {
			return err
		}
		if errors.IsNotFound(errJN) {
			updateJN = false
		} else if strconv.Itoa(int(*existingJournalNodeStatefulSet.Spec.Replicas)) != strconv.Itoa(hdfs.Spec.JournalNode.Replicas) {
			updateJN = true
		}

		existingZookeeperStatefulSet := &appsv1.StatefulSet{}
		errZK := r.Get(ctx, client.ObjectKey{
			Namespace: hdfs.Namespace,
			Name:      hdfs.Name + "-zookeeper",
		}, existingZookeeperStatefulSet)
		if errZK != nil && !errors.IsNotFound(errZK) {
			return err
		}

		if errors.IsNotFound(errZK) {
			updateZK = false
		} else if strconv.Itoa(int(*existingZookeeperStatefulSet.Spec.Replicas)) != strconv.Itoa(hdfs.Spec.Zookeeper.Replicas) {
			updateZK = true
		}
	}

	// Create or update the Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredConfigMap); err != nil {
			logger.Error(err, "Error occurred during Create configmap")
			return err
		}
		for {
			existingConfigMap := &corev1.ConfigMap{}
			err := r.Get(ctx, client.ObjectKeyFromObject(desiredConfigMap), existingConfigMap)
			if err != nil && !errors.IsNotFound(err) {
				return err
			}
			if errors.IsNotFound(err) {
				logger.Info("waiting to create cluster config ...")
				time.Sleep(100 * time.Millisecond)
			} else {
				break
			}
		}
		//hdfs.Status.CreationTime = time.Now().String()
		//errStatus := r.Status().Update(ctx, hdfs)
		//if errStatus != nil {
		//	return errStatus
		//}
	} else if !compareXML(desiredConfigMap.Data["hdfs-site.xml"], existingConfigMap.Data["hdfs-site.xml"]) ||
		!compareXML(desiredConfigMap.Data["core-site.xml"], existingConfigMap.Data["core-site.xml"]) || updateJN || updateZK {
		logger.Info("updating configmap")
		logger.Info("updateJN : " + strconv.FormatBool(updateJN))
		logger.Info("updateZK : " + strconv.FormatBool(updateZK))
		existingConfigMap.Data = desiredConfigMap.Data
		if err := r.Update(ctx, existingConfigMap); err != nil {
			return err
		}
		err = r.ScaleDownAndUpStatefulSet(ctx, hdfs.Name+"-datanode", hdfs.Namespace)
		if err != nil {
			return err
		}
		err = r.ScaleDownAndUpStatefulSet(ctx, hdfs.Name+"-namenode", hdfs.Namespace)
		if err != nil {
			return err
		}
		err = r.ScaleDownAndUpDeployment(ctx, hdfs.Name+"-hadoop", hdfs.Namespace)
		if err != nil {
			return err
		}
		if hdfs.Spec.NameNode.Replicas == 2 {
			err = r.ScaleDownAndUpStatefulSet(ctx, hdfs.Name+"-journalnode", hdfs.Namespace)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

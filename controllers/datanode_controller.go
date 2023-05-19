package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DesiredDataNodeConfigMap
// The jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo path is a query string used to access the JMX
// (Java Management Extensions) interface of the DataNode in an HDFS cluster.
// JMX is a Java technology that provides a standard way to manage and monitor Java applications.
// Hadoop exposes several metrics and management operations through JMX. The JMX interface is exposed through an
// HTTP endpoint on each Hadoop component, including DataNodes.
// In this specific query:
// service=DataNode specifies that you want to access the JMX information for the DataNode service.
// name=DataNodeInfo specifies that you want to access the DataNodeInfo MBean (Management Bean),
// which provides information about the DataNode.
// When you send an HTTP request with this query string to the DataNode's JMX HTTP endpoint
// (typically on port 50075 or 1006), it will return a JSON object containing the DataNodeInfo MBean's attributes,
// such as the cluster ID, volume failures, and other details about the DataNode's status.
func (r *HDFSClusterReconciler) desiredDataNodeConfigMap(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.ConfigMap, error) {
	dataNodeConfigMapTemplate := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-datanode-script",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "datanode",
			},
		},
		Data: map[string]string{
			"check-status.sh": `#!/usr/bin/env bash
set -o errexit
set -o errtrace
set -o nounset
set -o pipefail
set -o xtrace
_PORTS="9866 9864"
_URL_PATH="jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo"
_CLUSTER_ID=""
for _PORT in $_PORTS; do
  _CLUSTER_ID+=$(curl -s http://localhost:${_PORT}/$_URL_PATH |  \
      grep ClusterId) || true
done
echo $_CLUSTER_ID | grep -q -v null`,
		},
	}

	if err := ctrl.SetControllerReference(hdfsCluster, dataNodeConfigMapTemplate, r.Scheme); err != nil {
		return dataNodeConfigMapTemplate, err
	}

	return dataNodeConfigMapTemplate, nil
}

func (r *HDFSClusterReconciler) desiredDataNodeStatefulSet(hdfsCluster *v1alpha1.HDFSCluster) (*appsv1.StatefulSet, error) {
	stsTemplate := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-datanode",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "datanode",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"cluster":   hdfsCluster.Name,
					"app":       "hdfsCluster",
					"component": "datanode",
				},
			},
			ServiceName: hdfsCluster.Name + "-datanode",
			Replicas:    stringToInt32(hdfsCluster.Spec.DataNode.Replicas),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"cluster":   hdfsCluster.Name,
						"app":       "hdfsCluster",
						"component": "datanode",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "hdfs-datanode",
							Image: "amiralh4/datanode:3.3.1",
							Ports: []corev1.ContainerPort{
								{
									Name:          "default",
									ContainerPort: 9864,
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/bash",
											"-c",
											"/scripts/check-status.sh",
										},
									},
								},
								InitialDelaySeconds: 60,
								PeriodSeconds:       30,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/sh",
											"-c",
											"/scripts/check-status.sh",
										},
									},
								},
								InitialDelaySeconds: 60,
								PeriodSeconds:       30,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: func() *bool { b := true; return &b }(),
							},
							//Command: []string{
							//	"chmod", "+x", "/scripts/check-status.sh",
							//},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "hdfs-site",
									MountPath: "/opt/hadoop/etc/hadoop/hdfs-site.xml",
									SubPath:   "hdfs-site.xml",
								},
								{
									Name:      "core-site",
									MountPath: "/opt/hadoop/etc/hadoop/core-site.xml",
									SubPath:   "core-site.xml",
								},
								{
									Name:      "datanode-script",
									MountPath: "/scripts/check-status.sh",
									SubPath:   "check-status.sh",
								},
								{
									Name:      hdfsCluster.Name + "-datanode",
									MountPath: "/data/hadoop/datanode",
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
					Volumes: []corev1.Volume{
						{
							Name: "hdfs-site",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: hdfsCluster.Name + "-cluster-config",
									},
									Items: []corev1.KeyToPath{
										{
											Key:  "hdfs-site.xml",
											Path: "hdfs-site.xml",
										},
									},
								},
							},
						},
						{
							Name: "core-site",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: hdfsCluster.Name + "-cluster-config",
									},
									Items: []corev1.KeyToPath{
										{
											Key:  "core-site.xml",
											Path: "core-site.xml",
										},
									},
								},
							},
						},
						{
							Name: "datanode-script",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: hdfsCluster.Name + "-datanode-script",
									},
									Items: []corev1.KeyToPath{
										{
											Key:  "check-status.sh",
											Path: "check-status.sh",
										},
									},
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: hdfsCluster.Name + "-datanode",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(hdfsCluster.Spec.DataNode.Resources.Storage),
							},
						},
						//Selector: &metav1.LabelSelector{
						//	MatchLabels: map[string]string{
						//		"cluster":   hdfsCluster.Name,
						//		"app":       "hdfsCluster",
						//		"component": "datanode",
						//	},
						//},
					},
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(hdfsCluster, stsTemplate, r.Scheme); err != nil {
		return stsTemplate, err
	}

	return stsTemplate, nil
}

//func (r *HDFSClusterReconciler) dataNodesExist(ctx context.Context, hdfsCluster *v1alpha1.HDFSCluster) (bool, error) {
//	// Define the desired DataNode StatefulSet object
//	desiredStatefulSet, _ := r.desiredDataNodeStatefulSet(hdfsCluster)
//
//	// Check if the StatefulSet already exists
//	existingStatefulSet := &appsv1.StatefulSet{}
//	err := r.Get(ctx, client.ObjectKeyFromObject(desiredStatefulSet), existingStatefulSet)
//
//	if err != nil {
//		if errors.IsNotFound(err) {
//			return false, nil
//		}
//		return false, err
//	}
//
//	// Check if all DataNode pods are in Running state
//	replicas := desiredStatefulSet.Status.Replicas
//	for i := int32(0); i < replicas; i++ {
//		dataNodePod := &corev1.Pod{}
//		dataNodePodName := types.NamespacedName{
//			Namespace: hdfsCluster.Namespace,
//			Name:      fmt.Sprintf("%s-%d", desiredStatefulSet.Name, i),
//		}
//		err = r.Get(ctx, dataNodePodName, dataNodePod)
//
//		if err != nil {
//			if errors.IsNotFound(err) {
//				return false, nil
//			}
//			return false, err
//		}
//
//		if dataNodePod.Status.Phase != corev1.PodRunning {
//			return false, nil
//		}
//	}
//
//	return true, nil
//}

func (r *HDFSClusterReconciler) createOrUpdateDataNode(ctx context.Context, hdfs *v1alpha1.HDFSCluster) error {
	// Define the desired NameNode Service object
	desiredDAtaNodeConfigMap, _ := r.desiredDataNodeConfigMap(hdfs)
	desiredDataNodeStatefulSet, _ := r.desiredDataNodeStatefulSet(hdfs)

	// Check if the datanode already exists

	// Check if the Service already exists
	existingConfigMap := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredDAtaNodeConfigMap), existingConfigMap)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredDAtaNodeConfigMap); err != nil {
			return err
		}
	}

	existingStatefulSet := &appsv1.StatefulSet{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desiredDataNodeStatefulSet), existingStatefulSet)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredDataNodeStatefulSet); err != nil {
			return err
		}
		//replica, _ := strconv.Atoi(hdfs.Spec.DataNode.Replicas)
		//for i := 0; i < replica; i++ {
		//	pvc := &corev1.PersistentVolumeClaim{}
		//	retry := 0
		//	for {
		//		if err := r.Get(ctx, client.ObjectKey{
		//			Namespace: hdfs.Namespace,
		//			Name:      hdfs.Name + "-datanode-" + hdfs.Name + "-datanode-" + strconv.Itoa(i),
		//		}, pvc); err != nil {
		//			time.Sleep(time.Second * 1)
		//			retry++
		//			//continue
		//		} else {
		//			break
		//		}
		//		if retry > 10 {
		//			return err
		//		}
		//	}
		//
		//	if err := ctrl.SetControllerReference(hdfs, pvc, r.Scheme); err != nil {
		//		return err
		//	}
		//
		//	if err := r.Update(ctx, pvc); err != nil {
		//		return err
		//	}
		//}
	} else {
		existingStatefulSet.Spec = desiredDataNodeStatefulSet.Spec
		if err := r.Update(ctx, existingStatefulSet); err != nil {
			return err
		}
	}

	return nil
}

package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/url"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
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
	compute, _ := resourceRequirements(hdfsCluster.Spec.DataNode.Resources)

	var webPort int
	if val, ok := hdfsCluster.Spec.ClusterConfig.HdfsSite["dfs.namenode.http-address"]; ok {
		u, err := url.Parse("//" + val)
		if err != nil {
			return nil, err
		}
		webPort, _ = strconv.Atoi(u.Port())
	} else {
		webPort = 9870
	}

	initContainerCommand := "while [ $(curl -m 1 -s -o /dev/null -w \"%{http_code}\" http://" + hdfsCluster.Name + "-namenode." + hdfsCluster.Namespace + ".svc.cluster.local:" + strconv.Itoa(webPort) + "/index.html)!= \"200\" ]; do echo waiting; sleep 2; done"

	var datanodeDataDir string
	if val, ok := hdfsCluster.Spec.ClusterConfig.HdfsSite["dfs.datanode.data.dir"]; ok {
		datanodeDataDir = val
	} else {
		datanodeDataDir = "/data/hadoop/datanode"
	}

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
			Replicas:    int32Ptr(int32(hdfsCluster.Spec.DataNode.Replicas)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"cluster":   hdfsCluster.Name,
						"app":       "hdfsCluster",
						"component": "datanode",
					},
				},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"cluster":   hdfsCluster.Name,
												"app":       "hdfsCluster",
												"component": "datanode",
											},
										},
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "hdfs-datanode",
							Image: "amiralh4/datanode:3.3.1",
							//Ports: []corev1.ContainerPort{
							//	{
							//		Name:          "default",
							//		ContainerPort: int32(port),
							//	},
							//},
							//LivenessProbe: &corev1.Probe{
							//	ProbeHandler: corev1.ProbeHandler{
							//		Exec: &corev1.ExecAction{
							//			Command: []string{
							//				"/bin/bash",
							//				"-c",
							//				"/scripts/check-status.sh",
							//			},
							//		},
							//	},
							//	InitialDelaySeconds: 60,
							//	PeriodSeconds:       30,
							//},
							//ReadinessProbe: &corev1.Probe{
							//	ProbeHandler: corev1.ProbeHandler{
							//		Exec: &corev1.ExecAction{
							//			Command: []string{
							//				"/bin/sh",
							//				"-c",
							//				"/scripts/check-status.sh",
							//			},
							//		},
							//	},
							//	InitialDelaySeconds: 60,
							//	PeriodSeconds:       30,
							//},
							SecurityContext: &corev1.SecurityContext{
								Privileged: func() *bool { b := true; return &b }(),
							},
							Resources: *compute,
							Env: []corev1.EnvVar{
								{
									Name:  "DATA_DIR",
									Value: datanodeDataDir,
								},
							},
							Lifecycle: &corev1.Lifecycle{
								PostStart: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/sh",
											"-c",
											"rm -rf $DATA_DIR/lost+found",
										},
									},
								},
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
									MountPath: datanodeDataDir,
								},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Image:           "curlimages/curl:8.1.2",
							Name:            "wait-for-namenode",
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command: []string{
								"sh",
								"-c",
								initContainerCommand,
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
									DefaultMode: int32Ptr(0755),
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
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion:         "hdfs.aut.tech/v1alpha1",
								BlockOwnerDeletion: func() *bool { b := true; return &b }(),
								Controller:         func() *bool { b := true; return &b }(),
								Kind:               "HDFSCluster",
								Name:               hdfsCluster.Name,
								UID:                hdfsCluster.UID,
							},
						},
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
	} else if !reflect.DeepEqual(existingStatefulSet.Spec.Replicas, desiredDataNodeStatefulSet.Spec.Replicas) ||
		!reflect.DeepEqual(existingStatefulSet.Spec.Template.Spec.Containers[0].Resources, desiredDataNodeStatefulSet.Spec.Template.Spec.Containers[0].Resources) {
		if *desiredDataNodeStatefulSet.Spec.Replicas < *existingStatefulSet.Spec.Replicas {
			for i := *desiredDataNodeStatefulSet.Spec.Replicas; i < *existingStatefulSet.Spec.Replicas; i++ {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      hdfs.Name + "-datanode-" + hdfs.Name + "-datanode-" + strconv.Itoa(int(i)),
						Namespace: hdfs.Namespace,
					},
				}
				if err := r.Delete(ctx, pvc); err != nil {
					return err
				}
			}
		}
		existingStatefulSet.Spec.Replicas = desiredDataNodeStatefulSet.Spec.Replicas
		existingStatefulSet.Spec.Template.Spec.Containers[0].Resources = desiredDataNodeStatefulSet.Spec.Template.Spec.Containers[0].Resources
		if err := r.Update(ctx, existingStatefulSet); err != nil {
			return err
		}
	}

	return nil
}

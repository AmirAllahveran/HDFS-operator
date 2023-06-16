package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
)

func (r *HDFSClusterReconciler) desiredZookeeperConfigMap(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.ConfigMap, error) {
	configMapTemplate := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-zookeeper-script",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "zookeeper",
			},
		},
		Data: map[string]string{
			"init-certs.sh": "#!/bin/bash",
			"setup.sh": `#!/bin/bash
# Execute entrypoint as usual after obtaining ZOO_SERVER_ID
# check ZOO_SERVER_ID in persistent volume via myid
# if not present, set based on POD hostname
if [[ -f "/bitnami/zookeeper/data/myid" ]]; then
export ZOO_SERVER_ID="$(cat /bitnami/zookeeper/data/myid)"
else
HOSTNAME="$(hostname -s)"
if [[ $HOSTNAME =~ (.*)-([0-9]+)$ ]]; then
ORD=${BASH_REMATCH[2]}
export ZOO_SERVER_ID="$((ORD + 1 ))"
else
echo "Failed to get index from hostname $HOSTNAME"
exit 1
fi
fi
exec /entrypoint.sh /run.sh`,
		},
	}

	if err := ctrl.SetControllerReference(hdfsCluster, configMapTemplate, r.Scheme); err != nil {
		return configMapTemplate, err
	}

	return configMapTemplate, nil
}

func (r *HDFSClusterReconciler) desiredZookeeperService(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.Service, error) {
	svcTemplate := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: hdfsCluster.Namespace,
			Name:      hdfsCluster.Name + "-zookeeper",
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "zookeeper",
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
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
			},
			ClusterIP: corev1.ClusterIPNone,
			Selector: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "zookeeper",
			},
		},
	}

	if err := ctrl.SetControllerReference(hdfsCluster, svcTemplate, r.Scheme); err != nil {
		return svcTemplate, err
	}

	return svcTemplate, nil
}

func (r *HDFSClusterReconciler) desiredZookeeperStatefulSet(hdfsCluster *v1alpha1.HDFSCluster) (*appsv1.StatefulSet, error) {
	zookeeperServices := hdfsCluster.Name + "-zookeeper-0." + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:2888:3888::1"
	if hdfsCluster.Spec.Zookeeper.Replicas == 3 {
		zookeeperServices = hdfsCluster.Name + "-zookeeper-0." + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:2888:3888::1 " +
			hdfsCluster.Name + "-zookeeper-1." + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:2888:3888::2 " +
			hdfsCluster.Name + "-zookeeper-2." + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:2888:3888::3"
	}
	compute, _ := resourceRequirements(hdfsCluster.Spec.Zookeeper.Resources)
	stsTemplate := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-zookeeper",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "zookeeper",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"cluster":   hdfsCluster.Name,
					"app":       "hdfsCluster",
					"component": "zookeeper",
				},
			},
			ServiceName: hdfsCluster.Name + "-zookeeper",
			Replicas:    int32Ptr(int32(hdfsCluster.Spec.Zookeeper.Replicas)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"cluster":   hdfsCluster.Name,
						"app":       "hdfsCluster",
						"component": "zookeeper",
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
												"component": "zookeeper",
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
							Name:  "hdfs-zookeeper",
							Image: "docker.io/bitnami/zookeeper:3.8.1-debian-11-r31",
							Command: []string{
								"/scripts/setup.sh",
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "client",
									ContainerPort: 2181,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "server",
									ContainerPort: 2888,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "leader-election",
									ContainerPort: 3888,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Resources: *compute,
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: func() *bool { b := false; return &b }(),
								RunAsNonRoot:             func() *bool { b := true; return &b }(),
								RunAsUser:                int64Ptr(int64(1001)),
							},
							Env: []corev1.EnvVar{
								{
									Name:  "BITNAMI_DEBUG",
									Value: "false",
								},
								{
									Name:  "ZOO_DATA_LOG_DIR",
									Value: "",
								},
								{
									Name:  "ZOO_PORT_NUMBER",
									Value: "2181",
								},
								{
									Name:  "ZOO_TICK_TIME",
									Value: "2000",
								},
								{
									Name:  "ZOO_INIT_LIMIT",
									Value: "10",
								},
								{
									Name:  "ZOO_SYNC_LIMIT",
									Value: "5",
								},
								{
									Name:  "ZOO_PRE_ALLOC_SIZE",
									Value: "65536",
								},
								{
									Name:  "ZOO_SNAPCOUNT",
									Value: "100000",
								},
								{
									Name:  "ZOO_MAX_CLIENT_CNXNS",
									Value: "10000",
								},
								{
									Name:  "ZOO_4LW_COMMANDS_WHITELIST",
									Value: "srvr, mntr, ruok",
								},
								{
									Name:  "ZOO_LISTEN_ALLIPS_ENABLED",
									Value: "no",
								},
								{
									Name:  "ZOO_AUTOPURGE_INTERVAL",
									Value: "0",
								},
								{
									Name:  "ZOO_AUTOPURGE_RETAIN_COUNT",
									Value: "3",
								},
								{
									Name:  "ZOO_MAX_SESSION_TIMEOUT",
									Value: "40000",
								},
								{
									Name:  "ZOO_SERVERS",
									Value: zookeeperServices,
								},
								{
									Name:  "ZOO_ENABLE_AUTH",
									Value: "no",
								},
								{
									Name:  "ZOO_ENABLE_QUORUM_AUTH",
									Value: "no",
								},
								{
									Name:  "ZOO_HEAP_SIZE",
									Value: "1024",
								},
								{
									Name:  "ZOO_LOG_LEVEL",
									Value: "ERROR",
								},
								{
									Name:  "ALLOW_ANONYMOUS_LOGIN",
									Value: "yes",
								},
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name"},
									},
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/bash",
											"-c",
											"echo \"ruok\" | timeout 2 nc -w 2 localhost 2181 | grep imok",
										},
									},
								},
								FailureThreshold:    6,
								InitialDelaySeconds: 30,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								TimeoutSeconds:      5,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/sh",
											"-c",
											"echo \"ruok\" | timeout 2 nc -w 2 localhost 2181 | grep imok",
										},
									},
								},
								FailureThreshold:    6,
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								TimeoutSeconds:      5,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      hdfsCluster.Name + "-zookeeper",
									MountPath: "/bitnami/zookeeper",
								},
								{
									Name:      "zookeeper-script",
									MountPath: "/scripts/setup.sh",
									SubPath:   "setup.sh",
								},
							},
						},
					},
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup: int64Ptr(int64(1001)),
					},
					RestartPolicy: corev1.RestartPolicyAlways,
					Volumes: []corev1.Volume{{
						Name: "zookeeper-script",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								DefaultMode: int32Ptr(493),
								LocalObjectReference: corev1.LocalObjectReference{
									Name: hdfsCluster.Name + "-zookeeper-script",
								},
							},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: hdfsCluster.Name + "-zookeeper",
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
								corev1.ResourceStorage: resource.MustParse(hdfsCluster.Spec.JournalNode.Resources.Storage),
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

func (r *HDFSClusterReconciler) createOrUpdateZookeeper(ctx context.Context, hdfs *v1alpha1.HDFSCluster) error {

	desiredZookeeperConfigMap, _ := r.desiredZookeeperConfigMap(hdfs)
	desiredZookeeperService, _ := r.desiredZookeeperService(hdfs)
	desiredZookeeperStatefulSet, _ := r.desiredZookeeperStatefulSet(hdfs)

	// Check if the cm already exists
	existingConfigMap := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredZookeeperConfigMap), existingConfigMap)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredZookeeperConfigMap); err != nil {
			return err
		}
	}
	// Check if the Service already exists
	existingService := &corev1.Service{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desiredZookeeperService), existingService)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredZookeeperService); err != nil {
			return err
		}
	}

	existingStatefulSet := &appsv1.StatefulSet{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desiredZookeeperStatefulSet), existingStatefulSet)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the sts
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredZookeeperStatefulSet); err != nil {
			return err
		}
	} else if existingStatefulSet.Spec.Replicas != desiredZookeeperStatefulSet.Spec.Replicas ||
		&existingStatefulSet.Spec.Template.Spec.Containers[0].Resources != &desiredZookeeperStatefulSet.Spec.Template.Spec.Containers[0].Resources {
		if *desiredZookeeperStatefulSet.Spec.Replicas < *existingStatefulSet.Spec.Replicas {
			for i := *desiredZookeeperStatefulSet.Spec.Replicas; i < *existingStatefulSet.Spec.Replicas; i++ {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      hdfs.Name + "-zookeeper-" + hdfs.Name + "-zookeeper-" + strconv.Itoa(int(i)),
						Namespace: hdfs.Namespace,
					},
				}
				if err := r.Delete(ctx, pvc); err != nil {
					return err
				}
			}
		}
		existingStatefulSet.Spec.Replicas = desiredZookeeperStatefulSet.Spec.Replicas
		existingStatefulSet.Spec.Template.Spec.Containers[0].Resources = desiredZookeeperStatefulSet.Spec.Template.Spec.Containers[0].Resources
		if err := r.Update(ctx, existingStatefulSet); err != nil {
			return err
		}
	}

	return nil
}

package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"net/url"
	"reflect"
	"regexp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
)

func (r *HDFSClusterReconciler) desiredHANameNodeConfigMap(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.ConfigMap, error) {
	configMapTemplate := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-ha-namenode-script",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "namenode",
			},
		},
		Data: map[string]string{
			"start-namenode-ha.sh": `#!/usr/bin/env bash
# Exit on error. Append "|| true" if you expect an error.
set -o errexit
# Exit on error inside any functions or subshells.
set -o errtrace
# Do not allow use of undefined vars. Use ${VAR:-} to use an undefined VAR
set -o nounset
set -o pipefail
# Turn on traces, useful while debugging.
set -o xtrace

#!/bin/bash
_METADATA_DIR=$NAMENODE_DIR/current

if [ "$POD_NAME" = "$NAMENODE_POD_0" ]; then
    echo "Running on NameNode Pod 0."
    if [ ! -d "$NAMENODE_DIR/current" ]; then
        echo "Formatting NameNode on Pod 0..."
        hdfs namenode -format -nonInteractive hdfs-k8s ||
            (echo "NameNode format failed, removing metadata directory..." ; rm -rf $NAMENODE_DIR/current; exit 1)
    fi
    _ZKFC_FORMATTED=$NAMENODE_DIR/current/.hdfs-k8s-zkfc-formatted
    if [ ! -f "$NAMENODE_DIR/current/.hdfs-k8s-zkfc-formatted" ]; then
        echo "Formatting Zookeeper Failover Controller..."
        _OUT=$(hdfs zkfc -formatZK -nonInteractive 2>&1)
        # zkfc masks fatal exceptions and returns exit code 0
        if (echo $_OUT | grep -q "FATAL"); then
            echo "ZKFC format failed with fatal error."
            exit 1
        fi
        echo "ZKFC format successful. Touching $_ZKFC_FORMATTED..."
        touch $_ZKFC_FORMATTED
    fi
elif [ "$POD_NAME" = "$NAMENODE_POD_1" ]; then
    echo "Running on NameNode Pod 1."
    if [ ! -d "$NAMENODE_DIR/current" ]; then
        echo "Bootstrapping Standby NameNode on Pod 1..."
        hdfs namenode -bootstrapStandby -nonInteractive ||  
            (echo "Standby NameNode bootstrap failed, removing metadata directory..."; rm -rf $NAMENODE_DIR/current; exit 1)
	else
		ls -lah $NAMENODE_DIR/current
    fi
fi
echo "Starting Zookeeper Fail over Controller..."
hdfs --daemon start zkfc
echo "Starting NameNode..."
hdfs namenode`,
		},
	}

	if err := ctrl.SetControllerReference(hdfsCluster, configMapTemplate, r.Scheme); err != nil {
		return configMapTemplate, err
	}

	return configMapTemplate, nil
}

func (r *HDFSClusterReconciler) createOrUpdateNameNode(ctx context.Context, hdfsCluster *v1alpha1.HDFSCluster, logger logr.Logger) error {
	// Define the desired NameNode Service object
	desiredService, _ := r.desiredNameNodeService(hdfsCluster)
	// Define the desired NameNode StatefulSet object

	desiredStatefulSet := &appsv1.StatefulSet{}
	if hdfsCluster.Spec.NameNode.Replicas == 1 {
		hdfsCluster.Status.ClusterType = "Single"
		errStatus := r.Status().Update(ctx, hdfsCluster)
		if errStatus != nil {
			return errStatus
		}
		desiredStatefulSet, _ = r.desiredSingleNameNodeStatefulSet(hdfsCluster)
	} else {
		hdfsCluster.Status.ClusterType = "HighAvailable"
		errStatus := r.Status().Update(ctx, hdfsCluster)
		if errStatus != nil {
			return errStatus
		}
		desiredConfigMap, _ := r.desiredHANameNodeConfigMap(hdfsCluster)
		desiredStatefulSet, _ = r.desiredHANameNodeStatefulSet(hdfsCluster)

		// Check if the configmap already exists
		existingConfigMap := &corev1.ConfigMap{}
		err := r.Get(ctx, client.ObjectKeyFromObject(desiredConfigMap), existingConfigMap)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}

		// Create or update the ConfigMap
		if errors.IsNotFound(err) {
			if err := r.Create(ctx, desiredConfigMap); err != nil {
				return err
			}
		}
	}

	// Check if the Service already exists
	existingService := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredService), existingService)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredService); err != nil {
			return err
		}
		//} else {
		//	existingService.Spec = desiredService.Spec
		//	if err := r.Update(ctx, existingService); err != nil {
		//		return err
		//	}
	} else if !reflect.DeepEqual(existingService.Spec.Ports, desiredService.Spec.Ports) {
		existingService.Spec.Ports = desiredService.Spec.Ports
		if err := r.Update(ctx, existingService); err != nil {
			return err
		}
	}

	// Check if the StatefulSet already exists
	existingStatefulSet := &appsv1.StatefulSet{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desiredStatefulSet), existingStatefulSet)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the StatefulSet
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredStatefulSet); err != nil {
			return err
		}
	} else if !reflect.DeepEqual(existingStatefulSet.Spec.Replicas, desiredStatefulSet.Spec.Replicas) ||
		!reflect.DeepEqual(existingStatefulSet.Spec.Template.Spec.Containers[0].Resources, desiredStatefulSet.Spec.Template.Spec.Containers[0].Resources) {
		logger.Info("updating namenode 165")
		if *desiredStatefulSet.Spec.Replicas < *existingStatefulSet.Spec.Replicas {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      hdfsCluster.Name + "-namenode-" + hdfsCluster.Name + "-namenode-1",
					Namespace: hdfsCluster.Namespace,
				},
			}
			if err := r.Delete(ctx, pvc); err != nil {
				logger.Info("delete namenode 173")
				return err
			}
			err := r.deleteJournalNode(ctx, hdfsCluster)
			if err != nil {
				return err
			}
			err = r.deleteZookeeper(ctx, hdfsCluster)
			if err != nil {
				return err
			}
		}
		existingStatefulSet.Spec.Replicas = desiredStatefulSet.Spec.Replicas
		existingStatefulSet.Spec.Template.Spec.Containers[0].Resources = desiredStatefulSet.Spec.Template.Spec.Containers[0].Resources
		if err := r.Update(ctx, existingStatefulSet); err != nil {
			logger.Info("updating namenode 181")
			return err
		}
	}

	return nil
}

func (r *HDFSClusterReconciler) desiredNameNodeService(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.Service, error) {

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
	var defaultPort int
	if hdfsCluster.Spec.NameNode.Replicas == 1 {
		if val, ok := hdfsCluster.Spec.ClusterConfig.CoreSite["fs.defaultFS"]; ok {
			u, err := url.Parse(val)
			if err != nil {
				return nil, err
			}
			defaultPort, _ = strconv.Atoi(u.Port())
		} else {
			defaultPort = 8020
		}
	} else {
		// Compile your regex
		var key string
		re, _ := regexp.Compile(`dfs.namenode.rpc-address\..+`)
		// Iteration over the map
		for k, _ := range hdfsCluster.Spec.ClusterConfig.HdfsSite {
			if re.MatchString(k) {
				key = k
				break
			}
		}
		if val, ok := hdfsCluster.Spec.ClusterConfig.HdfsSite[key]; ok {
			u, err := url.Parse("//" + val)
			if err != nil {
				return nil, err
			}
			defaultPort, _ = strconv.Atoi(u.Port())
		} else {
			defaultPort = 8020
		}
	}

	svcTemplate := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: hdfsCluster.Namespace,
			Name:      hdfsCluster.Name + "-namenode",
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "namenode",
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "web",
					Port:       int32(webPort),
					TargetPort: intstr.FromString("web"),
				},
				{
					Name:       "default",
					Port:       int32(defaultPort),
					TargetPort: intstr.FromString("default"),
				},
			},
			ClusterIP: corev1.ClusterIPNone,
			Selector: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "namenode",
			},
		},
	}

	if err := ctrl.SetControllerReference(hdfsCluster, svcTemplate, r.Scheme); err != nil {
		return svcTemplate, err
	}

	return svcTemplate, nil
}

func (r *HDFSClusterReconciler) desiredSingleNameNodeStatefulSet(hdfsCluster *v1alpha1.HDFSCluster) (*appsv1.StatefulSet, error) {
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
	var namenodeDataDir string
	if val, ok := hdfsCluster.Spec.ClusterConfig.HdfsSite["dfs.namenode.data.dir"]; ok {
		namenodeDataDir = val
	} else {
		namenodeDataDir = "/data/hadoop/namenode"
	}

	var defaultPort int
	if val, ok := hdfsCluster.Spec.ClusterConfig.CoreSite["fs.defaultFS"]; ok {
		u, err := url.Parse(val)
		if err != nil {
			return nil, err
		}
		defaultPort, _ = strconv.Atoi(u.Port())
	} else {
		defaultPort = 8020
	}

	compute, _ := resourceRequirements(hdfsCluster.Spec.NameNode.Resources)

	stsTemplate := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-namenode",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "namenode",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"cluster":   hdfsCluster.Name,
					"app":       "hdfsCluster",
					"component": "namenode",
				},
			},
			ServiceName: hdfsCluster.Name + "-namenode",
			Replicas:    int32Ptr(int32(hdfsCluster.Spec.NameNode.Replicas)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"cluster":   hdfsCluster.Name,
						"app":       "hdfsCluster",
						"component": "namenode",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "hdfs-namenode",
							Image: "amiralh4/namenode:3.3.1",
							Ports: []corev1.ContainerPort{
								{
									Name:          "default",
									ContainerPort: int32(defaultPort),
								},
								{
									Name:          "web",
									ContainerPort: int32(webPort),
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "NAMENODE_DIR",
									Value: namenodeDataDir,
								},
							},
							Resources: *compute,
							Lifecycle: &corev1.Lifecycle{
								PostStart: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/sh",
											"-c",
											"rm -rf $NAMENODE_DIR/lost+found"},
									},
								},
							},
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
									Name:      hdfsCluster.Name + "-namenode",
									MountPath: namenodeDataDir,
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
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: hdfsCluster.Name + "-namenode",
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
								corev1.ResourceStorage: resource.MustParse(hdfsCluster.Spec.NameNode.Resources.Storage),
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

func (r *HDFSClusterReconciler) desiredHANameNodeStatefulSet(hdfsCluster *v1alpha1.HDFSCluster) (*appsv1.StatefulSet, error) {
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

	var httpPort int
	if val, ok := hdfsCluster.Spec.ClusterConfig.HdfsSite["dfs.journalnode.http-address"]; ok {
		u, err := url.Parse("//" + val)
		if err != nil {
			return nil, err
		}
		httpPort, _ = strconv.Atoi(u.Port())
	} else {
		httpPort = 8480
	}

	var namenodeDataDir string
	if val, ok := hdfsCluster.Spec.ClusterConfig.HdfsSite["dfs.namenode.data.dir"]; ok {
		namenodeDataDir = val
	} else {
		namenodeDataDir = "/data/hadoop/namenode"
	}

	initContainerCommandZKNslookup := "while [ $(nslookup " + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local | grep -c 'Address: ') != " + strconv.Itoa(hdfsCluster.Spec.Zookeeper.Replicas) + " ]; do echo waiting; sleep 2; done"
	initContainerCommandJNNslookup := "while [ $(nslookup " + hdfsCluster.Name + "-journalnode." + hdfsCluster.Namespace + ".svc.cluster.local | grep -c 'Address: ') != " + strconv.Itoa(hdfsCluster.Spec.JournalNode.Replicas) + " ]; do echo waiting; sleep 2; done"
	initContainerCommandZK := "while [ $(curl -m 1 -s -o /dev/null -w \"%{http_code}\" http://" + hdfsCluster.Name + "-zookeeper-" + strconv.Itoa(hdfsCluster.Spec.Zookeeper.Replicas-1) + "." + hdfsCluster.Name + "-zookeeper." + hdfsCluster.Namespace + ".svc.cluster.local:8080/commands/ruok)!= \"200\" ]; do echo waiting; sleep 2; done"
	initContainerCommandJN := "while [ $(curl -m 1 -s -o /dev/null -w \"%{http_code}\" http://" + hdfsCluster.Name + "-journalnode-" + strconv.Itoa(hdfsCluster.Spec.JournalNode.Replicas-1) + "." + hdfsCluster.Name + "-journalnode." + hdfsCluster.Namespace + ".svc.cluster.local:" + strconv.Itoa(httpPort) + "/index.html)!= \"200\" ]; do echo waiting; sleep 2; done"

	var defaultPort int

	// Compile your regex
	var key string
	re, _ := regexp.Compile(`dfs.namenode.rpc-address\..+`)
	// Iteration over the map
	for k, _ := range hdfsCluster.Spec.ClusterConfig.HdfsSite {
		if re.MatchString(k) {
			key = k
			break
		}
	}
	if val, ok := hdfsCluster.Spec.ClusterConfig.HdfsSite[key]; ok {
		u, err := url.Parse("//" + val)
		if err != nil {
			return nil, err
		}
		defaultPort, _ = strconv.Atoi(u.Port())
	} else {
		defaultPort = 8020
	}

	compute, _ := resourceRequirements(hdfsCluster.Spec.NameNode.Resources)

	stsTempalte := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-namenode",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "namenode",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"cluster":   hdfsCluster.Name,
					"app":       "hdfsCluster",
					"component": "namenode",
				},
			},
			ServiceName: hdfsCluster.Name + "-namenode",
			Replicas:    int32Ptr(int32(hdfsCluster.Spec.NameNode.Replicas)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"cluster":   hdfsCluster.Name,
						"app":       "hdfsCluster",
						"component": "namenode",
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
												"component": "namenode",
											},
										},
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Image:           "arunvelsriram/utils",
							Name:            "wait-for-zookeeper",
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command: []string{
								"sh",
								"-c",
								initContainerCommandJNNslookup,
								"sh",
								"-c",
								initContainerCommandJN,
								"sh",
								"-c",
								initContainerCommandZKNslookup,
								"sh",
								"-c",
								initContainerCommandZK,
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "hdfs-namenode",
							Image: "amiralh4/namenode:3.3.1",
							Ports: []corev1.ContainerPort{
								{
									Name:          "default",
									ContainerPort: int32(defaultPort),
								},
								{
									Name:          "web",
									ContainerPort: int32(webPort),
								},
							},
							Command: []string{
								"/bin/bash",
								"-c",
								"/scripts/start-namenode-ha.sh",
							},
							Resources: *compute,
							Env: []corev1.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name:  "NAMENODE_POD_0",
									Value: hdfsCluster.Name + "-namenode-0",
								},
								{
									Name:  "NAMENODE_POD_1",
									Value: hdfsCluster.Name + "-namenode-1",
								},
								{
									Name:  "NAMENODE_DIR",
									Value: namenodeDataDir,
								},
							},
							Lifecycle: &corev1.Lifecycle{
								PostStart: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/sh",
											"-c",
											"rm -rf $NAMENODE_DIR/lost+found",
										},
									},
								},
							},
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
									Name:      "ha-namenode-script",
									MountPath: "/scripts/start-namenode-ha.sh",
									SubPath:   "start-namenode-ha.sh",
								},
								{
									Name:      hdfsCluster.Name + "-namenode",
									MountPath: namenodeDataDir,
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
							Name: "ha-namenode-script",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									DefaultMode: int32Ptr(0755),
									LocalObjectReference: corev1.LocalObjectReference{
										Name: hdfsCluster.Name + "-ha-namenode-script",
									},
									Items: []corev1.KeyToPath{
										{
											Key:  "start-namenode-ha.sh",
											Path: "start-namenode-ha.sh",
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
						Name: hdfsCluster.Name + "-namenode",
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
								corev1.ResourceStorage: resource.MustParse(hdfsCluster.Spec.NameNode.Resources.Storage),
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

	if err := ctrl.SetControllerReference(hdfsCluster, stsTempalte, r.Scheme); err != nil {
		return stsTempalte, err
	}

	return stsTempalte, nil
}

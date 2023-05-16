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
)

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
					Name:       "server",
					Port:       2888,
					TargetPort: intstr.FromString("server"),
				},
				{
					Name:       "leader-election",
					Port:       3888,
					TargetPort: intstr.FromString("leader-election"),
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
	replicas := int(*stringToInt32(hdfsCluster.Spec.JournalNode.Replicas))
	zookeeperServices := "server.1=zk-0.zk-headless.default.svc.cluster.local:2888:3888;2181"
	if replicas == 3 {
		zookeeperServices = "server.1=zk-0.zk-headless.default.svc.cluster.local:2888:3888;2181 server.2=zk-1.zk-headless.default.svc.cluster.local:2888:3888;2181 server.3=zk-2.zk-headless.default.svc.cluster.local:2888:3888;2181"
	}

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
			Replicas:    stringToInt32(hdfsCluster.Spec.JournalNode.Replicas),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"cluster":   hdfsCluster.Name,
						"app":       "hdfsCluster",
						"component": "zookeeper",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "hdfs-zookeeper",
							Image: "bitnami/zookeeper:3.8.1",
							Ports: []corev1.ContainerPort{
								{
									Name:          "server",
									ContainerPort: 2888,
								},
								{
									Name:          "leader-election",
									ContainerPort: 3888,
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "ZOO_SERVERS",
									Value: zookeeperServices,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      hdfsCluster.Name + "-zookeeper",
									MountPath: "/bitnami/zookeeper",
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: hdfsCluster.Name + "-zookeeper",
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

	desiredZookeeperService, _ := r.desiredZookeeperService(hdfs)
	desiredZookeeperStatefulSet, _ := r.desiredZookeeperStatefulSet(hdfs)

	// Check if the journal already exists

	// Check if the Service already exists
	existingService := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredZookeeperService), existingService)
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
	} else {
		existingStatefulSet.Spec = desiredZookeeperStatefulSet.Spec
		if err := r.Update(ctx, existingStatefulSet); err != nil {
			return err
		}
	}

	return nil
}

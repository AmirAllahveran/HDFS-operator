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

func (r *HDFSClusterReconciler) createOrUpdateNameNode(ctx context.Context, hdfsCluster *v1alpha1.HDFSCluster) error {
	// Define the desired NameNode Service object
	desiredService, _ := r.desiredNameNodeService(hdfsCluster)
	// Define the desired NameNode StatefulSet object
	desiredStatefulSet, _ := r.desiredNameNodeStatefulSet(hdfsCluster)

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
	} else {
		existingService.Spec = desiredService.Spec
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
	} else {
		existingStatefulSet.Spec = desiredStatefulSet.Spec
		if err := r.Update(ctx, existingStatefulSet); err != nil {
			return err
		}
	}

	return nil
}

//func (r *HDFSClusterReconciler) desiredNameNodeService(hdfsCluster *hdfsv1.HDFSCluster) *corev1.Service {
//	// Define your desired NameNode Service object here
//}
//
//func (r *HDFSClusterReconciler) desiredNameNodeStatefulSet(hdfsCluster *hdfsv1.HDFSCluster) *appsv1.StatefulSet {
//	// Define your desired NameNode StatefulSet object here
//}

func (r *HDFSClusterReconciler) desiredNameNodeService(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.Service, error) {
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
					Port:       9870,
					TargetPort: intstr.FromString("web"),
				},
				{
					Name:       "default",
					Port:       9000,
					TargetPort: intstr.FromString("default"),
				},
			},
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

func (r *HDFSClusterReconciler) desiredNameNodeStatefulSet(hdfsCluster *v1alpha1.HDFSCluster) (*appsv1.StatefulSet, error) {
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
			ServiceName: hdfsCluster.Name + "-namenode-service",
			Replicas:    stringToInt32(hdfsCluster.Spec.NameNode.Replicas),
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
							Image: "amiralh4/datanode:3.3.1",
							Ports: []corev1.ContainerPort{
								{
									Name:          "default",
									ContainerPort: 9000,
								},
								{
									Name:          "web",
									ContainerPort: 9870,
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
									Name:      "datanode-script",
									MountPath: "/scripts/check-status.sh",
									SubPath:   "check-status.sh",
								},
								{
									Name:      hdfsCluster.Name + "-namenode",
									MountPath: "/data/hadoop/namenode",
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
										Name: hdfsCluster.Name + "-config",
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
										Name: hdfsCluster.Name + "-config",
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
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"cluster":   hdfsCluster.Name,
								"app":       "hdfsCluster",
								"component": "datanode",
							},
						},
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

//func (r *HDFSClusterReconciler) nameNodeExists(ctx context.Context, hdfsCluster *v1alpha1.HDFSCluster) (bool, error) {
//	// Define the desired NameNode StatefulSet object
//	desiredStatefulSet, _ := r.desiredNameNodeStatefulSet(hdfsCluster)
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
//	// Check if the NameNode pod is in Running state
//	nameNodePod := &corev1.Pod{}
//	nameNodePodName := types.NamespacedName{
//		Namespace: hdfsCluster.Namespace,
//		Name:      fmt.Sprintf("%s-0", desiredStatefulSet.Name),
//	}
//	err = r.Get(ctx, nameNodePodName, nameNodePod)
//
//	if err != nil {
//		if errors.IsNotFound(err) {
//			return false, nil
//		}
//		return false, err
//	}
//
//	if nameNodePod.Status.Phase != corev1.PodRunning {
//		return false, nil
//	}
//
//	return true, nil
//}

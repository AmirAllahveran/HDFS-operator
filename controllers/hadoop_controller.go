package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *HDFSClusterReconciler) desiredHadoopDeployment(hdfsCluster *v1alpha1.HDFSCluster) (*appsv1.Deployment, error) {
	deploymentTemplate := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-hadoop",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "hadoop",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"cluster":   hdfsCluster.Name,
					"app":       "hdfsCluster",
					"component": "hadoop",
				},
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Replicas: int32Ptr(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"cluster":   hdfsCluster.Name,
						"app":       "hdfsCluster",
						"component": "hadoop",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "hadoop",
							Image: "amiralh4/hadoop:3.3.1",
							//Ports: []corev1.ContainerPort{
							//	{
							//		Name:          "default",
							//		ContainerPort: 9864,
							//	},
							//},
							Command: []string{
								"sleep",
							},
							Args: []string{
								"1000000000",
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
		},
	}

	if err := ctrl.SetControllerReference(hdfsCluster, deploymentTemplate, r.Scheme); err != nil {
		return deploymentTemplate, err
	}

	return deploymentTemplate, nil
}

func (r *HDFSClusterReconciler) createHadoop(ctx context.Context, hdfs *v1alpha1.HDFSCluster) error {
	// Define the desired Hadoop object
	desiredHadoopDeployment, _ := r.desiredHadoopDeployment(hdfs)

	// Check if the hadoop already exists
	existingDeployment := &appsv1.Deployment{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredHadoopDeployment), existingDeployment)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	// Create Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredHadoopDeployment); err != nil {
			return err
		}
	}
	return nil
}

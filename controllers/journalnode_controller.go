package controllers

import (
	"context"
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
)

func (r *HDFSClusterReconciler) desiredJournalNodePodDisruptionBudget(hdfs *v1alpha1.HDFSCluster) (*v1.PodDisruptionBudget, error) {
	replicas, _ := strconv.ParseInt(hdfs.Spec.JournalNode.Replicas, 10, 32)
	minAvailable := replicas/2 + 1
	pdbTemplate := &v1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfs.Name + "-journalnode",
			Namespace: hdfs.Namespace,
		},
		Spec: v1.PodDisruptionBudgetSpec{
			MinAvailable: &intstr.IntOrString{
				IntVal: int32(minAvailable),
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"cluster":   hdfs.Name,
					"app":       "hdfsCluster",
					"component": "journalnode",
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(hdfs, pdbTemplate, r.Scheme); err != nil {
		return pdbTemplate, err
	}

	return pdbTemplate, nil
}

func (r *HDFSClusterReconciler) desiredJournalNodeService(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.Service, error) {
	svcTemplate := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: hdfsCluster.Namespace,
			Name:      hdfsCluster.Name + "-journalnode",
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "journalnode",
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       8480,
					TargetPort: intstr.FromString("http"),
				},
				{
					Name:       "ipc",
					Port:       8485,
					TargetPort: intstr.FromString("ipc"),
				},
			},
			ClusterIP: corev1.ClusterIPNone,
			Selector: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "journalnode",
			},
		},
	}

	if err := ctrl.SetControllerReference(hdfsCluster, svcTemplate, r.Scheme); err != nil {
		return svcTemplate, err
	}

	return svcTemplate, nil
}

func (r *HDFSClusterReconciler) createOrUpdateJournalNode(ctx context.Context, hdfs *v1alpha1.HDFSCluster) error {

	desiredJournalNodeService, _ := r.desiredJournalNodeService(hdfs)
	desiredJournalNodePodDisruptionBudget, _ := r.desiredJournalNodePodDisruptionBudget(hdfs)
	desiredJournalNodeStatefulSet, _ := r.desiredJournalNodeStatefulSet(hdfs)

	// Check if the journal already exists

	// Check if the Service already exists
	existingService := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredJournalNodeService), existingService)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the Service
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredJournalNodeService); err != nil {
			return err
		}
	}

	// Check if the PodDisruptionBudget already exists
	existingPodDisruptionBudget := &v1.PodDisruptionBudget{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desiredJournalNodePodDisruptionBudget), existingPodDisruptionBudget)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the PodDisruptionBudget
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredJournalNodePodDisruptionBudget); err != nil {
			return err
		}
	}

	existingStatefulSet := &appsv1.StatefulSet{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desiredJournalNodeStatefulSet), existingStatefulSet)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Create or update the sts
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desiredJournalNodeStatefulSet); err != nil {
			return err
		}
	} else {
		existingStatefulSet.Spec.Replicas = desiredJournalNodeStatefulSet.Spec.Replicas
		existingStatefulSet.Spec.Template.Spec.Containers[0].Resources = desiredJournalNodeStatefulSet.Spec.Template.Spec.Containers[0].Resources
		existingStatefulSet.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests = desiredJournalNodeStatefulSet.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests
		if err := r.Update(ctx, existingStatefulSet); err != nil {
			return err
		}
	}

	return nil
}

func (r *HDFSClusterReconciler) desiredJournalNodeStatefulSet(hdfsCluster *v1alpha1.HDFSCluster) (*appsv1.StatefulSet, error) {
	stsTemplate := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name + "-journalnode",
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"cluster":   hdfsCluster.Name,
				"app":       "hdfsCluster",
				"component": "journalnode",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"cluster":   hdfsCluster.Name,
					"app":       "hdfsCluster",
					"component": "journalnode",
				},
			},
			ServiceName: hdfsCluster.Name + "-journalnode",
			Replicas:    stringToInt32(hdfsCluster.Spec.JournalNode.Replicas),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"cluster":   hdfsCluster.Name,
						"app":       "hdfsCluster",
						"component": "journalnode",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "hdfs-journalnode",
							Image: "amiralh4/journalnode:3.3.1",
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: 8480,
								},
								{
									Name:          "ipc",
									ContainerPort: 8485,
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
									Name:      hdfsCluster.Name + "-journalnode",
									MountPath: "/data/hadoop/journalnode",
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
						Name: hdfsCluster.Name + "-journalnode",
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
						//Selector: &metav1.LabelSelector{
						//	MatchLabels: map[string]string{
						//		"cluster":   hdfsCluster.Name,
						//		"app":       "hdfsCluster",
						//		"component": "journalnode",
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

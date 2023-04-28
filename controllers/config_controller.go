package controllers

import (
	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

func (r *HDFSClusterReconciler) DesiredClusterConfigMap(hdfsCluster *v1alpha1.HDFSCluster) (*corev1.ConfigMap, error) {
	cmTemplate := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hdfsCluster.Name,
			Namespace: hdfsCluster.Namespace,
			Labels: map[string]string{
				"app": hdfsCluster.Name,
			},
		},
		Data: map[string]string{
			"core-site.xml": `<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <!--  core-site.xml content here -->
</configuration>
`,
			"hdfs-site.xml": `<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <!--  hdfs-site.xml content here -->
</configuration>
`,
		},
	}
	if err := ctrl.SetControllerReference(hdfsCluster, cmTemplate, r.Scheme); err != nil {
		return cmTemplate, err
	}

	return cmTemplate, nil
}

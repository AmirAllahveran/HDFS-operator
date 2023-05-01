package v1alpha1

import (
	"context"
	"net/http"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// +kubebuilder:webhook:path=/validate-hdfs,mutating=false,failurePolicy=fail,groups=hdfs.aut.tech,resources=hdfsclusters,verbs=create;update,versions=v1alpha1,name=vhdfsclusters.kb.io,sideEffects=None,admissionReviewVersions=v1alpha1

type HDFSValidator struct {
	Scheme  *runtime.Scheme
	decoder *admission.Decoder
}

func (v *HDFSValidator) SetupWebhookWithManager(mgr ctrl.Manager) {
	mgr.GetWebhookServer().Register("/validate-hdfs", &webhook.Admission{Handler: v})
}

func (v *HDFSValidator) Handle(ctx context.Context, req webhook.AdmissionRequest) webhook.AdmissionResponse {
	newHdfs := &HDFSCluster{}
	if err := v.decoder.DecodeRaw(req.Object, newHdfs); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if newHdfs.Spec.ClusterConfig.CustomHadoopConfig.CoreSite != "" &&
		strings.Contains(newHdfs.Spec.ClusterConfig.CustomHadoopConfig.CoreSite, "dfsReplication") {
		return admission.Denied("DfsReplication is not allowed in CustomConfig")
	}

	if req.Operation == "UPDATE" {
		oldHdfs := &HDFSCluster{}
		if err := v.decoder.DecodeRaw(req.OldObject, oldHdfs); err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}

		if newHdfs.Spec.DataNode.Resources.Storage != oldHdfs.Spec.DataNode.Resources.Storage ||
			newHdfs.Spec.NameNode.Resources.Storage != oldHdfs.Spec.NameNode.Resources.Storage {
			return admission.Denied("storage field is immutable")
		}

		if newHdfs.Spec.ClusterConfig.DfsReplication != oldHdfs.Spec.ClusterConfig.DfsReplication {
			return admission.Denied("Spec.ClusterConfig.DfsReplication is immutable")
		}
	}

	return admission.Allowed("")
}

func (v *HDFSValidator) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}

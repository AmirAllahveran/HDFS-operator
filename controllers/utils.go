package controllers

import (
	"context"
	"encoding/xml"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/go-xmlfmt/xmlfmt"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func stringToInt32(s string) *int32 {
	i, _ := strconv.ParseInt(s, 10, 32)
	result := int32(i)
	return &result
}

func int32Ptr(i int32) *int32 { return &i }

func int64Ptr(i int64) *int64 { return &i }

func (r *HDFSClusterReconciler) ScaleDownAndUpStatefulSet(ctx context.Context, name, namespace string) error {
	existingStatefulSet := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, existingStatefulSet)
	if err != nil {
		return err
	}

	// Add annotation to force pod recreation
	if existingStatefulSet.Spec.Template.Annotations == nil {
		existingStatefulSet.Spec.Template.Annotations = map[string]string{}
	}
	existingStatefulSet.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().UTC().Format(time.RFC3339)

	// Update the StatefulSet with the new replica count
	if err := r.Update(ctx, existingStatefulSet); err != nil {
		return err
	}

	return nil
}

func (r *HDFSClusterReconciler) ScaleDownAndUpDeployment(ctx context.Context, name, namespace string) error {
	existingDeployment := &appsv1.Deployment{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, existingDeployment)
	if err != nil {
		return err
	}

	// Add annotation to force pod recreation
	if existingDeployment.Spec.Template.Annotations == nil {
		existingDeployment.Spec.Template.Annotations = map[string]string{}
	}
	existingDeployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().UTC().Format(time.RFC3339)
	// Update the StatefulSet with the new replica count
	if err := r.Update(ctx, existingDeployment); err != nil {
		return err
	}
	return nil
}

func mapToXml(properties map[string]string) string {
	var res string
	for key, value := range properties {
		property := `<property>
	<name>` + key + `</name>
	<value>` + value + `</value>
</property>`
		res = res + property
	}

	res = xmlfmt.FormatXML(res, "", "  ")

	return res
}

func resourceRequirements(resources v1alpha1.Resources) (*v1.ResourceRequirements, error) {
	var err error

	req := v1.ResourceRequirements{
		Requests: v1.ResourceList{},
		Limits:   v1.ResourceList{},
	}

	if resources.Cpu != "" {
		req.Requests[v1.ResourceCPU], err = resource.ParseQuantity(resources.Cpu)
		if err != nil {
			return nil, err
		}
		req.Limits[v1.ResourceCPU] = req.Requests[v1.ResourceCPU]
	}

	if resources.Memory != "" {
		req.Requests[v1.ResourceMemory], err = resource.ParseQuantity(resources.Memory)
		if err != nil {
			return nil, err
		}
		req.Limits[v1.ResourceMemory] = req.Requests[v1.ResourceMemory]
	}

	return &req, nil
}

type Property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type Configuration struct {
	Properties []Property `xml:"property"`
}

func compareXML(xml1, xml2 string) bool {
	var parsed1, parsed2 Configuration

	// Get the substring from <configuration> to </configuration>
	xml1 = extractConfiguration(xml1)
	xml2 = extractConfiguration(xml2)

	err1 := xml.Unmarshal([]byte(xml1), &parsed1)
	err2 := xml.Unmarshal([]byte(xml2), &parsed2)

	// If there is an error in unmarshalling, we return false
	if err1 != nil || err2 != nil {
		return false
	}

	// Sort the properties by name to ensure consistent comparison
	sort.Slice(parsed1.Properties, func(i, j int) bool {
		return parsed1.Properties[i].Name < parsed1.Properties[j].Name
	})
	sort.Slice(parsed2.Properties, func(i, j int) bool {
		return parsed2.Properties[i].Name < parsed2.Properties[j].Name
	})

	return compareProperties(parsed1.Properties, parsed2.Properties)
}

func compareProperties(properties1, properties2 []Property) bool {
	if len(properties1) != len(properties2) {
		return false
	}
	for i := range properties1 {
		if properties1[i].Name != properties2[i].Name || properties1[i].Value != properties2[i].Value {
			return false
		}
	}
	return true
}

func extractConfiguration(xmlString string) string {
	start := strings.Index(xmlString, "<configuration>")
	end := strings.LastIndex(xmlString, "</configuration>")
	return xmlString[start : end+len("</configuration>")]
}

package controllerutils

import (
	"context"
	"encoding/xml"
	"sort"
	"strconv"
	"strings"
	"time"

	hdfsv1alpha1 "github.com/AmirAllahveran/HDFS-operator/api/v1alpha1"
	"github.com/go-xmlfmt/xmlfmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StringToInt32 converts the provided string to an int32 pointer.
func StringToInt32(s string) *int32 {
	i, _ := strconv.ParseInt(s, 10, 32)
	result := int32(i)
	return &result
}

// Int32Ptr returns a pointer to the supplied value.
func Int32Ptr(i int32) *int32 { return &i }

// Int64Ptr returns a pointer to the supplied value.
func Int64Ptr(i int64) *int64 { return &i }

// RestartStatefulSet forces a rolling restart of the statefulset identified by name and namespace.
func RestartStatefulSet(ctx context.Context, c client.Client, name, namespace string) error {
	existing := &appsv1.StatefulSet{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing); err != nil {
		return err
	}

	if existing.Spec.Template.Annotations == nil {
		existing.Spec.Template.Annotations = map[string]string{}
	}
	existing.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().UTC().Format(time.RFC3339)

	return c.Update(ctx, existing)
}

// RestartDeployment forces a rolling restart of the deployment identified by name and namespace.
func RestartDeployment(ctx context.Context, c client.Client, name, namespace string) error {
	existing := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing); err != nil {
		return err
	}

	if existing.Spec.Template.Annotations == nil {
		existing.Spec.Template.Annotations = map[string]string{}
	}
	existing.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().UTC().Format(time.RFC3339)

	return c.Update(ctx, existing)
}

// MapToXML converts a map of properties to a formatted XML configuration.
func MapToXML(properties map[string]string) string {
	var res string
	for key, value := range properties {
		property := `<property>
    <name>` + key + `</name>
    <value>` + value + `</value>
</property>`
		res += property
	}

	return xmlfmt.FormatXML(res, "", "  ")
}

// ResourceRequirements generates a V1 resource requirements struct based on the provided spec resources.
func ResourceRequirements(resources hdfsv1alpha1.Resources) (*corev1.ResourceRequirements, error) {
	req := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	if resources.Cpu != "" {
		cpu, err := resource.ParseQuantity(resources.Cpu)
		if err != nil {
			return nil, err
		}
		req.Requests[corev1.ResourceCPU] = cpu
		req.Limits[corev1.ResourceCPU] = cpu
	}

	if resources.Memory != "" {
		memory, err := resource.ParseQuantity(resources.Memory)
		if err != nil {
			return nil, err
		}
		req.Requests[corev1.ResourceMemory] = memory
		req.Limits[corev1.ResourceMemory] = memory
	}

	return &req, nil
}

type property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type configuration struct {
	Properties []property `xml:"property"`
}

// CompareXML compares two XML strings containing Hadoop configuration payloads.
func CompareXML(xml1, xml2 string) bool {
	var parsed1, parsed2 configuration

	xml1 = extractConfiguration(xml1)
	xml2 = extractConfiguration(xml2)

	if err := xml.Unmarshal([]byte(xml1), &parsed1); err != nil {
		return false
	}
	if err := xml.Unmarshal([]byte(xml2), &parsed2); err != nil {
		return false
	}

	sort.Slice(parsed1.Properties, func(i, j int) bool {
		return parsed1.Properties[i].Name < parsed1.Properties[j].Name
	})
	sort.Slice(parsed2.Properties, func(i, j int) bool {
		return parsed2.Properties[i].Name < parsed2.Properties[j].Name
	})

	if len(parsed1.Properties) != len(parsed2.Properties) {
		return false
	}
	for i := range parsed1.Properties {
		if parsed1.Properties[i].Name != parsed2.Properties[i].Name || parsed1.Properties[i].Value != parsed2.Properties[i].Value {
			return false
		}
	}

	return true
}

func extractConfiguration(xmlString string) string {
	start := strings.Index(xmlString, "<configuration>")
	if start < 0 {
		start = 0
	}

	end := strings.LastIndex(xmlString, "</configuration>")
	if end < 0 {
		end = len(xmlString)
	} else {
		end += len("</configuration>")
	}

	if start >= end {
		return xmlString
	}

	return xmlString[start:end]
}

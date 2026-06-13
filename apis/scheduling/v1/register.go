// +kwrapper:package=gate
// +kubebuilder:object:generate=true
// +groupName=scheduling.kaschnit.github.io
package v1

import (
	"github.com/kaschnit/kaschnit-scheduler/apis/scheduling"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// Version is this API version.
	Version = "v1"
	// QueueFQRN is the fully-qualified resource name of [Queue].
	QueueFQRN = "queues." + Version + "." + scheduling.GroupName
)

var (
	SchemeGroupVersion = schema.GroupVersion{Group: scheduling.GroupName, Version: Version}
	SchemeBuilder      = &runtime.SchemeBuilder{addKnownTypes}
	AddToScheme        = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Queue{},
		&QueueList{},
	)

	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// Resource converts the resource name to [schema.GroupResource] for this API group/version.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

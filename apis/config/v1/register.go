package v1

import (
	"github.com/kaschnit/custom-scheduler/apis/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	SchemeGroupVersion = schema.GroupVersion{Group: config.GroupName, Version: "v1"}
	SchemeBuilder      = &runtime.SchemeBuilder{addKnownTypes}
	AddToScheme        = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&QuotaAwarePreemptionArgs{},
	)

	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

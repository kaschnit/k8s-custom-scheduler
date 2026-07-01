package kubesched

import (
	configv1 "k8s.io/kube-scheduler/config/v1"
	configapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
)

const KubeSchedulerConfigurationKind = "KubeSchedulerConfiguration"

func ToConfigAPIWithDefaults(cfg configv1.KubeSchedulerConfiguration) (configapi.KubeSchedulerConfiguration, error) {
	scheme.Scheme.Default(&cfg)

	if cfg.APIVersion == "" {
		cfg.APIVersion = configv1.SchemeGroupVersion.String()
	}
	if cfg.Kind == "" {
		cfg.Kind = KubeSchedulerConfigurationKind
	}

	var apiCfg configapi.KubeSchedulerConfiguration
	if err := scheme.Scheme.Convert(&cfg, &apiCfg, nil); err != nil {
		return apiCfg, err
	}

	// TypeMeta gets unset by conversion.
	apiCfg.TypeMeta = cfg.TypeMeta

	return apiCfg, nil
}

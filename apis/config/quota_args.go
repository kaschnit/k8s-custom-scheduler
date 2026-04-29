package config

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// QuotaArgs holds arguments used to configure Quota plugin.
type QuotaArgs struct {
	metav1.TypeMeta `json:",inline"`

	// EnableAsyncPreemption is whether to enable async preemption.
	// Defaults to false.
	EnableAsyncPreemption bool `json:"enableAsyncPreemption"`

	// Queues are the queues by name.
	Queues map[string]QueueConfig `json:"queues"`
}

type QueueConfig struct {
	// Quota is the quota for this queue.
	Quota corev1.ResourceList `json:"quota"`
}

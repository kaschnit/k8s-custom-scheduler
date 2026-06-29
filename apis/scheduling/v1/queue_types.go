package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type QuotaSpec struct {
	// Max is the max quota for this queue.
	// +optional
	Max corev1.ResourceList `json:"max,omitempty"`
}

type PreemptsRule struct {
	// FromPods are the pods within this queue than can preempt the selected queues/pods.
	// If this is omitted, this queue cannot preempt any selected queues/pods.
	// An explicit empty selector indicates all pods in this queue can selected queues/pods.
	// +optional
	FromPods *metav1.LabelSelector `json:"fromPods,omitempty"`
	// ToQueues are the queues that this queue can preempt.
	// If this is omitted, no victim queues are selected.
	// An explicit empty selector indicates the queue can preempt all queues.
	// +optional
	ToQueues *metav1.LabelSelector `json:"toQueues,omitempty"`
	// ToPods are the pods within the selected queues that this queue can preempt.
	// If this is omitted, this queue cannot preempt any pods from the selected queues.
	// An explicit empty selector indicates this queue can preempt all pods in the selected queues.
	// +optional
	ToPods *metav1.LabelSelector `json:"toPods,omitempty"`
}

type PreemptedByRule struct {
	// FromQueues are the queues that can preempt this queue.
	// If this is omitted, no queue can preempt this queue.
	// An explicit empty selector indicates that this queue can be preempted by any queue.
	// +optional
	FromQueues *metav1.LabelSelector `json:"toQueues,omitempty"`
	// FromPods are the pods within the selected queues that can preempt this queue.
	// If this is omitted, no pods in the selected queues can preempt this queue.
	// An explicit empty selector indicates all pods in the selected queues can preempt this queue.
	// +optional
	FromPods *metav1.LabelSelector `json:"fromPods,omitempty"`
	// ToPods are the pods within this queue that can be preempted.
	// If this is omitted, no pods in this queue can be preempted.
	// An explicit empty selector indicates all pods in this queue can be preempted by the selected
	// queues/pods.
	// +optional
	ToPods *metav1.LabelSelector `json:"toPods,omitempty"`
}

type PreemptionSpec struct {
	// Preempts defines the criteria for what pods this queue's pods can preempt.
	Preempts PreemptsRule `json:"preempts,omitempty"`
	// PreemptedBy defines the criteria for what pods can preempt this queue's pods.
	PreemptedBy PreemptedByRule `json:"preemptedBy,omitempty"`
}

type QueueSpec struct {
	// Quota is the queue's quota settings.
	// +optional
	Quota QuotaSpec `json:"quota,omitempty"`
	// Preemption is the queues' preemption settings.
	// +optional
	Preemption PreemptionSpec `json:"preemption,omitempty"`
}

type QuotaStatus struct {
	// EffectiveMax is the effective max resource for this queue.
	// This takes into account unset resources, cluster capacity, etc.
	// +optional
	EffectiveMax corev1.ResourceList `json:"effectiveMax,omitempty"`
	// Used is the used quota for this queue.
	// +optional
	Used corev1.ResourceList `json:"used,omitempty"`
}

type QueueStatus struct {
	// Quota is the queue's quota status.
	// +optional
	Quota QuotaStatus `json:"quota,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

type Queue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec is the queue's specification.
	Spec QueueSpec `json:"spec,omitempty"`
	// Status is the queue's status.
	Status QueueStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	// Items are the queues in this list.
	Items []Queue `json:"items"`
}

package pods

import (
	schedulingapi "github.com/kaschnit/kaschnit-scheduler/apis/scheduling"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func New(name string, opts ...Option) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

type Option func(*corev1.Pod)

func WithName(name string) Option {
	return func(p *corev1.Pod) {
		p.Name = name
	}
}

func WithNamespace(namespace string) Option {
	return func(p *corev1.Pod) {
		p.Namespace = namespace
	}
}

func WithSchedulerName(schedulerName string) Option {
	return func(p *corev1.Pod) {
		p.Spec.SchedulerName = schedulerName
	}
}

func AddContainer(containers ...corev1.Container) Option {
	return func(p *corev1.Pod) {
		p.Spec.Containers = append(p.Spec.Containers, containers...)
	}
}

func WithQueue(queueName string) Option {
	return func(p *corev1.Pod) {
		if p.Labels == nil {
			p.Labels = make(map[string]string)
		}

		p.Labels[schedulingapi.LabelKeyQueue] = queueName
	}
}

func WithPriorityClass(priorityClassName string) Option {
	return func(p *corev1.Pod) {
		p.Spec.PriorityClassName = priorityClassName
	}
}

# custom-scheduler

Writing a custom Kubernetes scheduler using the kube-scheduler framework.

This is not intended for production use, it's a toy project for fun and to help me
learn and understand the kube-scheduler plugin framework.

## Getting Started

### Prerequisites
- go version v1.21+ (go toolchains support)

### Run locally

Run `make kind-deploy`.

#### Quota test

Quotas configured in `KubeSchedulerConfiguration`. Each "queue" has a quota configured and the sum of pods in that queue cannot exceed quota.

1. Run `make kind deploy`
1. Run `kubectl create -f test/kind/preemptor.yaml` to create a pod (can't be preempted)
1. Run `kubectl create -f test/kind/victim.yaml` to create another pod (can't preempt)

Notice that `victim` pod is unschedulable due to quota exceeded.

#### Preemption for quota test

Preemption case: there is some capacity left, but a queue exceeds its quota. In this case, a preemptor submitted to that queue can be scheduled by preempting other pods in that same queue.

1. Run `make kind deploy`
1. Run `kubectl create -f test/kind/victim.yaml` to create another pod (can be preempted)
1. Run `kubectl create -f test/kind/preemptor.yaml` to create a pod (is a preemptor)

Notice that `preemptor` pod preempts `victim` because `preemptor` can't schedule to to quota.

#### Preemption for capacity test

Preemption case: a queue does not exceed its quota but there is no capacity left on nodes. In this case, a preemptor submitted to any queue can get be scheduled by preempting other pods in any queue (same queue or different queue).

TODO

#### Preemption for quota and capacity test

Preemption case: there is no capacity left on cluster, and a queue exceeds its quota; and preempting only in the quota will not free enough resources. In this case, a preemptor submitted to that queue can be scheduled by preempting some pods in the same queue, and other pods in any queue (same queue or different queue).

TODO

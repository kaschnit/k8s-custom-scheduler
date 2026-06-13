package scheduling

const (
	// LabelKeyQueue is the name of the label whose value is the queue.
	LabelKeyQueue = GroupName + "/queue"
	// LabelKeyPreemptor specifies whether this pod can preempt other pods.
	// If unspecified, empty, or invalid, defaults to false (this pod cannot preempt).
	LabelKeyPreemptor = GroupName + "/preemptor"
	// LabelKeyVictim specifies whether this pod can be preempted by other pods.
	// If unspecified, empty, or invalid, defaults to false (this pod cannot be preempted).
	LabelKeyVictim = GroupName + "/victim"
)

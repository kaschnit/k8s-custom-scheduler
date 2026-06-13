package quotaawarepreempt

import (
	"github.com/kaschnit/kaschnit-scheduler/internal/fwkutil"
	"github.com/kaschnit/kaschnit-scheduler/internal/queue"
	fwk "k8s.io/kube-scheduler/framework"
)

const stateKeyQueueSnapshot fwk.StateKey = PluginName + "QueueSnapshot"

var _ fwk.StateData = (*QueueSnapshotState)(nil)

// QueueSnapshotState is shared scheduling state related to quota usage.
type QueueSnapshotState struct {
	QueueMgr *queue.Manager
}

// NewQueueSnapshotState creates a snapshot of the queue manager.
func NewQueueSnapshotState(queueMgr *queue.Manager) *QueueSnapshotState {
	if queueMgr == nil {
		queueMgr = queue.NewManager()
	} else {
		queueMgr = queueMgr.Clone()
	}

	return &QueueSnapshotState{
		QueueMgr: queueMgr,
	}
}

// Clone implements [fwk.StateData].
func (s *QueueSnapshotState) Clone() fwk.StateData {
	return &QueueSnapshotState{
		QueueMgr: s.QueueMgr.Clone(),
	}
}

// StateManager manages the scheduling cycle state for the quota-aware preemption plugin.
type StateManager struct {
	cycleState fwk.CycleState
}

// NewStateManager creates a new [StateManager].
func NewStateManager(cycleState fwk.CycleState) *StateManager {
	return &StateManager{
		cycleState: cycleState,
	}
}

// ReadQueueSnapshot reads the queue snapshot from the scheduling cycle state.
func (mgr *StateManager) ReadQueueSnapshot() (*QueueSnapshotState, error) {
	return fwkutil.ReadState[*QueueSnapshotState](mgr.cycleState, stateKeyQueueSnapshot)
}

// WriteQueueSnapshot writes the queue snapshot to the scheduling cycle state.
func (mgr *StateManager) WriteQueueSnapshot(data *QueueSnapshotState) {
	mgr.cycleState.Write(stateKeyQueueSnapshot, data)
}

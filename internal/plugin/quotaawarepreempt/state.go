package quotaawarepreempt

import (
	"sync"

	"github.com/kaschnit/kaschnit-scheduler/internal/fwkutil"
	"github.com/kaschnit/kaschnit-scheduler/internal/queue"
	fwk "k8s.io/kube-scheduler/framework"
)

const stateKeyQueueSnapshot fwk.StateKey = PluginName + "QueueSnapshot"

var _ fwk.StateData = (*QueueSnapshotState)(nil)

// QueueSnapshotState is shared scheduling state related to quota usage.
type QueueSnapshotState struct {
	QueueMgr   *queue.Manager
	clones     *[]*queue.Manager
	clonesLock *sync.Mutex
}

// NewQueueSnapshotState creates a snapshot of the queue manager.
func NewQueueSnapshotState(queueMgr *queue.Manager) *QueueSnapshotState {
	if queueMgr == nil {
		queueMgr = queue.NewManager()
	} else {
		queueMgr = queueMgr.Clone()
	}

	return &QueueSnapshotState{
		QueueMgr:   queueMgr,
		clones:     new([]*queue.Manager),
		clonesLock: new(sync.Mutex),
	}
}

// Clone implements [fwk.StateData].
func (s *QueueSnapshotState) Clone() fwk.StateData {
	queueMgr := s.QueueMgr.Clone()

	s.clonesLock.Lock()
	*s.clones = append(*s.clones, queueMgr)
	s.clonesLock.Unlock()

	return &QueueSnapshotState{
		QueueMgr:   queueMgr,
		clones:     s.clones,
		clonesLock: s.clonesLock,
	}
}

// Close closes the queue snapshot.
// The queue snapshot should not be used after closing.
func (s *QueueSnapshotState) Close() {
	s.clonesLock.Lock()
	defer s.clonesLock.Unlock()

	var wg sync.WaitGroup
	wg.Go(s.QueueMgr.Close)
	for _, clone := range *s.clones {
		wg.Go(clone.Close)
	}
	wg.Wait()

	s.clones = new([]*queue.Manager)
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

// CloseQueueSnapshot closes the scheduling state's queue snapshot.
func (mgr *StateManager) CloseQueueSnapshot() error {
	queueSnapshot, err := mgr.ReadQueueSnapshot()
	if err != nil {
		return err
	}

	queueSnapshot.Close()

	return nil
}

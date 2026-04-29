package quotaawarepreempt

import (
	"errors"
	"fmt"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

var (
	errReadState = errors.New("failed to read from cycleState")
)

const stateKeyPreFilter fwk.StateKey = "PreFilter" + Name

var _ fwk.StateData = (*PreFilterState)(nil)

type PreFilterState struct {
	request             framework.Resource
	nominatedReqInQuota framework.Resource
}

func (s *PreFilterState) Clone() fwk.StateData {
	return &PreFilterState{
		request: *s.request.Clone(),
	}
}

const stateKeyQuotaSnapshot fwk.StateKey = "QuotaSnapshot" + Name

var _ fwk.StateData = (*QuotaUsageSnapshotState)(nil)

type QuotaUsageSnapshotState struct {
	quotaUsages QuotaUsages
}

func (s *QuotaUsageSnapshotState) Clone() fwk.StateData {
	return &QuotaUsageSnapshotState{
		quotaUsages: s.quotaUsages.clone(),
	}
}

type StateManager struct {
	cycleState fwk.CycleState
}

func NewStateManager(cycleState fwk.CycleState) *StateManager {
	return &StateManager{
		cycleState: cycleState,
	}
}

func (mgr *StateManager) ReadPreFilter() (*PreFilterState, error) {
	return readState[*PreFilterState](mgr.cycleState, stateKeyPreFilter)
}

func (mgr *StateManager) WritePreFilter(data *PreFilterState) {
	mgr.cycleState.Write(stateKeyPreFilter, data)
}

func (mgr *StateManager) ReadQuotaUsageSnapshot() (*QuotaUsageSnapshotState, error) {
	return readState[*QuotaUsageSnapshotState](mgr.cycleState, stateKeyQuotaSnapshot)
}

func (mgr *StateManager) WriteQuotaUsageSnapshot(data *QuotaUsageSnapshotState) {
	mgr.cycleState.Write(stateKeyQuotaSnapshot, data)
}

func readState[T fwk.StateData](cycleState fwk.CycleState, key fwk.StateKey) (T, error) {
	rawState, err := cycleState.Read(key)
	if err != nil {
		// State doesn't exist
		var emptyState T
		return emptyState, fmt.Errorf("%w: error reading %q: %w", errReadState, key, err)
	}

	state, ok := rawState.(T)
	if !ok {
		var emptyState T
		return emptyState, fmt.Errorf("%w: failed to convert state %+v to %T", errReadState, rawState, emptyState)
	}

	return state, nil
}

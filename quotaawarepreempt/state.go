package quotaawarepreempt

import (
	"github.com/kaschnit/custom-scheduler/internal/fwkutil"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const stateKeyPreFilter fwk.StateKey = "PreFilter" + PluginName

var _ fwk.StateData = (*PreFilterState)(nil)

// QuotaUsageSnapshotState is shared scheduling state related to the prefilter point.
type PreFilterState struct {
	request             framework.Resource
	nominatedReqInQuota framework.Resource
}

// Clone implements [fwk.StateData].
func (s *PreFilterState) Clone() fwk.StateData {
	return &PreFilterState{
		request:             *s.request.Clone(),
		nominatedReqInQuota: *s.nominatedReqInQuota.Clone(),
	}
}

const stateKeyQuotaSnapshot fwk.StateKey = "QuotaSnapshot" + PluginName

var _ fwk.StateData = (*QuotaUsageSnapshotState)(nil)

// QuotaUsageSnapshotState is shared scheduling state related to quota usage.
type QuotaUsageSnapshotState struct {
	quotaUsages QuotaUsages
}

// Clone implements [fwk.StateData].
func (s *QuotaUsageSnapshotState) Clone() fwk.StateData {
	return &QuotaUsageSnapshotState{
		quotaUsages: s.quotaUsages.clone(),
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

// ReadPreFilter reads the prefilter data from the scheduling cycle state.
func (mgr *StateManager) ReadPreFilter() (*PreFilterState, error) {
	return fwkutil.ReadState[*PreFilterState](mgr.cycleState, stateKeyPreFilter)
}

// WritePreFilter writes the prefilter data to the scheduling cycle state.
func (mgr *StateManager) WritePreFilter(data *PreFilterState) {
	mgr.cycleState.Write(stateKeyPreFilter, data)
}

// ReadQuotaUsageSnapshot reads the quota usage snapshot data from the scheduling cycle state.
func (mgr *StateManager) ReadQuotaUsageSnapshot() (*QuotaUsageSnapshotState, error) {
	return fwkutil.ReadState[*QuotaUsageSnapshotState](mgr.cycleState, stateKeyQuotaSnapshot)
}

// WriteQuotaUsageSnapshot writes the quota usage snapshot data to the scheduling cycle state.
func (mgr *StateManager) WriteQuotaUsageSnapshot(data *QuotaUsageSnapshotState) {
	mgr.cycleState.Write(stateKeyQuotaSnapshot, data)
}

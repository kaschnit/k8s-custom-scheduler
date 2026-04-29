package fwkutil

import (
	"errors"
	"fmt"

	fwk "k8s.io/kube-scheduler/framework"
)

var (
	errReadState = errors.New("failed to read from cycleState")
)

// ReadState reads the state data with the given key from the scheduling cycle state.
// Returns an error if the key does not exist or cannot be converted to the provided type.
func ReadState[T fwk.StateData](cycleState fwk.CycleState, key fwk.StateKey) (T, error) {
	rawState, err := cycleState.Read(key)
	if err != nil {
		// State doesn't exist
		var emptyState T
		return emptyState, fmt.Errorf("%w: error reading %q: %w", errReadState, key, err)
	}

	state, ok := rawState.(T)
	if !ok {
		// State cannot be converted
		var emptyState T
		return emptyState, fmt.Errorf("%w: failed to convert state %+v to %T", errReadState, rawState, emptyState)
	}

	return state, nil
}

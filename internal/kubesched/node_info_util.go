package kubesched

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
)

var (
	ErrLookupNode = errors.New("failed to look up node")
)

// GetPodIDsOnNode gets the set of pod IDs on the given node name.
func GetPodIDsOnNode(fh fwk.Handle, nodeName string) (sets.Set[types.UID], error) {
	nomNodeInfo, err := fh.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrLookupNode, err)
	}
	if nomNodeInfo == nil {
		return nil, fmt.Errorf("%w: node was null", ErrLookupNode)
	}

	podsIDsOnNode := make(sets.Set[types.UID], len(nomNodeInfo.GetPods()))
	for _, podInfo := range nomNodeInfo.GetPods() {
		podsIDsOnNode.Insert(podInfo.GetPod().GetUID())
	}

	return podsIDsOnNode, nil
}

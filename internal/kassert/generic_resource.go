package kassert

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// IsErrNotFound asserts that err indicatates a the Kubernetes resource was not found.
func IsErrNotFound(t TestingT, err error, msg ...any) {
	t.Helper()

	if len(msg) == 0 {
		msg = []any{"Resource should not exist"}
	}

	assert.True(t, apierrors.IsNotFound(err), msg...)
}

// ObjectInListByUID asserts that obj's UID is one of the UIDs of objs.
func ObjectInListByUID(t TestingT, obj metav1.Object, objs []metav1.Object) {
	require.NotNil(t, obj, "Expected obj to be non-nil")
	require.NotNil(t, objs, "Expected objs list to be non-nil")

	objIDs := make([]types.UID, 0, len(objs))
	for _, objInList := range objs {
		objIDs = append(objIDs, objInList.GetUID())
	}

	assert.Contains(t, objIDs, obj.GetUID(), "Expected object ID %s in list: %s")
}

package kubeassert

import (
	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// IsErrNotFound asserts that err indicatates a the Kubernetes resource was not found.
func IsErrNotFound(t assert.TestingT, err error, msg ...any) {
	if len(msg) == 0 {
		msg = []any{"Resource should not exist"}
	}

	assert.True(t, apierrors.IsNotFound(err), msg...)
}

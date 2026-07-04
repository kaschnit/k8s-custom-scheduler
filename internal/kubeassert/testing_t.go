package kubeassert

import (
	"testing"
)

var _ TestingT = (*testing.T)(nil)

type TestingT interface {
	Helper()
	Errorf(format string, args ...any)
}

package kassert

import (
	"testing"
)

var _ TestingT = (*testing.T)(nil)

// TestingT is an interface to shim testing library and testify library.
// It defines an intersection between [testing.T] and various similar interfaces
// that testify provides.
type TestingT interface {
	// Helper comes from [testing.T].
	Helper()
	// Errorf comes from [testing.T].
	Errorf(format string, args ...any)
	// FailNow comes from [testing.T].
	FailNow()
}

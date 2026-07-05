package kassert

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Eventually asserts that the condition eventually passes all assertions.
// This is similar to [assert.EventuallyWithT] except uses default eventually configs
// and allows options to be passed to customize them.
func Eventually(
	t *testing.T,
	condition func(c *assert.CollectT),
	opts ...EventuallyConfigOpt,
) {
	t.Helper()

	cfg := newEventuallyConfig(opts...)

	require.EventuallyWithT(t, condition, cfg.Timeout, cfg.PollInterval)
}

// EventuallyConfig is configuration for [Eventually] and related functions.
type EventuallyConfig struct {
	PollInterval time.Duration
	Timeout      time.Duration
}

func newEventuallyConfig(opts ...EventuallyConfigOpt) EventuallyConfig {
	cfg := EventuallyConfig{
		PollInterval: 250 * time.Millisecond,
		Timeout:      3 * time.Second,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

// EventuallyConfigOpt applies a change to [EventuallyConfig].
type EventuallyConfigOpt func(*EventuallyConfig)

// WithPollInterval creates an option that configures poll interval.
func WithPollInterval(pollInterval time.Duration) EventuallyConfigOpt {
	return func(ec *EventuallyConfig) {
		ec.PollInterval = pollInterval
	}
}

// WithTimeout creates an option that configures timeout.
func WithTimeout(timeout time.Duration) EventuallyConfigOpt {
	return func(ec *EventuallyConfig) {
		ec.Timeout = timeout
	}
}

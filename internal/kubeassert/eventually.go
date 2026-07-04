package kubeassert

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Eventually(
	t *testing.T,
	condition func(c *assert.CollectT),
	opts ...EventuallyConfigOpt,
) {
	t.Helper()

	cfg := newEventuallyConfig(opts...)

	assert.EventuallyWithT(t, condition, cfg.Timeout, cfg.PollInterval)
}

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

type EventuallyConfigOpt func(*EventuallyConfig)

func WithPollInterval(pollInterval time.Duration) EventuallyConfigOpt {
	return func(ec *EventuallyConfig) {
		ec.PollInterval = pollInterval
	}
}

func WithTimeout(timeout time.Duration) EventuallyConfigOpt {
	return func(ec *EventuallyConfig) {
		ec.Timeout = timeout
	}
}

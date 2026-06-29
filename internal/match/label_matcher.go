package match

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// LabelMatcher is a read-only version of [labels.Selector].
// It provides only the ability to match labels.
type LabelMatcher interface {
	// Matches is the same as [labels.Selector.Matches].
	Matches(labels.Labels) bool
}

// Nothing returns true for matchers which will never match any objects.
// It otherwise returns false.
func Nothing(matcher LabelMatcher) bool {
	return matcher == labels.Nothing()
}

// LabelSelectorAsMatcherOrNothing converts selector to a [LabelMatcher].
// If any errors are encountered a match-nothing matcher is returned along with the error.
func LabelSelectorAsMatcherOrNothing(selector *metav1.LabelSelector) (LabelMatcher, error) {
	matcher, err := metav1.LabelSelectorAsSelector(selector)
	if matcher == nil || err != nil {
		matcher = labels.Nothing()
	}

	return matcher, err
}

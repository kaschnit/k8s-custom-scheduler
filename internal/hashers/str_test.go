package hashers_test

import (
	"github.com/benbjohnson/immutable"
	"github.com/kaschnit/kaschnit-scheduler/internal/hashers"
)

var _ immutable.Hasher[string] = hashers.StrLike[string]{}

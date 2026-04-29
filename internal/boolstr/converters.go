package boolstr

import "strings"

func IsTrue(s string) bool {
	return strings.ToLower(s) == "true"
}

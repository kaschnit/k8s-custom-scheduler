package lazy

// Value is a value allowed in lazy data structures.
type Value[V any] interface {
	// Clone clones this value.
	Clone() V
}

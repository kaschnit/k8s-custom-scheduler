package hashers

// StrLike is a hasher for string-like types.
// Use generics to avoid reflection for checking underlying string type.
// Do not use for secure hashing; expected use is for hash maps and
// similar data structures.
type StrLike[T ~string] struct{}

// Hash returns a hash of the string-like value.
func (h StrLike[T]) Hash(key T) uint32 {
	var hash uint32
	for _, b := range key {
		hash = 31*hash + uint32(b)
	}
	return hash
}

// Equal tells whether the two string-like values are equal.
func (h StrLike[T]) Equal(a, b T) bool {
	return a == b
}

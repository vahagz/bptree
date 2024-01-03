// Package customerrors defines errors that can occur while using bptree
package customerrors

import "errors"

var (
	// ErrKeyNotFound should be returned from lookup operations when the
	// lookup key is not found in index/store.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyTooLarge is returned by index implementations when a key is
	// larger than a configured limit if any.
	ErrKeyTooLarge = errors.New("key is too large")

	// ErrEmptyKey should be returned by backends when an operation is
	// requested with an empty key.
	ErrEmptyKey = errors.New("empty key")

	// ErrImmutable should be returned by backends when write operation
	// (put/del) is attempted on a readonly.
	ErrImmutable = errors.New("operation not allowed in read-only mode")
	
	// ErrNotFound should be returned when trying update key-value pair
	// and key not found in bptree
	ErrNotFound = errors.New("not found")
)

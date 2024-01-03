// Package helpers provides helpful functions which are frequently used
// in bptree package
package helpers

import "bytes"

// CompareMatrix is equivalent of bytes.Compare but for slice of bytes slices.
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func CompareMatrix(a, b [][]byte) int {
	var cmp int
	for i := range a {
		cmp = bytes.Compare(a[i], b[i])
		if cmp != 0 {
			break
		}
	}
	return cmp
}

// Copy returns copy of matrix
func Copy(matrix [][]byte) [][]byte {
	cp := make([][]byte, len(matrix))
	for i := range cp {
		cp[i] = make([]byte, len(matrix[i]))
		copy(cp[i], matrix[i])
	}
	return cp
}

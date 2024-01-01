package helpers

import "bytes"

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

func Copy(matrix [][]byte) [][]byte {
	cp := make([][]byte, len(matrix))
	for i := range cp {
		cp[i] = make([]byte, len(matrix[i]))
		copy(cp[i], matrix[i])
	}
	return cp
}

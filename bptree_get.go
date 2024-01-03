package bptree

import "github.com/vahagz/bptree/helpers"

// Get fetches the values associated with the given key.
// Returns empty slice if key not found.
func (tree *BPlusTree) Get(key, suffix [][]byte) ([][]byte, error) {
	key, err := tree.validateAndMerge(key, suffix)
	if err != nil {
		return nil, err
	}

	suffixPresent := suffix != nil
	result := [][]byte{}
	return result, tree.Scan(ScanOptions{
		Key:     key,
		Reverse: false,
		Strict:  true,
	}, func(k [][]byte, v []byte) (bool, error) {
		k1 := key
		k2 := k
		if !suffixPresent {
			k2 = tree.RemoveSuffix(k)
		}

		if helpers.CompareMatrix(k1, k2) != 0 {
			return true, nil
		}

		result = append(result, v)
		return false, nil
	})
}

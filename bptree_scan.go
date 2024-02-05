package bptree

import (
	"github.com/vahagz/bptree/helpers"
	allocator "github.com/vahagz/disk-allocator/heap"
	"github.com/vahagz/disk-allocator/heap/cache"
)

// Scan performs an index scan starting at the given key. Each entry will be
// passed to the scanFn. If the key is zero valued (nil or len=0), then the
// left/right leaf key will be used as the starting key. Scan continues until
// the right most leaf node is reached or the scanFn returns 'true' indicating
// to stop the scan.
func (tree *BPlusTree) Scan(
	opts ScanOptions,
	scanFn func(key [][]byte, val []byte) (bool, error),
) error {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if tree.meta.count == 0 {
		return nil
	}
	
	if len(opts.Key) != 0 {
		// we have a specific key to start at. find the node containing the
		// key and start the scan there.
		opts.Key = helpers.Copy(opts.Key)
		if (opts.Strict && opts.Reverse) || (!opts.Strict && !opts.Reverse) {
			opts.Key = tree.AddSuffix(opts.Key, suffixFill)
		} else {
			opts.Key = tree.AddSuffix(opts.Key, suffixZero)
		}
	}

	return tree.scan(opts.Key, opts, cache.READ, func(
		key [][]byte,
		val []byte,
		_ int,
		_ cache.Pointable[*node],
	) (bool, error) {
		return scanFn(key, val)
	})
}

// Scan performs an index scan starting at the given key. Each entry will be
// passed to the scanFn. If the key is zero valued (nil or len=0), then the
// left/right leaf key will be used as the starting key. Scan continues until
// the right most leaf node is reached or the scanFn returns 'true' indicating
// to stop the scan.
func (tree *BPlusTree) scan(
	key [][]byte,
	opts ScanOptions,
	flag cache.LOCKMODE,
	scanFn func(key [][]byte, val []byte, index int, leaf cache.Pointable[*node]) (bool, error),
) error {
	var (
		beginAt cache.Pointable[*node]
		found bool
		idx int
	)

	root := tree.rootF(flag)
	if len(key) == 0 {
		// No explicit key provided by user, find the a leaf-node based on
		// scan direction and start there.
		if !opts.Reverse {
			beginAt = tree.leftLeaf(root, flag)
			idx = 0
		} else {
			beginAt = tree.rightLeaf(root, flag)
			idx = len(beginAt.Get().entries) - 1
		}
	} else {
		beginAt, idx, found = tree.searchRec(root, key, flag)
		// ======= magic =======
		if tree.IsUniq() && found {
			if !opts.Strict {
				if opts.Reverse {
					idx--
				} else {
					idx++
				}
			}
		} else if opts.Reverse {
			idx--
		}
		// =====================
	}

	// starting at found leaf node, follow the 'next' pointer until.
	var nextNode allocator.Pointable

	L: for beginAt != nil {
		if !opts.Reverse {
			for i := idx; i < len(beginAt.Get().entries); i++ {
				e := beginAt.Get().entries[i]
				if stop, err := scanFn(e.key, e.val, i, beginAt); err != nil {
					beginAt.UnlockFlag(flag)
					return err
				} else if stop {
					beginAt.UnlockFlag(flag)
					break L
				}
			}
			nextNode = beginAt.Get().right
		} else {
			for i := idx; i >= 0; i-- {
				e := beginAt.Get().entries[i]
				if stop, err := scanFn(e.key, e.val, i, beginAt); err != nil {
					beginAt.UnlockFlag(flag)
					return err
				} else if stop {
					beginAt.UnlockFlag(flag)
					break L
				}
			}
			nextNode = beginAt.Get().left
		}

		beginAt.UnlockFlag(flag)
		if nextNode.IsNil() {
			break
		}

		beginAt = tree.fetchF(nextNode, flag)
		if !opts.Reverse {
			idx = 0
		} else {
			idx = len(beginAt.Get().entries) - 1
		}
	}

	return nil
}

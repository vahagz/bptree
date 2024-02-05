package bptree

import (
	"math"

	"github.com/pkg/errors"
	"github.com/vahagz/bptree/helpers"
	"github.com/vahagz/disk-allocator/heap/cache"
)

// Del removes the key-value entry from the bptree. If the key does not
// exist, returns error.
func (tree *BPlusTree) Del(key, suffix [][]byte) (int, error) {
	count, err := tree.DelMem(key, suffix)
	if err != nil {
		return 0, err
	}
	return count, tree.WriteAll()
}

// Del removes the key-value entry from the bptree. If the key does not
// exist, returns error. Unlike Del DelMem will not force to flush
// dirty nodes to disk after deletion.
func (tree *BPlusTree) DelMem(key, suffix [][]byte) (int, error) {
	key, err := tree.validateAndMerge(key, suffix)
	if err != nil {
		return 0, err
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	suffixPresent := suffix != nil
	count := 0
	cnt := true
	for cnt {
		cnt = false
		tree.scan(key, ScanOptions{
			Reverse: false,
			Strict:  true,
		}, cache.NONE, func(
			k [][]byte,
			_ []byte,
			_ int,
			ptr cache.Pointable[*node],
		) (bool, error) {
			k1 := key
			k2 := k
			if !suffixPresent {
				k1 = tree.RemoveSuffix(k1)
				k2 = tree.RemoveSuffix(k2)
			}

			if helpers.CompareMatrix(k1, k2) == 0 {
				cnt = true
				ptr.Lock()
				if isDelete := tree.del(k, ptr); isDelete {
					count++
				}
			}
			return true, nil
		})
	}

	if count > 0 {
		tree.meta.count -= uint64(count)
		tree.meta.dirty = true
	}

	return count, nil
}

// removeFromLeaf is helper function for deletion.
// Removes entry from nPtr where key is found.
func (tree *BPlusTree) removeFromLeaf(key [][]byte, pPtr, nPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	index, found := nv.search(key)
	if !found {
		panic(errors.New("[removeFromLeaf] key not found"))
	}

	nv.removeEntries(index, index + 1)
	if pPtr != nil {
		pv := pPtr.Get()
		index, _ := pv.search(key)
		if index != 0 && len(nv.entries) > 0 {
			pv.Dirty(true)
			pv.entries[index - 1] = entry{
				key: helpers.Copy(nv.entries[0].key),
			}
		}
	}
}

// removeFromInternal is helper function for deletion.
// Removes entry from nPtr where key is found.
func (tree *BPlusTree) removeFromInternal(key [][]byte, nPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	index, found := nv.search(key)
	if found {
		leftMostLeaf := tree.fetchR(nv.children[index])
		leftMostLeaf = tree.leftLeaf(leftMostLeaf, cache.READ)
		defer leftMostLeaf.RUnlock()

		lv := leftMostLeaf.Get()
		nv.Dirty(true)
		nv.entries[index - 1] = entry{
			key: helpers.Copy(lv.entries[0].key),
		}
	}
}

// borrowKeyFromRightLeaf is helper function for deletion.
// Called when there are enough entries in right leaf of nPtr.
func (tree *BPlusTree) borrowKeyFromRightLeaf(pPtr, nPtr, rightPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	rv := rightPtr.Get()
	nv.appendEntry(rv.entries[0])
	rv.removeEntries(0, 1)

	pv := pPtr.Get()
	index, _ := pv.search(rv.entries[len(rv.entries)-1].key)
	pv.Dirty(true)
	pv.entries[index - 1] = entry{
		key: helpers.Copy(rv.entries[0].key),
	}
}

// borrowKeyFromLeftLeaf is helper function for deletion.
// Called when there are enough entries in left leaf of nPtr.
func (tree *BPlusTree) borrowKeyFromLeftLeaf(pPtr, nPtr, leftPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	lv := leftPtr.Get()
	nv.insertEntry(0, lv.entries[len(lv.entries)-1])
	lv.removeEntries(len(lv.entries) - 1, len(lv.entries))

	pv := pPtr.Get()
	index, _ := pv.search(nv.entries[len(nv.entries)-1].key)
	pv.Dirty(true)
	pv.entries[index - 1] = entry{
		key: helpers.Copy(nv.entries[0].key),
	}
}

// mergeNodeWithRightLeaf is helper function for deletion.
// Called when in sum nPtr and right leaf have enough entries
// for one node.
func (tree *BPlusTree) mergeNodeWithRightLeaf(pPtr, nPtr, rightPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	rv := rightPtr.Get()
	nv.Dirty(true)
	rv.Dirty(true)

	nv.entries = append(nv.entries, rv.entries...)
	nv.right = rv.right
	if !nv.right.IsNil() {
		rightRightPtr := tree.fetchW(nv.right)
		defer rightRightPtr.Unlock()

		rrv := rightRightPtr.Get()
		rrv.left = nPtr.Ptr()
	}

	pv := pPtr.Get()
	index, _ := pv.search(rv.entries[len(rv.entries)-1].key)
	pv.removeEntries(index - 1, index)
	pv.removeChildren(index, index + 1)

	tree.freeNode(rightPtr)
}

// mergeNodeWithLeftLeaf is helper function for deletion.
// Called when in sum nPtr and left leaf have enough entries
// for one node.
func (tree *BPlusTree) mergeNodeWithLeftLeaf(pPtr, nPtr, leftPtr, rightPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	lv := leftPtr.Get()
	nv.Dirty(true)
	lv.Dirty(true)

	lv.entries = append(lv.entries, nv.entries...)
	lv.right = nv.right
	if !lv.right.IsNil() {
		rv := rightPtr.Get()
		rv.Dirty(true)
		rv.left = leftPtr.Ptr()
	}

	pv := pPtr.Get()
	index, _ := pv.search(nv.entries[len(nv.entries)-1].key)
	pv.removeEntries(index - 1, index)
	pv.removeChildren(index, index + 1)

	tree.freeNode(nPtr)
}

// borrowKeyFromRightInternal is helper function for deletion.
// Called when there are enough entries in right internal node of nPtr.
func (tree *BPlusTree) borrowKeyFromRightInternal(parentIndex int, pPtr, nPtr, rightPtr cache.Pointable[*node]) {
	pv := pPtr.Get()
	nv := nPtr.Get()
	rv := rightPtr.Get()

	nv.appendEntry(pv.entries[parentIndex])
	pv.Dirty(true)
	pv.entries[parentIndex] = entry{
		key: helpers.Copy(rv.entries[0].key),
	}
	rv.removeEntries(0, 1)
	nv.appendChild(rv.children[0])
	rv.removeChildren(0, 1)
	
	lastChildPtr := tree.fetchW(nv.children[len(nv.children)-1])
	defer lastChildPtr.Unlock()
	lcv := lastChildPtr.Get()
	lcv.Dirty(true)
	lcv.parent = nPtr.Ptr()
}

// borrowKeyFromLeftInternal is helper function for deletion.
// Called when there are enough entries in left internal node of nPtr.
func (tree *BPlusTree) borrowKeyFromLeftInternal(parentIndex int, pPtr, nPtr, leftPtr cache.Pointable[*node]) {
	pv := pPtr.Get()
	nv := nPtr.Get()
	lv := leftPtr.Get()

	nv.insertEntry(0, pv.entries[parentIndex - 1])
	pv.Dirty(true)
	pv.entries[parentIndex - 1] = entry{
		key: helpers.Copy(lv.entries[len(lv.entries)-1].key),
	}
	lv.removeEntries(len(lv.entries)-1, len(lv.entries))
	nv.insertChild(0, lv.children[len(lv.children)-1])
	lv.removeChildren(len(lv.children) - 1, len(lv.children))

	firstChildPtr := tree.fetchW(nv.children[0])
	defer firstChildPtr.Unlock()
	fcv := firstChildPtr.Get()
	fcv.Dirty(true)
	fcv.parent = nPtr.Ptr()
}

// mergeNodeWithRightInternal is helper function for deletion.
// Called when in sum nPtr and right internal node have enough
// entries for one node.
func (tree *BPlusTree) mergeNodeWithRightInternal(parentIndex int, pPtr, nPtr, rightPtr cache.Pointable[*node]) {
	pv := pPtr.Get()
	nv := nPtr.Get()
	rv := rightPtr.Get()

	nv.appendEntry(pv.entries[parentIndex])
	pv.removeEntries(parentIndex, parentIndex + 1)
	pv.removeChildren(parentIndex + 1, parentIndex + 2)
	nv.entries = append(nv.entries, rv.entries...)
	nv.children = append(nv.children, rv.children...)
	for _, childPtr := range rv.children {
		ptr := tree.fetchW(childPtr)
		v := ptr.Get()
		v.Dirty(true)
		v.parent = nPtr.Ptr()
		ptr.Unlock()
	}

	tree.freeNode(rightPtr)
}

// mergeNodeWithLeftInternal is helper function for deletion.
// Called when in sum nPtr and left internal node have enough
// entries for one node.
func (tree *BPlusTree) mergeNodeWithLeftInternal(parentIndex int, pPtr, nPtr, leftPtr cache.Pointable[*node]) {
	pv := pPtr.Get()
	nv := nPtr.Get()
	lv := leftPtr.Get()

	lv.appendEntry(pv.entries[parentIndex - 1])
	pv.removeEntries(parentIndex - 1, parentIndex)
	pv.removeChildren(parentIndex, parentIndex + 1)
	lv.entries = append(lv.entries, nv.entries...)
	lv.children = append(lv.children, nv.children...)
	for _, childPtr := range nv.children {
		ptr := tree.fetchW(childPtr)
		v := ptr.Get()
		v.Dirty(true)
		v.parent = leftPtr.Ptr()
		ptr.Unlock()
	}

	tree.freeNode(nPtr)
}

// del deletes entry from bptree. If after deletion
// in any node occurs underflow it desides which helper
// function to call to maintain tree consistency.
func (tree *BPlusTree) del(key [][]byte, nPtr cache.Pointable[*node]) bool {
	var pPtr cache.Pointable[*node]
	nv := nPtr.Get()
	if !nv.parent.IsNil() {
		pPtr = tree.fetchW(nv.parent)
	}

	if nv.isLeaf() {
		tree.removeFromLeaf(key, pPtr, nPtr);
	} else {
		tree.removeFromInternal(key, nPtr);
	}

	minCapacity := int(math.Ceil(float64(tree.meta.degree) / 2) - 1)

	if len(nv.entries) < minCapacity {
		if nPtr.Ptr().Addr() == tree.meta.root.Addr() {
			if len(nv.entries) == 0 && len(nv.children) != 0 {
				tree.meta.dirty = true
				tree.meta.root = nv.children[0]
				rPtr := tree.rootW()
				rv := rPtr.Get()
				rv.Dirty(true)
				rv.parent = tree.heap.Nil()
				tree.removeFromInternal(key, rPtr)
				rPtr.Unlock()
				tree.freeNode(nPtr)
			}

			nPtr.Unlock()
			return true
		}

		var rightPtr cache.Pointable[*node]
		var leftPtr cache.Pointable[*node]
		var rv *node
		var lv *node

		if !nv.right.IsNil() {
			rightPtr = tree.fetchW(nv.right)
			rv = rightPtr.Get()
		}
		if !nv.left.IsNil() {
			leftPtr = tree.fetchW(nv.left)
			lv = leftPtr.Get()
		}

		if nv.isLeaf() {
			if        rightPtr != nil && rv.parent.Addr() == nv.parent.Addr() && len(rv.entries) > minCapacity {
				tree.borrowKeyFromRightLeaf(pPtr, nPtr, rightPtr)
			} else if leftPtr != nil && lv.parent.Addr() == nv.parent.Addr() && len(lv.entries) > minCapacity {
				tree.borrowKeyFromLeftLeaf(pPtr, nPtr, leftPtr)
			} else if rightPtr != nil && rv.parent.Addr() == nv.parent.Addr() && len(rv.entries) <= minCapacity {
				tree.mergeNodeWithRightLeaf(pPtr, nPtr, rightPtr)
			} else if leftPtr != nil && lv.parent.Addr() == nv.parent.Addr() && len(lv.entries) <= minCapacity {
				tree.mergeNodeWithLeftLeaf(pPtr, nPtr, leftPtr, rightPtr)
			}
		} else {
			pv := pPtr.Get()
			parentIndex, _ := pv.search(nv.entries[len(nv.entries)-1].key)
			if pv.children[parentIndex].Addr() != nPtr.Ptr().Addr() {
				parentIndex = -1
			}

			if len(pv.children) > parentIndex + 1 {
				rightPtr = tree.fetchW(pv.children[parentIndex + 1])
				rv = rightPtr.Get()
			}
			if parentIndex > 0 {
				leftPtr = tree.fetchW(pv.children[parentIndex - 1])
				lv = leftPtr.Get()
			}

			if        rv != nil && rv.parent.Addr() == nv.parent.Addr() && len(rv.entries) > minCapacity {
				tree.borrowKeyFromRightInternal(parentIndex, pPtr, nPtr, rightPtr)
			} else if lv != nil && lv.parent.Addr() == nv.parent.Addr() && len(lv.entries) > minCapacity {
				tree.borrowKeyFromLeftInternal(parentIndex, pPtr, nPtr, leftPtr)
			} else if rv != nil && rv.parent.Addr() == nv.parent.Addr() && len(rv.entries) <= minCapacity {
				tree.mergeNodeWithRightInternal(parentIndex, pPtr, nPtr, rightPtr)
			} else if lv != nil && lv.parent.Addr() == nv.parent.Addr() && len(lv.entries) <= minCapacity {
				tree.mergeNodeWithLeftInternal(parentIndex, pPtr, nPtr, leftPtr)
			}
		}

		tree.removeFromInternal(key, nPtr)
		if rightPtr != nil {
			tree.removeFromInternal(key, rightPtr)
			rightPtr.Unlock()
		}
		if leftPtr != nil {
			tree.removeFromInternal(key, leftPtr)
			leftPtr.Unlock()
		}
	}

	nPtr.Unlock()
	if (pPtr != nil) {
		tree.del(key, pPtr);
	}

	return true
}

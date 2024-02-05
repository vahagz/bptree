package bptree

import (
	"bytes"
	"math"

	"github.com/pkg/errors"
	"github.com/vahagz/bptree/helpers"
	allocator "github.com/vahagz/disk-allocator/heap"
	"github.com/vahagz/disk-allocator/heap/cache"
)

// Put puts the key-value pair into the B+ tree. Value will be
// updated or inserted depending on passed PutOptions.
func (tree *BPlusTree) Put(key, suffix [][]byte, val []byte, opt PutOptions) (bool, error) {
	success, err := tree.PutMem(key, suffix, val, opt)
	if err != nil {
		return false, err
	}

	return success, tree.WriteAll()
}

// PutMem puts the key-value pair into the B+ tree. Value will be
// updated or inserted depending on passed PutOptions. Unlike Put
// PutMem will not force to flush dirty nodes to disk after insertion/update.
func (tree *BPlusTree) PutMem(key, suffix [][]byte, val []byte, opt PutOptions) (bool, error) {
	key, err := tree.validateAndMerge(key, suffix)
	if err != nil {
		return false, err
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	e := entry{
		key: key,
		val: val,
	}

	success, err := tree.put(e, opt)
	if err != nil {
		return false, err
	}

	if success && !opt.Update {
		tree.meta.counter++
		tree.meta.count++
		tree.meta.dirty = true
	}

	return success, nil
}

// CanInsert returns true if passed (key + suffix) key can
// be inserted. Othervise false will be returned.
func (tree *BPlusTree) CanInsert(key, suffix [][]byte) bool {
	key, err := tree.validateAndMerge(key, suffix)
	if err != nil {
		return false
	}

	tree.mu.RLock()
	defer tree.mu.RUnlock()
	
	leaf, _, found := tree.searchRec(tree.rootR(), key, cache.READ)
	defer leaf.RUnlock()
	return !(found && tree.IsUniq())
}

// insert inserts entry to bptree. Is called when update if false
// in PutOptions.
func (tree *BPlusTree) insert(e entry) error {
	root := tree.rootW()
	leaf, index, found := tree.searchRec(root, e.key, cache.WRITE)
	if found && tree.IsUniq() {
		leaf.Unlock()
		return errors.New("key already exists")
	}

	leaf.Get().insertEntry(index, e)
	if leaf.Get().IsFull() {
		tree.split(leaf)
		return nil
	}

	leaf.Unlock()
	return nil
}

// update updates entry value bptree. Is called when update if true
// in PutOptions.
func (tree *BPlusTree) update(e entry) (updated bool, err error) {
	return updated, tree.scan(e.key, ScanOptions{
		Reverse: false,
		Strict:  true,
	}, cache.WRITE, func(
		key [][]byte,
		val []byte,
		index int,
		leaf cache.Pointable[*node],
	) (bool, error) {
		if helpers.CompareMatrix(e.key, key) == 0 && bytes.Compare(e.val, val) != 0 {
			leaf.Get().update(index, e.val)
			updated = true
		}
		return true, nil
	})
}

// always returns true if Update is false in PutOptions
// otherwise returns true if found entry that should be updated
func (tree *BPlusTree) put(e entry, opt PutOptions) (bool, error) {
	if opt.Update {
		return tree.update(e)
	}
	return true, tree.insert(e)
}

// split is helper function for insertion. Is called when leaf node
// overflew after insertion.
func (tree *BPlusTree) split(nPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	var siblingPtr cache.Pointable[*node]
	if nv.isLeaf() {
		siblingPtr = tree.alloc(nodeLeaf)
	} else {
		siblingPtr = tree.alloc(nodeInternal)
	}

	sv := siblingPtr.Get()
	breakPoint := int(math.Ceil(float64(tree.meta.degree-1) / 2))
	pe := nv.entries[breakPoint]

	nv.Dirty(true)
	sv.Dirty(true)

	sv.parent = nv.parent
	if nv.isLeaf() {
		sv.entries = make([]entry, 0, tree.meta.degree)
		sv.entries = append(sv.entries, nv.entries[breakPoint:]...)
		nv.entries = nv.entries[:breakPoint]

		pe.val = nil

		sv.right = nv.right
		sv.left = nPtr.Ptr()
		nv.right = siblingPtr.Ptr()
		if !sv.right.IsNil() {
			nNext := tree.fetchW(sv.right)
			nNext.Get().Dirty(true)
			nNext.Get().left = siblingPtr.Ptr()
			nNext.Unlock()
		}
	} else {
		sv.entries = make([]entry, 0, tree.meta.degree)
		sv.entries = append(sv.entries, nv.entries[breakPoint+1:]...)
		sv.children = make([]allocator.Pointable, 0, tree.meta.degree+1)
		sv.children = append(sv.children, nv.children[breakPoint+1:]...)
		for _, sChildPtr := range sv.children {
			sChild := tree.fetchW(sChildPtr)
			scv := sChild.Get()
			scv.Dirty(true)
			scv.parent = siblingPtr.Ptr()
			sChild.Unlock()
		}

		nv.entries = nv.entries[:breakPoint]
		nv.children = nv.children[:breakPoint+1]
	}

	var pPtr cache.Pointable[*node]
	if nv.parent.IsNil() {
		pPtr = tree.alloc(nodeInternal)
		tree.meta.dirty = true
		tree.meta.root = pPtr.Ptr()
		sv.parent = tree.meta.root
		nv.parent = tree.meta.root
		pPtr.Get().insertChild(0, nPtr.Ptr())
	} else {
		pPtr = tree.fetchW(nv.parent)
	}

	pv := pPtr.Get()
	pv.Dirty(true)
	index, _ := pv.search(pe.key)
	pv.insertEntry(index, pe)
	pv.insertChild(index + 1, siblingPtr.Ptr())

	if pv.parent.IsNil() {
		tree.meta.dirty = true
		tree.meta.root = pPtr.Ptr()
	}

	nPtr.Unlock()
	siblingPtr.Unlock()
	if pv.IsFull() {
		tree.split(pPtr)
		return
	}
	pPtr.Unlock()
}

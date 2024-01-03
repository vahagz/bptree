// Package bptree implements an on-disk bptree indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys and values
// can be blobs binary data.
package bptree

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/vahagz/bptree/customerrors"
	"github.com/vahagz/bptree/helpers"
	allocator "github.com/vahagz/disk-allocator/heap"
	"github.com/vahagz/disk-allocator/heap/cache"
	"github.com/vahagz/pager"
)

// bin is the byte order used for all marshals/unmarshals.
var bin = binary.LittleEndian

type suffixOption int

// these values used to set extra counter value
// FILL will set all bits with 1
// ZERO will set all bits with 0
// CURRENT will set current counter value (tree.meta.counter)
const (
	suffixFill suffixOption = iota
	suffixZero
	suffixCurrent
)

type nodeType int

const (
	nodeLeaf nodeType = iota
	nodeInternal
)

// Open opens the named file as a bptree index file and returns an instance
// bptree for use. Use ":memory:" for an in-memory bptree instance for quick
// testing setup. Degree of the tree is computed based on maxKeySize and pageSize
// used by the pager. If nil options are provided, defaultOptions will be used.
func Open(fileName string, opts *Options) (*BPlusTree, error) {
	if opts.Degree < 5 {
		return nil, errors.New("degree must be >= 5")
	}

	if opts.Uniq {
		opts.SuffixCols = 0
		opts.MaxSuffixSize = 0
	}

	pagerFile := fmt.Sprintf("%s.idx", fileName)
	p, err := pager.Open(pagerFile, opts.PageSize, false, 0644)
	if err != nil {
		return nil, err
	}

	heap, err := allocator.Open(fileName, &allocator.Options{
		TargetPageSize: uint16(opts.PageSize),
		TreePageSize:   uint16(opts.PageSize),
		Pager:          p,
	})
	if err != nil {
		return nil, err
	}

	tree := &BPlusTree{
		file: pagerFile,
		mu:   &sync.RWMutex{},
		heap: heap,
	}

	tree.cache = cache.NewCache[*node](opts.CacheSize, tree.newNode)

	if err := tree.open(opts); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree represents an on-disk bptree. Size of each node is
// decided based on key size, value size and tree degree.
type BPlusTree struct {
	file    string              // name of the bptree file on disk
	metaPtr allocator.Pointable // pointer to tree metadata on disk

	// tree state
	mu    *sync.RWMutex        // used for concurrency safety
	heap  *allocator.Allocator // file where actual tree data will be stored
	cache *cache.Cache[*node]  // cache to store in-memory nodes to avoid io
	meta  *metadata            // tree metadata
}

// WriteAll writes all the nodes marked 'dirty' to the underlying pager.
func (tree *BPlusTree) WriteAll() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	return tree.writeAll()
}

// PrepareSpace allocates size bytes on underlying bptree file.
// This is usefull if big amount of data is going to be inserted.
// It's increases performance of insertion.
func (tree *BPlusTree) PrepareSpace(size uint32) {
	tree.heap.PreAlloc(size)
}

// Size returns the number of entries in the entire tree.
func (tree *BPlusTree) Size() int64 { return int64(tree.meta.size) }

// IsUniq returns true if tree keys are configured as uniq. Othervise false.
func (tree *BPlusTree) IsUniq() bool { return helpers.GetBit(tree.meta.flags, uniquenessBit) }

// Close flushes any writes and closes the underlying pager.
func (tree *BPlusTree) Close() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.heap == nil {
		return nil
	}

	_ = tree.writeAll() // write if any nodes are pending
	err := tree.heap.Close()
	tree.heap = nil
	return err
}

// Options returns copy of tree options.
func (tree *BPlusTree) Options() Options {
	return Options{
		PageSize:      int(tree.meta.pageSize),
		MaxKeySize:    int(tree.meta.keySize),
		KeyCols:       int(tree.meta.keyCols),
		MaxSuffixSize: int(tree.meta.suffixSize),
		SuffixCols:    int(tree.meta.suffixCols),
		MaxValueSize:  int(tree.meta.valSize),
		Degree:        int(tree.meta.degree),
		Uniq:          tree.IsUniq(),
	}
}

// String prints tree general info.
func (tree *BPlusTree) String() string {
	return fmt.Sprintf(
		"BPlusTree{file='%s', size=%d, degree=%d}",
		tree.file, tree.Size(), tree.meta.degree,
	)
}

// Print pretty prints tree into terminal. Not recomended
// to call if tree is too big.
func (tree *BPlusTree) Print() {
	root := tree.rootR()
	defer root.RUnlock()
	fmt.Println("============= bptree =============")
	tree.print(root, 0, cache.READ)
	fmt.Println("============ freelist ============")
	tree.heap.Print()
	fmt.Println("==================================")
}

// ClearCache flushes all cached data to
// disk (marked as 'dirty') and frees memory.
func (tree *BPlusTree) ClearCache() {
	tree.cache.Clear()
}

// Remove removes tree data from disk.
func (tree *BPlusTree) Remove() {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	tree.heap.Remove()
}

// AddSuffix adds extra counter bytes at end of key
// if Uniq option was set to False while creating BPTree.
func (tree *BPlusTree) AddSuffix(key [][]byte, flag suffixOption) [][]byte {
	if tree.IsUniq() {
		return key
	}

	suf := make([]byte, tree.meta.suffixSize)
	if flag == suffixFill {
		for i := range suf {
			suf[i] = math.MaxUint8
		}
	} else if flag == suffixZero {
		// do nothing, already filled with zeros
	} else if flag == suffixCurrent {
		bin.PutUint64(suf[0:8], tree.meta.counter)
	}

	return append(key, suf)
}

// reverse version of AddSuffix.
func (tree *BPlusTree) RemoveSuffix(key [][]byte) [][]byte {
	return key[:len(key)-int(tree.meta.suffixCols)]
}

// CheckConsistency checks consistency of tree. It traverses
// tree and checks all bptree rules on each node and if something
// is wront false will be returned.
func (tree *BPlusTree) CheckConsistency(list [][]byte) bool {
	maxChildCount := tree.meta.degree
	maxEntryCount := maxChildCount - 1
	minChildCount := uint16(math.Ceil(float64(tree.meta.degree) / 2))
	minEntryCount := minChildCount - 1

	type Itm struct{
		val   []byte
		count int
	}

	m := map[string]*Itm{}
	for i := range list {
		if itm, ok := m[string(list[i])]; ok {
			itm.count++
		} else {
			m[string(list[i])] = &Itm{
				val: list[i],
				count: 1,
			}
		}
	}

	defer func() {
		if err := recover(); err != nil {
			if v, check := err.(bool); check && v == false {
				return
			}

			panic(err)
		}
	}()

	var traverse func(nPtr cache.Pointable[*node], indent int, flag cache.LOCKMODE)
	traverse = func(nPtr cache.Pointable[*node], indent int, flag cache.LOCKMODE) {
		n := nPtr.Get()

		entryCount := uint16(len(n.entries))
		childCount := uint16(len(n.children))
		if (
				nPtr.Ptr().Addr() == tree.meta.root.Addr() && (
					entryCount == 0 ||
					entryCount > maxEntryCount)) || (
				nPtr.Ptr().Addr() != tree.meta.root.Addr() && (
					entryCount < minEntryCount ||
					entryCount > maxEntryCount)) {
			if !n.isLeaf() && (
				childCount < minChildCount ||
				childCount > maxChildCount) {
					fmt.Println("node violated")
					fmt.Println("entry count =>", len(n.entries))
					fmt.Println("child count =>", len(n.children))
					fmt.Println("ptr =>", nPtr.Ptr().Addr())
					panic(false)
				}
		}

		for i := len(n.entries) - 1; i >= 0; i-- {
			if !n.isLeaf() {
				child := tree.fetchF(n.children[i+1], flag)
				traverse(child, indent + 4, flag)
				defer child.UnlockFlag(flag)
			}
			
			if _, ex := m[string(n.entries[i].key[0])]; !ex {
				if n.isLeaf() {
					fmt.Println("unexpected leaf entry value =>", n.entries[i].key[0])
				} else if !n.isLeaf() {
					fmt.Println("unexpected internal entry value =>", n.entries[i].key[0])
				}
				panic(false)
			}
		}

		if !n.isLeaf() {
			child := tree.fetchF(n.children[0], flag)
			traverse(child, indent + 4, flag)
			defer child.UnlockFlag(flag)
		}
	}

	traverse(tree.rootF(cache.NONE), 0, cache.NONE)

	for k := range m {
		vals, err := tree.Get([][]byte{m[k].val}, nil)
		if err != nil {
			panic(err)
		} else if len(vals) == 0 {
			fmt.Println("key not found =>", m[k])
			panic(false)
		}
	}

	return true
}

// validateAndMerge esures that passed key and suffix
// satisfy tree requirements. If anything is from error
// will be returned. Othervise will return merged key
// with key and suffix.
func (tree *BPlusTree) validateAndMerge(key, suffix [][]byte) ([][]byte, error) {
	key = helpers.Copy(key)
	if suffix != nil {
		for _, v := range suffix {
			key = append(key, append(make([]byte, 0, len(v)), v...))
		}
	}

	keylen := 0
	for _, v := range key {
		keylen += len(v)
	}

	if keylen > int(tree.meta.keySize + tree.meta.suffixSize) {
		return nil, customerrors.ErrKeyTooLarge
	} else if keylen == 0 {
		return nil, customerrors.ErrEmptyKey
	}

	return key, nil
}

// print pretty prints tree into terminal.
func (tree *BPlusTree) print(nPtr cache.Pointable[*node], indent int, flag cache.LOCKMODE) {
	n := nPtr.Get()
	for i := len(n.entries) - 1; i >= 0; i-- {
		if !n.isLeaf() {
			child := tree.fetchF(n.children[i+1], flag)
			tree.print(child, indent + 4, flag)
			defer child.UnlockFlag(flag)
		}
		fmt.Printf("%*s%v(%v)\n", indent, "", n.entries[i].key, nPtr.Ptr().Addr())
	}

	if !n.isLeaf() {
		child := tree.fetchF(n.children[0], flag)
		tree.print(child, indent + 4, flag)
		defer child.UnlockFlag(flag)
	}
}

// searchRec searches the sub-tree with root 'n' recursively until the key
// is found or the leaf node is reached. Returns the node last searched,
// index where the key should be and a flag to indicate if the key exists.
func (tree *BPlusTree) searchRec(
	n cache.Pointable[*node],
	key [][]byte,
	flag cache.LOCKMODE,
) (
	ptr cache.Pointable[*node],
	index int,
	found bool,
) {
	for !n.Get().isLeaf() {
		index, found = n.Get().search(key)
		ptr = tree.fetchF(n.Get().children[index], flag)

		n.UnlockFlag(flag)
		n = ptr
	}

	index, found = n.Get().search(key)
	return n, index, found
}

// rightLeaf returns the right most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) rightLeaf(n cache.Pointable[*node], flag cache.LOCKMODE) cache.Pointable[*node] {
	if n.Get().isLeaf() {
		return n
	}

	child := tree.fetchF(n.Get().children[len(n.Get().children) - 1], flag)
	n.UnlockFlag(flag)
	return tree.rightLeaf(child, flag)
}

// leftLeaf returns the left most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) leftLeaf(n cache.Pointable[*node], flag cache.LOCKMODE) cache.Pointable[*node] {
	if n.Get().isLeaf() {
		return n
	}

	child := tree.fetchF(n.Get().children[0], flag)
	n.UnlockFlag(flag)
	return tree.leftLeaf(child, flag)
}

// fetchF locks node wrapped pointer based on flag and returns
// the node from given pointer. Underlying file is accessed
// only if the node doesn't exist in cache.
func (tree *BPlusTree) fetchF(ptr allocator.Pointable, flag cache.LOCKMODE) cache.Pointable[*node] {
	nPtr := tree.cache.GetF(ptr, flag)
	if nPtr != nil {
		return nPtr
	}

	n := tree.newNode()
	if err := ptr.Get(n); err != nil {
		panic(errors.Wrap(err, "failed to get node data from pointer"))
	}

	n.Dirty(false)
	return tree.cache.AddF(ptr, flag)
}

// fetchW locks for read node wrapped pointer and returns.
func (tree *BPlusTree) fetchR(ptr allocator.Pointable) cache.Pointable[*node] {
	return tree.fetchF(ptr, cache.READ)
}

// fetchW locks for write node wrapped pointer and returns.
func (tree *BPlusTree) fetchW(ptr allocator.Pointable) cache.Pointable[*node] {
	return tree.fetchF(ptr, cache.WRITE)
}

// rootF locks root node wrapped pointer based on flag and returns.
func (tree *BPlusTree) rootF(flag cache.LOCKMODE) cache.Pointable[*node] {
	return tree.fetchF(tree.meta.root, flag)
}

// rootR locks for read root node wrapped pointer and returns.
func (tree *BPlusTree) rootR() cache.Pointable[*node] {
	return tree.rootF(cache.READ)
}

// rootW locks for write root node wrapped pointer and returns.
func (tree *BPlusTree) rootW() cache.Pointable[*node] {
	return tree.rootF(cache.WRITE)
}

// newNode returns new in-memory node.
func (tree *BPlusTree) newNode() *node {
	return &node{
		dirty:    true,
		meta:     tree.meta,
		dummyPtr: tree.heap.Nil(),
		right:    tree.heap.Nil(),
		left:     tree.heap.Nil(),
		parent:   tree.heap.Nil(),
		entries:  make([]entry, 0),
		children: make([]allocator.Pointable, 0),
	}
}

// alloc returns pointer to new node on disk. Node type is passed
// from arguments. Node size depends from node type (leaf, internal).
func (tree *BPlusTree) alloc(nt nodeType) cache.Pointable[*node] {
	var size uint32
	switch nt {
		case nodeLeaf: size = tree.leafNodeSize()
		case nodeInternal: size = tree.internalNodeSize()
		default: panic(fmt.Errorf("Invalid node type => %v", nt))
	}

	cPtr := tree.cache.AddW(tree.heap.Alloc(size))
	_ = cPtr.New() // in underhoods calls newNode method of bptree and assigns to pointer wrapper
	return cPtr
}

// freeNode removes node from cache and marks free in heap
// for future allocations.
func (tree *BPlusTree) freeNode(ptr cache.Pointable[*node]) {
	rawPtr := ptr.Ptr()
	tree.cache.Del(rawPtr)
	tree.heap.Free(rawPtr)
}

// leafNodeSize returns leaf node size based on
// tree metadata (key size, value size, degree).
func (tree *BPlusTree) leafNodeSize() uint32 {
	return uint32(leafNodeSize(
		int(tree.meta.degree),
		int(tree.meta.keySize + tree.meta.suffixSize),
		int(tree.meta.keyCols + tree.meta.suffixCols),
		int(tree.meta.valSize),
	))
}

// internalNodeSize returns internal node size based on
// tree metadata (key size, degree).
func (tree *BPlusTree) internalNodeSize() uint32 {
	return uint32(internalNodeSize(
		int(tree.meta.degree),
		int(tree.meta.keySize + tree.meta.suffixSize),
		int(tree.meta.keyCols + tree.meta.suffixCols),
	))
}

// open opens the bptree stored on disk using the heap.
// If heap is empty, a new bptree will be initialized.
func (tree *BPlusTree) open(opts *Options) error {
	tree.metaPtr = tree.heap.FirstPointer(metadataSize)
	if tree.heap.Size() == tree.metaPtr.Addr() - allocator.PointerMetaSize {
		// heap is empty, initialize a new bptree
		return tree.init(opts)
	}

	tree.meta = &metadata{
		root: tree.heap.Nil(),
	}
	if err := tree.metaPtr.Get(tree.meta); err != nil {
		return errors.Wrap(err, "failed to read meta while opening bptree")
	}

	// verify metadata
	if tree.meta.version != version {
		return fmt.Errorf("incompatible version %#x (expected: %#x)", tree.meta.version, version)
	}

	tree.cache.Add(tree.meta.root)
	return nil
}

// init initializes a new bptree in the underlying file. allocates 2 pages
// (1 for meta + 1 for root) and initializes the instance. metadata and the
// root node are expected to be written to file during insertion.
func (tree *BPlusTree) init(opts *Options) error {
	tree.meta = &metadata{
		dirty:      true,
		version:    version,
		flags:      0,
		size:       0,
		pageSize:   uint32(opts.PageSize),
		suffixCols: uint16(opts.SuffixCols),
		suffixSize: uint16(opts.MaxSuffixSize),
		keySize:    uint16(opts.MaxKeySize),
		keyCols:    uint16(opts.KeyCols),
		valSize:    uint16(opts.MaxValueSize),
		counter:    0,
		degree:     uint16(opts.Degree),
		cacheSize:  uint32(opts.CacheSize),
	}

	if !opts.Uniq && opts.SuffixCols == 0 {
		// add extra column for counter to maintain uniqness
		tree.meta.suffixCols = 1
		tree.meta.suffixSize = 8
	}

	helpers.SetBit(&tree.meta.flags, uniquenessBit, opts.Uniq)

	tree.metaPtr = tree.heap.Alloc(metadataSize)

	rootPtr := tree.alloc(nodeLeaf)
	tree.meta.root = rootPtr.Ptr()
	rootPtr.Unlock()

	return errors.Wrap(tree.metaPtr.Set(tree.meta), "failed to write meta after init")
}

// writeAll writes all the nodes marked dirty to the underlying pager.
func (tree *BPlusTree) writeAll() error {
	tree.cache.Flush()
	return tree.writeMeta()
}

// writeMeta writes meta to disk if marked dirty.
func (tree *BPlusTree) writeMeta() error {
	if tree.meta.dirty {
		return tree.metaPtr.Set(tree.meta)
	}
	return nil
}

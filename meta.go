package bptree

import (
	allocator "github.com/vahagz/disk-allocator/heap"
)

const (
	// version of bptree implementation. Used to determine incompatible
	// versions of bptree package.
	version       = uint8(0x1)

	// metadataSize is count of bytes necessary to store metadata on disk.
	metadataSize  = 40 + allocator.PointerSize

	// uniquenessBit is index of bit in node flags.
	uniquenessBit = 0b00000001
)

// metadata represents the metadata for the bptree stored in a file.
type metadata struct {
	// temporary state info
	dirty bool

	// actual metadata
	magic      uint16              // magic marker to identify bptree.
	version    uint8               // version of implementation.
	flags      uint8               // flags.
	suffixCols uint16              // columns count of suffix in key.
	suffixSize uint16              // maximum suffix size allowed.
	keyCols    uint16              // columns count in key.
	keySize    uint16              // maximum key size allowed.
	valSize    uint16              // maximum value size allowed.
	pageSize   uint32              // page size used to initialize.
	degree     uint16              // number of entries per node.
	count      uint64              // number of entries in the tree.
	counter    uint64              // counter increases on every insertion.
	cacheSize  uint32              // maximum count of in-memory cached nodes to avoid io
	root       allocator.Pointable // pointer to root node.
}

// implementation of encoding.BinaryMarshaler interface
func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSize)
	rootPtrBytes, err := m.root.MarshalBinary()
	if err != nil {
		return nil, err
	}

	bin.PutUint16(buf[0:2], m.magic)
	buf[2] = m.version
	buf[3] = m.flags
	bin.PutUint16(buf[4:6], m.suffixCols)
	bin.PutUint16(buf[6:8], m.suffixCols)
	bin.PutUint16(buf[8:10], m.keyCols)
	bin.PutUint16(buf[10:12], m.keySize)
	bin.PutUint16(buf[12:14], m.valSize)
	bin.PutUint32(buf[14:18], m.pageSize)
	bin.PutUint16(buf[18:20], m.degree)
	bin.PutUint64(buf[20:28], m.count)
	bin.PutUint64(buf[28:36], m.counter)
	bin.PutUint32(buf[36:40], m.cacheSize)
	copy(buf[40:], rootPtrBytes)
	return buf, nil
}

// implementation of encoding.BinaryUnmarshaler interface
func (m *metadata) UnmarshalBinary(d []byte) error {
	m.magic = bin.Uint16(d[0:2])
	m.version = d[2]
	m.flags = d[3]
	m.suffixCols = bin.Uint16(d[4:6])
	m.suffixSize = bin.Uint16(d[6:8])
	m.keyCols = bin.Uint16(d[8:10])
	m.keySize = bin.Uint16(d[10:12])
	m.valSize = bin.Uint16(d[12:14])
	m.pageSize = bin.Uint32(d[14:18])
	m.degree = bin.Uint16(d[18:20])
	m.count = bin.Uint64(d[20:28])
	m.counter = bin.Uint64(d[28:36])
	m.cacheSize = bin.Uint32(d[36:40])
	m.root.UnmarshalBinary(d[40:])
	return nil
}

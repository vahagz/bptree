package bptree

// Options represents the configuration options for the B+ tree index.
type Options struct {
	// PageSize to be for file I/O. All reads and writes will always
	// be done with pages of this size. Must be multiple of 4096.
	PageSize int `json:"page_size"`

	// MaxKeySize represents the maximum size allowed for the key.
	// Put call with keys larger than this will result in error.
	MaxKeySize int `json:"max_key_size"`

	// Count of columns of key
	KeyCols int `json:"key_cols"`

	// Count of columns of suffix in key
	// If set 0, bptree will take value from counter
	SuffixCols int `json:"suffix_cols"`

	// Max size allowed for suffix
	MaxSuffixSize int `json:"max_suffix_size"`

	// MaxValueSize represents the maximum size allowed for the value.
	// Put call with values larger than this will result in error.
	// Branching factor reduces as this size increases. So smaller
	// the better.
	MaxValueSize int `json:"max_value_size"`

	// number of children per node
	Degree int `json:"degree"`

	// if set true, values inserted must be unique, othervise values can repeat
	// but BPTree will add extra bytes at end of key to maintain uniqueness
	Uniq bool `json:"uniq"`

	// Max count of in-memory cached nodes to avoid io
	CacheSize int `json:"cache_size"`
}

// PutOptions tells bptree how to put kev-value pair
type PutOptions struct {
	// if set true, will try to find key-value pair with given key and update value.
	// Othervise will insert new key-value pair
	Update bool
}

// ScanOptions tells bptree how to start tree scan
type ScanOptions struct {
	// if Key present, scan will start from given key
	Key [][]byte

	// if set true, scan will be in decreasing order on keys.
	Reverse bool

	// if set true, given key will be included in scan.
	Strict bool
}

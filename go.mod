module github.com/vahagz/bptree

go 1.19

replace (
	github.com/vahagz/disk-allocator/heap v0.0.2 => ./pkg/disk-allocator/heap
	github.com/vahagz/pager v0.0.1 => ./pkg/disk-allocator/pkg/rbtree/pkg/pager
	github.com/vahagz/rbtree v0.0.1 => ./pkg/disk-allocator/pkg/rbtree
)

require (
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.4
	github.com/vahagz/disk-allocator v0.0.2
	github.com/vahagz/pager v0.0.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/vahagz/rbtree v0.0.1 // indirect
	golang.org/x/sys v0.16.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

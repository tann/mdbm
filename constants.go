// +build !linux,cgo

package mdbm

const (
	Async    int = 0x00000040 // Perform asynchronous writes
	Fsync    int = 0x00000080 // Sync file on close
	Create   int = 0x00000200 // Create file if it does not exist
	Truncate int = 0x00000400 // Truncate file
)

// +build linux,cgo
package mdbm

var (
	Create   int = 0x00000040 // Create file if it does not exist
	Truncate int = 0x00000200 // Truncate file
	Fsync    int = 0x00001000 // Sync file on close
	Async    int = 0x00002000 // Perform asynchronous writes
)

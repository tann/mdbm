package mdbm

/*

#cgo CFLAGS: -I/usr/include
#cgo LDFLAGS: -lmdbm

#include <stdlib.h>

#include "mdbm.h"

// cgo ain't playin' nice with C macros
void mdbm_iter_init(MDBM_ITER* iter) {
    MDBM_ITER_INIT(iter)
}

*/
import "C"

import (
	"errors"
	"log"
	"sync"
	"unsafe"
)

// These flags given to mdbm.Flags which set them when opening a DB
var (
	ReadWrite = C.MDBM_O_RDWR   // Read-write access
	WriteOnly = C.MDBM_O_WRONLY // Write-only access (deprecated in V3)
	ReadOnly  = C.MDBM_O_RDONLY // Read-only access
	Truncate  = C.MDBM_O_TRUNC  // Truncate file
	Create    = C.MDBM_O_CREAT  // Create file if it does not exist
	Async     = C.MDBM_O_ASYNC  // Asynchronous writes
	Fsync     = C.MDBM_O_FSYNC  // Sync file on close

	DirectIO = C.MDBM_O_DIRECT // Perform direction I/O

	LockAny       = C.MDBM_ANY_LOCKS         // Open, even if existing locks don't match flags
	LockPartition = C.MDBM_PARTITIONED_LOCKS // Partitioned locks
	LockReadWrite = C.MDBM_RW_LOCKS          // Read-write locks
	LockNone      = C.MDBM_OPEN_NOLOCK       // Don't lock during open
)

type MDBM struct {
	dbh                        *C.MDBM     // DB handle
	iter                       C.MDBM_ITER // DB iterator
	entry                      C.kvpair    // Last fetched entry w/ iter
	hasLock                    bool        // Exclusive DB lock
	flags, perms, psize, dsize int         // Options for openning DB

	mutex sync.Mutex
}

type option func(*MDBM)

func (db *MDBM) setFlags(flags int) {
	db.flags = flags
}

// Flags sets flags when opening a DB
func Flags(flags int) option {
	return func(db *MDBM) {
		db.setFlags(flags)
	}
}

func (db *MDBM) setPerms(perms int) {
	db.perms = perms
}

// Perms sets permissions to open/create a new DB
func Perms(perms int) option {
	return func(db *MDBM) {
		db.setPerms(perms)
	}
}

func (db *MDBM) setPageSize(size int) {
	db.psize = size
}

// PageSize sets page size in KB
func PageSize(size int) option {
	return func(db *MDBM) {
		db.setPageSize(size)
	}
}

func (db *MDBM) setStartSize(size int) {
	db.dsize = size
}

// StartSize sets initial size in KB for a new DB
func StartSize(size int) option {
	return func(db *MDBM) {
		db.setStartSize(size)
	}
}

// Open opens an existing DB or creates a new DB. Options can be passed in
// using these setters:
//	mdbm.Flags (Default mdbm.ReadWrite | mdbm.Create)
//	mdbm.Perms (Default 0600)
//	mdbm.PageSize (Default 4KB)
//	mdbm.StartSize (Default 1MB)
func Open(dbfile string, options ...option) (db *MDBM, err error) {
	db = &MDBM{
		flags: ReadWrite | Create,
		perms: 0666,
		psize: 0,
		dsize: 0,
	}
	C.mdbm_iter_init(&db.iter)

	for _, opt := range options {
		opt(db)
	}

	fn := C.CString(dbfile)
	defer C.free(unsafe.Pointer(fn))

	db.dbh, err = C.mdbm_open(fn, C.int(db.flags), C.int(db.perms), C.int(db.psize), C.int(db.dsize))
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Dup duplicates an existing DB handle
func (db *MDBM) Dup(options ...option) (dup *MDBM, err error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	dup = &MDBM{
		flags: db.flags,
		perms: db.perms,
		psize: db.psize,
		dsize: db.dsize,
	}
	C.mdbm_iter_init(&dup.iter)

	dup.dbh, err = C.mdbm_dup_handle(db.dbh, 0)
	if err != nil {
		return nil, err
	}

	return dup, nil
}

// Close closes DB opened by DB handle
func (db *MDBM) Close() {
	C.mdbm_close(db.dbh)
}

// Get gets a value for a given key
func (db *MDBM) Get(key []byte) (val []byte, err error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var k, v C.datum

	k.dptr = (*C.char)(unsafe.Pointer(&key[0]))
	k.dsize = C.int(len(key))

	C.mdbm_lock_smart(db.dbh, &k, 0)
	defer C.mdbm_unlock_smart(db.dbh, &k, 0)
	v, e := C.mdbm_fetch(db.dbh, k)
	if e != nil {
		return nil, errors.New("Cannot retrieve entry: " + e.Error())
	}
	return C.GoBytes(unsafe.Pointer(v.dptr), v.dsize), nil
}

// Put saves a key-value entry
func (db *MDBM) Put(key []byte, val []byte) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var k, v C.datum
	k.dptr = (*C.char)(unsafe.Pointer(&key[0]))
	k.dsize = C.int(len(key))
	v.dptr = (*C.char)(unsafe.Pointer(&val[0]))
	v.dsize = C.int(len(val))

	// Is locking here needed? storing/deleting implements
	// implicit locking
	C.mdbm_lock(db.dbh)
	defer C.mdbm_unlock(db.dbh)
	_, e := C.mdbm_store(db.dbh, k, v, C.MDBM_REPLACE)
	if e != nil {
		return errors.New("Cannot store entry: " + e.Error())
	}
	return nil
}

// Delete deletes an entry given a key
func (db *MDBM) Delete(key []byte) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var k C.datum
	k.dptr = (*C.char)(unsafe.Pointer(&key[0]))
	k.dsize = C.int(len(key))

	// Same as storing. Lock is already done
	// implicitly internally
	C.mdbm_lock(db.dbh)
	defer C.mdbm_unlock(db.dbh)
	_, e := C.mdbm_delete(db.dbh, k)
	if e != nil {
		return errors.New("Cannot delete entry: " + e.Error())
	}
	return nil
}

// Lock locks DB exclusively
func (db *MDBM) Lock() error {
	if !db.hasLock {
		_, e := C.mdbm_lock(db.dbh)
		if e != nil {
			return e
		}
		log.Println("Got lock!")
		db.hasLock = true
	}
	return nil
}

// Unlock releases current lock set by Lock
func (db *MDBM) Unlock() error {
	if db.hasLock {
		_, e := C.mdbm_unlock(db.dbh)
		if e != nil {
			return e
		}
		log.Println("Released lock!")
		db.hasLock = false
	}
	return nil
}

// Restart resets DB iterator
func (db *MDBM) Restart() {
	C.mdbm_iter_init(&db.iter)
}

// Fetch iterates through entries in DB for fetching. Each entry can be
// retrieved by making a call to Entry().
func (db *MDBM) Fetch() bool {
	db.Lock()
	db.entry = C.mdbm_next_r(db.dbh, &db.iter)
	if db.entry.key.dptr != nil && db.entry.key.dsize != 0 {
		return true
	}
	db.Unlock()
	return false
}

// Entry returns the last fetched entry using an iterator by calling Fetch()
func (db *MDBM) Entry() (key []byte, val []byte) {
	k := db.entry.key
	v := db.entry.val
	return C.GoBytes(unsafe.Pointer(k.dptr), k.dsize), C.GoBytes(unsafe.Pointer(v.dptr), v.dsize)
}

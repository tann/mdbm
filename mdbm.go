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

const (
	ReadOnly  int = 0x00000000 // Read-only access
	WriteOnly int = 0x00000001 // Write-only access (deprecated in V3)
	ReadWrite int = 0x00000002 // Read and write access

	DirectIO int = 0x00004000 // Perform direction I/O

	LockAny       int = 0x00020000 // Open, even if existing locks don't match flags
	LockPartition int = 0x02000000 // Partitioned locks
	LockReadWrite int = 0x08000000 // Read-write locks
	LockNone      int = 0x80000000 // Don't lock during open
)

type MDBM struct {
	dbh                        *C.MDBM     // DB handle
	iter                       C.MDBM_ITER // DB iterator
	entry                      C.kvpair    // Last fetched entry w/ iter
	hasLock                    bool        // Exclusive DB lock
	flags, perms, psize, dsize int         // Options for openning DB

	mutex sync.Mutex
}

type Option func(*MDBM)

/*

db, err := mdbm.Open("my.mdbm")
...


flags := mdbm.ReadWrite | mdbm.Create | mdbm.Truncate
db, err := mdbm.Open("my.mdbm", mdbm.Flags(flags), mdbm.Perms(0600), mdbm.PageSize(512), mdbm.StartSize(1024))
...

*/

func (db *MDBM) setFlags(flags int) {
	db.flags = flags
}

func Flags(flags int) Option {
	return func(db *MDBM) {
		db.setFlags(flags)
	}
}

func (db *MDBM) setPerms(perms int) {
	db.perms = perms
}

func Perms(perms int) Option {
	return func(db *MDBM) {
		db.setPerms(perms)
	}
}

func (db *MDBM) setPageSize(size int) {
	db.psize = size
}

func PageSize(size int) Option {
	return func(db *MDBM) {
		db.setPageSize(size)
	}
}

func (db *MDBM) setStartSize(size int) {
	db.dsize = size
}

func StartSize(size int) Option {
	return func(db *MDBM) {
		db.setStartSize(size)
	}
}

// db, err := mdbm.Open("my.mdbm")
// ...
//
//
// flags := mdbm.ReadWrite | mdbm.Create | mdbm.Truncate
// db, err := mdbm.Open("my.mdbm", mdbm.Flags(flags), mdbm.Perms(0600), mdbm.PageSize(512), mdbm.StartSize(1024))
// ...
//
func Open(dbfile string, options ...Option) (db *MDBM, err error) {
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

func (db *MDBM) Dup(options ...Option) (dup *MDBM, err error) {
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

func (db *MDBM) Close() {
	C.mdbm_close(db.dbh)
}

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

func (db *MDBM) Restart() {
	C.mdbm_iter_init(&db.iter)
}

func (db *MDBM) Fetch() bool {
	db.Lock()
	db.entry = C.mdbm_next_r(db.dbh, &db.iter)
	if db.entry.key.dptr != nil && db.entry.key.dsize != 0 {
		return true
	}
	db.Unlock()
	return false
}

func (db *MDBM) Entry() (key []byte, val []byte) {
	k := db.entry.key
	v := db.entry.val
	return C.GoBytes(unsafe.Pointer(k.dptr), k.dsize), C.GoBytes(unsafe.Pointer(v.dptr), v.dsize)
}

package mdbm_test

import (
	"fmt"
	"log"

	"github.com/tann/mdbm"
)

func Example() {
	// Open a DB for reading and writing (creating if not existing)
	db, err := mdbm.Open("my.db")
	if err != nil {
		log.Fatal("Cannot open DB: " + err.Error())
	}
	defer db.Close()

	// Store some key-value
	err = db.Put([]byte("key1"), []byte("val1"))
	if err != nil {
		log.Println("Cannot store \"key1\":", err.Error())
	}

	// Get value
	val, err := db.Get([]byte("key1"))
	if err != nil {
		log.Println("Cannot fetch value for \"key1\"", err.Error())
	}

	// Populate DB with entries
	for i := 0; i < 1000000; i++ {
		k := []byte(fmt.Sprintf("%dx%d", i, i))
		v := []byte(fmt.Sprintf("%d", i*i))
		db.Put(k, v)
	}

	// Iterate through all entries in DB
	for db.Fetch() {
		k, v := db.Entry()
	}
}

func ExampleMDBM_Open() {
	// Open with user-defined options
	flags := mdbm.ReadWrite | mdbm.Truncate | mdbm.Create
	db, err := mdbm.Open("my.db", mdbm.Flags(flags), mdbm.Perms(0666))
	if err != nil {
		log.Fatal("Cannot open my.db: " + err.Error())
	}
	defer db.Close()
}

func ExampleMDBM_Fetch() {
	db, err := mdbm.Open("my.db")
	if err != nil {
		log.Fatal("Cannot open my.db: " + err.Error())
	}
	defer db.Close()

	// Iterate with db.Fetch()
	for db.Fetch() {
		_, v := db.Entry()
		if string(v) == "127.0.0.1" {
			log.Println("Localhost IP exists!")

			// Make sure to release the lock before break
			db.Unlock()
			break
		}
	}
}

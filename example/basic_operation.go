package main

import (
	bitcask "bitcask-go"
	"fmt"
)

func main() {
	opts := bitcask.DefaultOptions
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("hello"), []byte("world"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val: ", string(val))

	err = db.Delete([]byte("hello"))
	if err != nil {
		panic(err)
	}

}

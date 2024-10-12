package benchmark

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

var db *bitcask.DB

func init() {
	var err error
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-bench")
	opts.DirPath = dir
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("filed to open bitcask: %v", err))
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

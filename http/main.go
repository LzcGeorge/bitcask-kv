package main

import (
	bitcask "bitcask-go"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	var err error
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-http")
	opts.DirPath = dir
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("filed to open bitcask: %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var kv map[string]string
	// 解析请求体, 从 body 中拿数据
	if err := json.NewDecoder(request.Body).Decode(&kv); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range kv {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put key: %s, %v", key, err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get key: %s, %v", key, err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]string{key: string(value)})
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && err != bitcask.ErrKeyIsEmpty {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete key: %s, %v", key, err)
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var res []string
	for _, key := range keys {
		res = append(res, string(key))
	}
	_ = json.NewEncoder(writer).Encode(res)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func handleMerge(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	_ = db.Merge()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}
func main() {
	http.HandleFunc("/put", handlePut)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/delete", handleDelete)
	http.HandleFunc("/listkeys", handleListKeys)
	http.HandleFunc("/stat", handleStat)
	http.HandleFunc("/merge", handleMerge)

	_ = http.ListenAndServe("localhost:8080", nil)
}

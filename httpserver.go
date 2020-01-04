package main

import (
	// "flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"net/http"
)

//search in kv store
var db *leveldb.DB

func main() {
	db, errdb := leveldb.OpenFile("kvs/iich", nil)
	if errdb != nil {
		log.Fatal(errdb)
	}
	defer db.Close()

	http.HandleFunc("/s", func(w http.ResponseWriter, r *http.Request) {
		keys, ok := r.URL.Query()["wd"]

		if !ok || len(keys[0]) < 1 {
			fmt.Fprintf(w, "Url Param 'wd' is missing")
			return
		}

		key := keys[0]
		log.Println(string(key))
		if data, err := db.Get([]byte(string(key)), nil); err != nil {
			//循环遍历数据
			/*fmt.Println("循环遍历数据")
			iter := db.NewIterator(nil, nil)
			for iter.Next() {
				fmt.Printf("key:%s, value:%s\n", iter.Key(), iter.Value())
			}
			iter.Release()*/
			fmt.Fprintf(w, "%v", err)
		} else {
			fmt.Fprintf(w, string(data))
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}

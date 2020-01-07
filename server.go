package main

import (
	// "flag"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

//put kv param Response
type Response struct {
	Success bool
	Message string
}

type Kv struct {
	Key string
	Val string
}

//put kv param Request
type Request struct {
	KvList []Kv
}

type DBManager struct {
	Db *bolt.DB
}

//batch put kv
func (h *DBManager) BatchPut(req Request, res *Response) (err error) {
	if req.KvList == nil || len(req.KvList) <= 0 {
		err = errors.New("A KvList must be specified")
		return
	}

	h.Db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("iich"))
		if b == nil {
			b, _ = tx.CreateBucket([]byte("iich"))
		}
		for _, kv := range req.KvList {

			log.Printf("{%s},{%s}", kv.Key, kv.Val)
			err := b.Put([]byte(kv.Key), []byte(kv.Val))
			if err != nil {
				res.Success = false
				res.Message = "Put Failed"
				fmt.Printf("Put Failed:%v", err)
				return err
			}
		}
		res.Success = true
		res.Message = "Batch Put Success "
		return nil
	})
	return
}

//search in kv store
var db *bolt.DB

func main() {

	db, err := bolt.Open("kvs/iich.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	go func(db *bolt.DB) {
		// Publish our Handler methods
		rpc.Register(&DBManager{db})
		// Create a TCP listener that will listen on `Port`
		listener, _ := net.Listen("tcp", ":1573")
		// Close the listener whenever we stop
		defer listener.Close()
		// Wait for incoming connections
		rpc.Accept(listener)
	}(db)

	http.HandleFunc("/s", func(w http.ResponseWriter, r *http.Request) {
		keys, ok := r.URL.Query()["wd"]

		if !ok || len(keys[0]) < 1 {
			fmt.Fprintf(w, "Url Param 'wd' is missing")
			return
		}

		key := keys[0]
		log.Println(string(key))
		err := db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("iich"))
			v := b.Get([]byte(key))
			if v != nil && len(v) > 0 {
				fmt.Fprintf(w, "The answer is: %s", v)
			} else {
				fmt.Fprintf(w, "Not exist", v)

			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(w, "not found")
		}

	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}

package main

import (
	"bufio"
	"github.com/Mignet/mapreduce"
	"github.com/huichen/sego"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var segmenter sego.Segmenter
var stopTokens string
var db *leveldb.DB

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	segments := segmenter.Segment([]byte(value))
	words := sego.SegmentsToSlice(segments, true)
	res = make([]mapreduce.KeyValue, len(words))
	for i, word := range words {
		if strings.Index(stopTokens, word) < 0 {
			res[i] = mapreduce.KeyValue{word, document}
		}
	}
	return res
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	var docs string
	var docMap = make(map[string]int)
	for _, val := range values {
		cnt, ok := docMap[val]
		if ok {
			docMap[val] = cnt + 1
		} else {
			docMap[val] = 1
		}
	}
	for k, v := range docMap {
		docs += strconv.Itoa(v) + "," + k + ";"
	}
	return docs
}

// Read lines from file and every line apply to callback
func readLines(path string, callback func(string)) {
	var f *os.File
	var err error
	var rd *bufio.Reader
	var line string

	f, err = os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	rd = bufio.NewReader(f)

	for {
		line, err = rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		callback(line)
	}
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run iich.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run iich.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run iich.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		log.Printf("%s: see usage comments in file\n", os.Args[0])
	} else {
		// 载入词典
		segmenter.LoadDictionary("data/dictionary.txt")
		//载入终止词
		contents, err := ioutil.ReadFile("data/stop_tokens.txt")
		if err != nil {
			panic(err)
		}
		stopTokens = string(contents)
		db, errdb := leveldb.OpenFile("kvs/iich", nil)
		if errdb != nil {
			log.Fatal(errdb)
		}
		defer db.Close()
		if os.Args[1] == "master" {
			var mr *mapreduce.Master
			if os.Args[2] == "sequential" {
				mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
			} else {
				mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
			}
			mr.Wait()
		} else {
			mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
		}

		readLines("mrtmp.iiseq", func(line string) {
			line = strings.TrimRight(line, "\r\n ")
			kv := strings.Split(line, ": ")
			log.Printf("{%s},{%s}", kv[0], kv[1])
			db.Put([]byte(kv[0]), []byte(kv[1]), nil)
		})

		files, _ := filepath.Glob("mrtmp.iiseq*")
		for _, s := range files {
			err := os.Remove(s)
			if err != nil {
				// 删除失败
				log.Println(err)
			}
		}

		log.Println("iich success!")
	}
}

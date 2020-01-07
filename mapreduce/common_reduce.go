package mapreduce

import (
	"encoding/json"
	// "fmt"
	"io"
	"io/ioutil"
	"log"
	// "sort"
	// "strconv"
	"strings"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.

	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var contents []byte
	for i := 0; i < nMap; i++ {

		rpath := reduceName(jobName, i, reduceTask)
		// fmt.Println("reduce:", rpath, jobName, i, reduceTask)
		content, err := ioutil.ReadFile(rpath)
		if err != nil {
			log.Fatal(err)
		}
		contents = append(contents, content...)
	}
	lines := strings.Split(string(contents), "\n")

	fs, bs := CreateFileAndBuf(outFile)
	// fmt.Println("out:", outFile)
	var keys = make(map[string][]string)
	for _, line := range lines {
		kv := new(KeyValue)
		err := json.NewDecoder(strings.NewReader(line)).Decode(&kv)
		if kv.Key != "" || kv.Value != "" {
			keys[kv.Key] = append(keys[kv.Key], kv.Value)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
	}
	enc := json.NewEncoder(bs)
	var s []string
	for key := range keys {
		s = append(s, key)
	}
	/*sort.Slice(s, func(i, j int) bool {
		s1, err1 := strconv.Atoi(s[i])
		s2, err2 := strconv.Atoi(s[j])
		if err1 != nil || err2 != nil {
			return false
		}
		return s1 < s2
	})*/
	for _, k := range s {
		if err := enc.Encode(KeyValue{k, reduceF(k, keys[k])}); err != nil {
			log.Fatalln(err)
		}
	}

	SafeClose(fs, bs)
}

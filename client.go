package main

import (
	"fmt"
	"net/rpc"
)

type Response struct {
	Success bool
	Message string
}
type Kv struct {
	Key string
	Val string
}
type Request struct {
	KvList []Kv
}

const HandlerName = "DBManager.BatchPut"

//仅用于测试kv格式

func main() {

	kv := Kv{Key: "zhangsan", Val: "test"}
	var (
		addr     = "127.0.0.1:1573"
		request  = &Request{[]Kv{kv}}
		response = new(Response)
	)

	// Establish the connection to the adddress of the
	// RPC server
	client, _ := rpc.Dial("tcp", addr)
	defer client.Close()

	// Perform a procedure call (core.HandlerName == Handler.Execute)
	// with the Request as specified and a pointer to a response
	// to have our response back.
	_ = client.Call(HandlerName, request, response)
	if response.Success {
		fmt.Println(response.Message)
	} else {
		fmt.Println("error", response.Message)
	}
}

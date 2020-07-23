package main

import (
	"chord"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

func NewNode(port int) dhtNode {
	var ret chord.Node
	ret.Init(":" + strconv.Itoa(port))
	ret.Server = rpc.NewServer()
	ret.Server.Register(&ret)
	l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Print("Error(4):: Failed to Listen " + ":" + strconv.Itoa(port))
		panic(err)
	}
	go ret.Serve(l)
	return &ret
}

// Todo: implement a struct which implements the interface "dhtNode".

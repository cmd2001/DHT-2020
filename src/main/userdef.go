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
	var ret DHTNode
	ret.Data.Init(":" + strconv.Itoa(port))
	ret.Server = rpc.NewServer()
	err := ret.Server.Register(ret.Data)

	if err != nil {
		fmt.Print("Error(4):: Failed to Register.")
		panic(nil)
	}

	return &ret
}

type DHTNode struct {
	Data   *chord.Node
	Server *rpc.Server
}

func (pos *DHTNode) Run() {
	listen, err := net.Listen("tcp", pos.Data.Ip)
	if err != nil {
		fmt.Println("Error(4):: Failed to Listen.", err)
		return
	}

	pos.Data.Listen = listen
	pos.Data.On = true

	go pos.Server.Accept(listen)
}

func (pos *DHTNode) Create() {
	_ = pos.Data.CreateNetwork()
}

func (pos *DHTNode) Join(ip string) {
	_ = pos.Data.JoinNetwork(ip)
}

func (pos *DHTNode) Quit() {
	if pos.Data.On == false {
		return
	}
	pos.Data.On = false
	_ = pos.Data.Quit()
	err := pos.Data.Listen.Close()
	if err != nil {
		fmt.Println("Error(4):: Failed to Close Listen.", err)
	}
}

func (pos *DHTNode) ForceQuit() {
	if pos.Data.On == false {
		return
	}
	pos.Data.On = false
	err := pos.Data.Listen.Close()
	if err != nil {
		fmt.Println("Error(4):: Failed to Close Listen.", err)
	}
}

func (pos *DHTNode) Ping(addr string) bool {
	return chord.Ping(addr) == nil
}

func (pos *DHTNode) Put(key string, value string) bool {
	return pos.Data.InsertKeyVal(key, value) == nil
}

func (pos *DHTNode) Get(key string) (bool, string) {
	var ret string
	err := pos.Data.QueryVal(key, &ret)
	return err == nil, ret
}

func (pos *DHTNode) Delete(key string) bool {
	return pos.Data.EraseKey(key) == nil
}

// Todo: implement a struct which implements the interface "dhtNode".

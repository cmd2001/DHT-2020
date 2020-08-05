package main

import (
	"fmt"
	"kademlia"
	"net"
	"net/rpc"
	"strconv"
)

func NewNode(port int) dhtNode {
	var ret DHTNode
	ret.Data = new(kademlia.RPCNode)
	ret.Data.Data = new(kademlia.Node)
	ret.Data.Data.Init(":" + strconv.Itoa(port))
	ret.Server = rpc.NewServer()
	err := ret.Server.Register(ret.Data)

	if err != nil {
		fmt.Print("Error(4):: Failed to Register.")
		panic(nil)
	}
	return &ret
}

type DHTNode struct {
	Data   *kademlia.RPCNode
	Server *rpc.Server
}

func (pos *DHTNode) Run() {
	listen, err := net.Listen("tcp", pos.Data.Data.Ip)
	if err != nil {
		fmt.Println("Error(4):: Failed to Listen.", err)
		return
	}

	pos.Data.Listen = listen
	pos.Data.Data.On = true

	go pos.Server.Accept(listen)
	go pos.Data.Data.Maintain()
}

func (pos *DHTNode) Create() { // nothing to do
}

func (pos *DHTNode) Join(ip string) bool {
	return pos.Data.Data.Join(ip) == nil
}

func (pos *DHTNode) Quit() { // always do ForceQuit
	pos.ForceQuit()
}

func (pos *DHTNode) ForceQuit() {
	if pos.Data.Data.On == false {
		return
	}
	pos.Data.Data.On = false
	err := pos.Data.Listen.Close()
	if err != nil {
		fmt.Println("Error(4):: Failed to Close Listen.", err)
	}
}

func (pos *DHTNode) Ping(addr string) bool {
	return pos.Data.Data.Ping(addr)
}

func (pos *DHTNode) Put(key string, value string) bool {
	pos.Data.Data.Store(kademlia.KV{Key: key, Value: value})
	return true
}

func (pos *DHTNode) Get(key string) (bool, string) {
	return pos.Data.Data.Query(key)
}

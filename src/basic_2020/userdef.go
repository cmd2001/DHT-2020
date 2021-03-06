package main

import (
	"chord"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
)

func NewNode(port int) dhtNode {
	var ret DHTNode
	ret.Data = new(chord.RPCNode)
	ret.Data.Data = new(chord.Node)
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
	Data   *chord.RPCNode
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

func (pos *DHTNode) Create() {
	_ = pos.Data.Data.CreateNetwork()
}

func (pos *DHTNode) Join(ip string) bool {
	return pos.Data.Data.JoinNetwork(ip) == nil
}

func (pos *DHTNode) Quit() {
	if pos.Data.Data.On == false {
		return
	}
	pos.Data.Data.On = false
	_ = pos.Data.Data.Quit()
	err := pos.Data.Listen.Close()
	if err != nil {
		fmt.Println("Error(4):: Failed to Close Listen.", err)
	}
}

func (pos *DHTNode) ForceQuit() {
	if pos.Data.Data.On == false {
		return
	}
	pos.Data.Data.On = false
	// pos.Data.Data.PrintLink()
	err := pos.Data.Listen.Close()
	if err != nil {
		fmt.Println("Error(4):: Failed to Close Listen.", err)
	}
}

func (pos *DHTNode) Ping(addr string) bool {
	return chord.Ping(addr) == nil
}

func (pos *DHTNode) Put(key string, value string) bool {
	return pos.Data.Data.InsertKeyVal(key, value) == nil
}

func (pos *DHTNode) Get(key string) (bool, string) {
	var ret string
	err := pos.Data.Data.QueryVal(key, &ret)
	return err == nil, ret
}

func (pos *DHTNode) Delete(key string) bool {
	return pos.Data.Data.EraseKey(key) == nil
}

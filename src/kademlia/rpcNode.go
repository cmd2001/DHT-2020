package kademlia

import (
	"math/big"
	"net"
)

type RPCNode struct {
	Data *Node

	Listen net.Listener
}

type PingArgument struct {
	Initiator Edge
}

func (pos *RPCNode) Ping(arg *PingArgument, _ *int) error {
	pos.Data.route[DiffBit(&pos.Data.Id, &arg.Initiator.Id)].push(arg.Initiator)
	return nil
}

type StoreArgument struct {
	Data      KV
	Initiator Edge
}

func (pos *RPCNode) Store(arg *StoreArgument, _ *int) error {
	pos.Data.data.insert(arg.Data)
	pos.Data.route[DiffBit(&pos.Data.Id, &arg.Initiator.Id)].push(arg.Initiator)
	return nil
}

type FindNodeArgument struct {
	Id        big.Int
	Initiator Edge
}

func (pos *RPCNode) FindNode(arg *FindNodeArgument, ret *RetBucket) error {
	*ret = pos.Data.FindNode(&arg.Id)
	pos.Data.route[DiffBit(&pos.Data.Id, &arg.Initiator.Id)].push(arg.Initiator)
	return nil
}

type FindValueArgument struct {
	Key       string
	Id        big.Int
	Initiator Edge
}

func (pos *RPCNode) FindValue(arg *FindValueArgument, ret *RetBucketValue) error {
	*ret = pos.Data.FindValue(&arg.Id, arg.Key)
	pos.Data.route[DiffBit(&pos.Data.Id, &arg.Initiator.Id)].push(arg.Initiator)
	return nil
}

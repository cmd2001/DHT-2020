package kademlia

import (
	"errors"
	"fmt"
	"math/big"
)

// method and struct to be called by RPCNode

type RetBucket struct {
	Data [BucketSize]Edge
}

func (pos *RetBucket) push(x Edge) int {
	for i := 0; i < BucketSize; i++ {
		if pos.Data[i].Ip == x.Ip {
			return 0
		} else if pos.Data[i].Ip == "" {
			pos.Data[i] = x
			return 1
		}
	}
	return 0
}

type RetBucketValue struct {
	Bucket RetBucket
	Val    string
	Flag   bool
}

func (pos *Node) FindNode(id *big.Int, tarSiz int) RetBucket {
	usedSize := 0
	var ret RetBucket
	for i := DiffBit(id, &pos.Id); i < BitLen && usedSize < tarSiz; i++ {
		for j := 0; j < BucketSize && usedSize < tarSiz; j++ {
			usedSize += ret.push(pos.route[i].Data[j])
		}
	}
	return ret
}

func (pos *Node) FindValue(id *big.Int, Key string) RetBucketValue {
	var ret RetBucketValue
	if pos.data.hasKey(Key) {
		ret.Flag = true
		ret.Val = pos.data.query(Key)
	} else {
		ret.Flag = false
		ret.Bucket = pos.FindNode(id, RetBucketSize)
	}
	return ret
}

func (pos *Node) Init(ip string) {
	pos.Ip = ip
	pos.Id = *hashStr(ip) // generate id in a "random" way initialized by ip
}

func (pos *Node) Join(ip string) error {
	if !pos.Ping(ip) {
		fmt.Println("Error(-1):: Ping Error.")
		return errors.New("error(-1):: Ping Error")
	}
	pos.route[DiffBit(&pos.Id, hashStr(ip))].push(Edge{ip, *hashStr(ip)}, Edge{pos.Ip, pos.Id})

	nodes := pos.NearestNode(&pos.Id)

	for i := 0; i < BucketSize; i++ {
		if nodes.Data[i].Ip != "" {
			pos.route[DiffBit(&pos.Id, &nodes.Data[i].Id)].push(nodes.Data[i], Edge{pos.Ip, pos.Id})
		}
	}

	return nil
}

func (pos *Node) Ping(ip string) bool {
	return Ping(Edge{pos.Ip, pos.Id}, ip)
}

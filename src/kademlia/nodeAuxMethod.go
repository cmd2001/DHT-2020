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

type RetBucketSmall struct {
	Data [RetBucketSize]Edge
}

func (pos *RetBucketSmall) push(x Edge) int {
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
	Bucket RetBucketSmall
	Val    string
	Flag   bool
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (pos *Node) FindNode(id *big.Int) RetBucketSmall {
	usedSize := 0
	var ret RetBucketSmall
	init := max(DiffBit(id, &pos.Id), 0)
	for i := init; i < BitLen && usedSize < RetBucketSize; i++ {
		pos.route[i].lock.Lock()
		for j := 0; j < BucketSize && usedSize < RetBucketSize; j++ {
			if pos.route[i].Data[j].Ip != "" && pos.Ping(pos.route[i].Data[j].Ip) {
				usedSize += ret.push(pos.route[i].Data[j])
			}
		}
		pos.route[i].lock.Unlock()
	}
	for i := init - 1; i > 0 && usedSize < RetBucketSize; i-- {
		pos.route[i].lock.Lock()
		for j := 0; j < BucketSize && usedSize < RetBucketSize; j++ {
			if pos.route[i].Data[j].Ip != "" && pos.Ping(pos.route[i].Data[j].Ip) {
				usedSize += ret.push(pos.route[i].Data[j])
			}
		}
		pos.route[i].lock.Unlock()
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
		ret.Bucket = pos.FindNode(id)
	}
	return ret
}

func (pos *Node) Init(ip string) {
	pos.Ip = ip
	pos.Id = *hashStr(ip) // generate id in a "random" way initialized by ip
	pos.data.data = make(map[string]string)
	pos.data.rem = make(map[string]int)
}

func (pos *Node) Join(ip string) error {
	if !pos.Ping(ip) {
		fmt.Println("Error(-1):: Ping Error.")
		return errors.New("error(-1):: Ping Error")
	}
	pos.route[DiffBit(&pos.Id, hashStr(ip))].push(Edge{ip, *hashStr(ip)}, Edge{pos.Ip, pos.Id})

	nodes := pos.NearestNode(&pos.Id)

	for i := 0; i < BucketSize; i++ {
		if nodes.Data[i].Ip != "" && nodes.Data[i].Ip != pos.Ip {
			pos.route[DiffBit(&pos.Id, &nodes.Data[i].Id)].push(nodes.Data[i], Edge{pos.Ip, pos.Id})
		}
	}

	return nil
}

func (pos *Node) Ping(ip string) bool {
	return Ping(Edge{pos.Ip, pos.Id}, ip)
}

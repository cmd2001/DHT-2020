package kademlia

import "math/big"

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

func (pos *Node) FindNode(id *big.Int) RetBucket {
	usedSize := 0
	var ret RetBucket
	for i := DiffBit(id, &pos.Id); i < BitLen && usedSize < BucketSize; i++ {
		for j := 0; j < BucketSize && usedSize < BucketSize; j++ {
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
		ret.Bucket = pos.FindNode(id)
	}
	return ret
}

func (pos *Node) Store(kv KV) {
	pos.data.insert(kv)
}

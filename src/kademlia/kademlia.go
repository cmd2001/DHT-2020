package kademlia

import (
	"math/big"
	"sync"
)

type KV struct {
	Key   string
	Value string
}

type DataPool struct {
	data map[string]string
	lock sync.Mutex
}

func (pos *DataPool) hasKey(key string) bool {
	pos.lock.Lock()
	_, ok := pos.data[key]
	pos.lock.Unlock()
	return ok
}

func (pos *DataPool) query(key string) (string, bool) {
	pos.lock.Lock()
	ret, ok := pos.data[key]
	pos.lock.Unlock()
	return ret, ok
}

func (pos *DataPool) insert(dat KV) {
	pos.lock.Lock()
	pos.data[dat.Key] = dat.Value
	pos.lock.Unlock()
}

type Edge struct {
	Ip string
	Id big.Int
}

type Bucket struct {
	Data [BucketSize]Edge
	lock sync.Mutex
}

func (pos *Bucket) push(x Edge) {
	pos.lock.Lock()
	foundPos := -1
	for i := 0; i < BucketSize; i++ {
		if pos.Data[i].Ip == x.Ip {
			foundPos = i
		}
	}
	if foundPos != -1 {
		for i := foundPos; i < BucketSize-1; i++ {
			pos.Data[i] = pos.Data[i+1]
		}
		pos.Data[BucketSize-1].Ip = ""
		for i := 0; i < BucketSize; i++ {
			if pos.Data[i].Ip == "" {
				pos.Data[i] = x
				break
			}
		}
	} else {
		if Ping(pos.Data[0].Ip) {
			x = pos.Data[0]
		}
		for i := 0; i < BucketSize-1; i++ {
			pos.Data[i] = pos.Data[i+1]
		}
		pos.Data[BucketSize-1].Ip = ""
		for i := 0; i < BucketSize; i++ {
			if pos.Data[i].Ip == "" {
				pos.Data[i] = x
				break
			}
		}
	}
	pos.lock.Unlock()
}

type Node struct {
	route [BitLen]Bucket
}

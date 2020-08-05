package kademlia

import (
	"fmt"
	"math/big"
	"sync"
)

type KV struct {
	Key   string
	Value string
}

type DataPool struct { // the lifetime of data is INFINITY.
	data map[string]string
	lock sync.Mutex
}

func (pos *DataPool) hasKey(key string) bool {
	pos.lock.Lock()
	_, ok := pos.data[key]
	pos.lock.Unlock()
	return ok
}

func (pos *DataPool) query(key string) string {
	pos.lock.Lock()
	ret := pos.data[key]
	pos.lock.Unlock()
	return ret
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

func (pos *Bucket) push(x Edge, cur Edge) bool {
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
		pos.lock.Unlock()
		return false
	} else {
		ret := true
		if Ping(cur, pos.Data[0].Ip) {
			x = pos.Data[0]
			ret = false
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
		pos.lock.Unlock()
		return ret
	}
}

type Node struct {
	route [BitLen]Bucket
	data  DataPool

	Ip string
	Id big.Int

	On bool
}

func (pos *Node) pushNode(edge Edge) {
	if pos.Ip == edge.Ip {
		return
	}
	if pos.route[DiffBit(&pos.Id, &edge.Id)].push(edge, Edge{pos.Ip, pos.Id}) {
		pos.data.lock.Lock()
		dat := pos.data.data
		pos.data.lock.Unlock()
		for key, value := range dat {
			h := hashStr(key)
			if DiffBit(h, &pos.Id) > DiffBit(h, &edge.Id) {
				client := Dial(edge.Ip)
				if client == nil {
					fmt.Println("Error(1):: Dial Connect Failure.")
					continue
				}
				err := client.Call("RPCNode.Store", &StoreArgument{KV{key, value}, Edge{pos.Ip, pos.Id}}, nil)
				_ = client.Close()
				if err != nil {
					fmt.Println("Error(2):: RPC Calling Failure.")
					fmt.Println(err)
				}
			}
		}
	}
}

func (pos *Node) NearestNode(id *big.Int) RetBucket { // alpha = 3, however, the RPC call is synchronous.
	ret := pos.FindNode(id, RetBucketSize)
	vis := make(map[string]struct{})
	for true {
		flag := false
		for i := 0; i < BucketSize; i++ {
			if _, ok := vis[ret.Data[i].Ip]; ret.Data[i].Ip != "" && !ok {
				vis[ret.Data[i].Ip] = struct{}{}
				client := Dial(ret.Data[i].Ip)
				if client == nil {
					fmt.Println("Error(1):: Dial Connect Failure.")
					fmt.Println("ret.ip = ", ret.Data[i].Ip)
				} else {
					var temp RetBucket

					err := client.Call("RPCNode.FindNode", &FindNodeArgument{*id, Edge{pos.Ip, pos.Id}}, &temp)
					_ = client.Close()

					if err != nil {
						fmt.Println("Error(2):: RPC Calling Failure.")
						fmt.Println(err)
					} else {
						ret = Merge(&ret, &temp, id)
					}
				}
				flag = true
				break
			}
		}
		if !flag {
			break
		}
	}
	return ret
}

func (pos *Node) Store(kv KV) {
	nodes := pos.NearestNode(hashStr(kv.Key))
	for i := 0; i < BucketSize; i++ {
		if nodes.Data[i].Ip != "" {
			client := Dial(nodes.Data[i].Ip)

			if client == nil {
				fmt.Println("Error(1):: Dial Connect Failure.")
				continue
			}

			err := client.Call("RPCNode.Store", &StoreArgument{kv, Edge{pos.Ip, pos.Id}}, nil)
			_ = client.Close()

			if err != nil {
				fmt.Println("Error(2):: RPC Calling Failure.")
				fmt.Println(err)
			}
		}
	}
}

func (pos *Node) Query(key string) (bool, string) {
	id := hashStr(key)
	ret := pos.FindNode(id, RetBucketSize) // use BucketSize instead of RetBucketSize for higher success rate
	vis := make(map[string]struct{})
	for true {
		flag := false
		for i := 0; i < BucketSize; i++ {
			if _, ok := vis[ret.Data[i].Ip]; ret.Data[i].Ip != " " && !ok {
				vis[ret.Data[i].Ip] = struct{}{}

				client := Dial(ret.Data[i].Ip)
				if client == nil {
					fmt.Println("Error(1):: Dial Connect Failure.")
				} else {
					var temp RetBucketValue
					err := client.Call("RPCNode.FindValue", &FindValueArgument{key, *id, Edge{pos.Ip, pos.Id}}, &temp)
					_ = client.Close()

					if err != nil {
						fmt.Println("Error(2):: RPC Calling Failure.")
						fmt.Println(err)
					} else {
						if temp.Flag {
							pos.data.insert(KV{key, temp.Val})
							return true, temp.Val
						} else {
							ret = Merge(&ret, &temp.Bucket, id)
						}
					}
				}
				flag = true
				break
			}
		}
		if !flag {
			break
		}
	}
	return false, ""
}

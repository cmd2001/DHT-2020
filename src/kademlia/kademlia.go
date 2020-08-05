package kademlia

import (
	"fmt"
	"math/big"
	"sync"
	"time"
)

type KV struct {
	Key   string
	Value string
}

type DataPool struct { // the lifetime of data is INFINITY.
	data map[string]string
	rem  map[string]int
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
	pos.rem[dat.Key] = 3 // instead of 1 hour, we set it to 3 maintain duration
	pos.lock.Unlock()
}

func (pos *DataPool) rePublish(dat KV) {
	pos.lock.Lock()
	pos.data[dat.Key] = dat.Value
	pos.rem[dat.Key] = 5
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

func (pos *Node) moveData(edge Edge) {
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

func (pos *Node) pushNode(edge Edge) {
	if pos.Ip == edge.Ip {
		return
	}
	if pos.route[DiffBit(&pos.Id, &edge.Id)].push(edge, Edge{pos.Ip, pos.Id}) {
		pos.moveData(edge)
	}
}

func (pos *Node) NearestNode(id *big.Int) RetBucket { // alpha = 3, however, the RPC call is synchronous.
	var ret RetBucket
	temp := pos.FindNode(id)
	for i := 0; i < RetBucketSize; i++ {
		ret.Data[i] = temp.Data[i]
	}
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
					var temp RetBucketSmall

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
	var ret RetBucket
	temp := pos.FindNode(id)
	for i := 0; i < RetBucketSize; i++ {
		ret.Data[i] = temp.Data[i]
	}
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
							for j := 0; j < i; j++ { // cache
								if ret.Data[j].Ip != "" && pos.Ping(ret.Data[j].Ip) {
									client2 := Dial(ret.Data[j].Ip)
									if client2 == nil {
										fmt.Println("Error(1):: Dial Connect Failure.")
									} else {
										err2 := client2.Call("RPCNode.Store", &StoreArgument{KV{key, temp.Val}, Edge{pos.Ip, pos.Id}}, nil)
										_ = client2.Close()
										if err2 != nil {
											fmt.Println("Error(2):: RPC Calling Failure.")
											fmt.Println(err)
										}
									}
								}
							}
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

func (pos *Node) Maintain() {
	for pos.On {
		// deep copy maps
		pos.data.lock.Lock()
		rem := make(map[string]int)
		for key, value := range pos.data.rem {
			rem[key] = value
		}

		dat := make(map[string]string)
		for key, value := range pos.data.data {
			dat[key] = value

		}
		pos.data.lock.Unlock()

		for key, value := range rem {
			if value == 1 {
				nodes := pos.NearestNode(hashStr(key))
				for j := 0; j < BucketSize; j++ {
					if nodes.Data[j].Ip != "" && nodes.Data[j].Ip != pos.Ip && pos.Ping(nodes.Data[j].Ip) {
						client := Dial(nodes.Data[j].Ip)
						if client == nil {
							fmt.Println("Error(1):: Dial Connect Failure.")
							continue
						}
						err := client.Call("RPCNode.RePublish", &StoreArgument{KV{key, dat[key]}, Edge{pos.Ip, pos.Id}}, nil)
						_ = client.Close()
						if err != nil {
							fmt.Println("Error(2):: RPC Calling Failure.")
							fmt.Println(err)
						}
					}
				}
			}
		}

		pos.data.lock.Lock()
		nv := make(map[string]int)
		for key, value := range rem {
			if value != 1 {
				nv[key] = max(value-1, pos.data.rem[key])
			} else {
				nv[key] = 5
			}
		}
		for key, value := range nv {
			pos.data.rem[key] = value
		}

		pos.data.lock.Unlock()

		time.Sleep(maintainDuration)
	}
}

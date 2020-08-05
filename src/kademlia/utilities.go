package kademlia

import (
	"crypto/sha1"
	"math/big"
	"net/rpc"
	"time"
)

const (
	tryTime       = 5
	waitTime      = time.Millisecond * 50
	BitLen        = 160
	BucketSize    = 20
	RetBucketSize = 3
)

var (
	two = big.NewInt(2)
)

func hashStr(s string) *big.Int {
	h := sha1.New()
	h.Write([]byte(s))
	return new(big.Int).SetBytes(h.Sum(nil))
}

func Dial(ip string) *rpc.Client {
	var err error
	var client *rpc.Client
	for i := 1; i <= tryTime; i++ {
		client, err = rpc.Dial("tcp", ip)
		if err != nil {
			time.Sleep(waitTime)
		} else {
			return client
		}
	}
	return nil
}

func Ping(Initiator Edge, ip string) bool {
	var err error
	var client *rpc.Client
	for i := 1; i <= tryTime; i++ {
		client, err = rpc.Dial("tcp", ip)
		if err != nil {
			time.Sleep(waitTime)
		} else {
			_ = client.Call("RPCNode.Ping", &Initiator, nil)
			_ = client.Close()
			return true
		}
	}
	return false
}

func Xor(a *big.Int, b *big.Int) *big.Int {
	ret := new(big.Int)
	ret.Xor(a, b)
	return ret
}

func HighBit(x *big.Int) int {
	return x.BitLen() - 1
}

func DiffBit(x *big.Int, y *big.Int) int {
	return HighBit(Xor(x, y))
}

func Merge(a *RetBucket, b *RetBucket, id *big.Int) RetBucket {
	var ret RetBucket
	pos := 0
	pa := 0
	pb := 0
	for pos < BucketSize && pa < BucketSize && pb < BucketSize {
		if a.Data[pa].Ip == "" && b.Data[pb].Ip == "" {
			break
		} else if b.Data[pb].Ip == "" {
			ret.Data[pos] = a.Data[pa]
			pos++
			pa++
		} else if a.Data[pa].Ip == "" {
			ret.Data[pos] = b.Data[pb]
			pos++
			pb++
		} else if DiffBit(&a.Data[pa].Id, id) < DiffBit(&b.Data[pa].Id, id) {
			ret.Data[pos] = a.Data[pa]
			pos++
			pa++
		} else {
			ret.Data[pos] = b.Data[pb]
			pos++
			pb++
		}
	}
	return ret
}

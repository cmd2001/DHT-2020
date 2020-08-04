package kademlia

import (
	"crypto/sha1"
	"math/big"
	"net/rpc"
	"time"
)

const (
	tryTime    = 5
	waitTime   = time.Millisecond * 50
	BitLen     = 160
	BucketSize = 20
	CacheSize  = 100
)

var (
	two = big.NewInt(2)
	mod = new(big.Int).Exp(two, big.NewInt(160), nil)
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

func Ping(ip string) bool {
	var err error
	var client *rpc.Client
	for i := 1; i <= tryTime; i++ {
		client, err = rpc.Dial("tcp", ip)
		if err != nil {
			time.Sleep(waitTime)
		} else {
			_ = client.Close()
			return false
		}
	}
	return true
}

func Xor(a *big.Int, b *big.Int) *big.Int {
	ret := new(big.Int)
	ret.Xor(a, b)
	return ret
}

func HighBit(x *big.Int) int {
	temp := big.NewInt(int64(1))
	Zero := big.NewInt(int64(0))
	cmp := new(big.Int)
	ret := -1
	for i := 0; i < BitLen; i++ {
		cmp.Add(temp, x)
		if cmp.Cmp(Zero) > 0 {
			ret = i
		}
		temp.Mul(temp, two)
	}
	return ret
}

func DiffBit(x *big.Int, y *big.Int) int {
	return HighBit(Xor(x, y))
}

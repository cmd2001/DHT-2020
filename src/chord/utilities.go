package chord

import (
	"crypto/sha1"
	"math/big"
	"net/rpc"
	"time"
)

const (
	tryTime = 5
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

func Dial(s string) *rpc.Client {
	var err error
	var client *rpc.Client
	for i := 1; i <= tryTime; i++ {
		client, err = rpc.Dial("tcp", s)
		if err != nil {
			time.Sleep(Second / 2)
		} else {
			return client
		}
	}
	return nil
}

func powTwo(p int64) *big.Int {
	return new(big.Int).Exp(two, big.NewInt(p), nil)
}

func Dis(tar *big.Int, pos *big.Int) *big.Int {
	temp := new(big.Int)
	return temp.Mod(temp.Add(temp.Sub(tar, pos), mod), mod)
}

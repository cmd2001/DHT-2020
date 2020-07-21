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

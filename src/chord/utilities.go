package chord

import (
	"crypto/sha1"
	"errors"
	"math/big"
	"net/rpc"
	"time"
)

const (
	tryTime  = 5
	waitTime = time.Millisecond * 50
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

// return rpc.Client to a given IP, nil when failed.
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

func Ping(ip string) error {
	var err error
	var client *rpc.Client
	for i := 1; i <= tryTime; i++ {
		client, err = rpc.Dial("tcp", ip)
		if err != nil {
			time.Sleep(waitTime)
		} else {
			_ = client.Close()
			return nil
		}
	}
	return errors.New("Error(1):: Ping Failure.")
}

func powTwo(p int64) *big.Int {
	return new(big.Int).Exp(two, big.NewInt(p), nil)
}

func inRange(l *big.Int, r *big.Int, x *big.Int) bool {
	var rr big.Int
	if l.Cmp(r) >= 0 {
		rr.Add(r, mod)
	} else {
		rr = *r
	}

	if l.Cmp(x) < 0 && x.Cmp(&rr) <= 0 {
		return true
	}
	var adder big.Int
	adder.Add(x, mod)
	if l.Cmp(&adder) < 0 && adder.Cmp(&rr) <= 0 {
		return true
	}
	return false
}

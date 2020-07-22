package chord

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"
)

const (
	Len    = 160
	Second = 1000 * time.Microsecond
)

type Edge struct {
	ip  string
	val big.Int
}

type Storage struct {
	data map[string]string
	lock sync.Mutex // for multithreading
}

type Node struct {
	finger [Len]Edge
	pre    Edge
	suc    Edge
	lock   sync.Mutex

	sto    Storage
	stoPre Storage

	pos   big.Int
	ip    string
	state bool // 1 for on
}

func (pos *Node) queryInside(tar string) string {
	var value string
	var ok bool

	pos.sto.lock.Lock()
	value, ok = pos.sto.data[tar]
	pos.sto.lock.Unlock()

	if ok {
		return value
	} else {
		fmt.Print("Error(0):: Value Not Found.")
		return ""
	}
}

func (pos *Node) findNearestSuc(val *big.Int) Edge {
	for i := Len - 1; i > 0; i-- {
		temp := new(big.Int)
		if Dis(val, &pos.pos).Cmp(Dis(val, temp.Add(&pos.pos, powTwo(int64(i))))) < 0 {
			pos.lock.Lock()
			var ret = pos.finger[i]
			pos.lock.Unlock()
			return ret
		}
	}
	return Edge{pos.ip, pos.pos}
}

// return node ip for a query key.
func (pos *Node) findNode(ret *string, tar string) error {
	nxt := pos.findNearestSuc(hashStr(tar))
	if nxt.ip == pos.ip {
		*ret = pos.ip
		return nil
	} else {
		client := Dial(nxt.ip)
		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.")
			*ret = ""
			return errors.New("Error(1):: Dial Connect Failure.")
		} else {
			err := client.Call("Node.findNode", nxt, tar)
			if err != nil {
				fmt.Print("Error(2):: RPC Calling Failure.")
				*ret = ""
				return errors.New("Error(2):: RPC Calling Failure.")
			}
		}
	}
	return nil
}

func (pos *Node) queryVal(ret *string, tar string) error {
	var ip string
	err := pos.findNode(&ip, tar)
	if err != nil {
		fmt.Print("Error(3):: Query Failure.")
		return errors.New("Error(0):: Value Not Found.")
	}
	client := Dial(ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		*ret = ""
		return errors.New("Error(1):: Dial Connect Failure.")
	}
	err = client.Call("Node.queryInside", ret, tar)
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.")
		*ret = ""
		return errors.New("Error(2):: RPC Calling Failure.")
	}
	return nil
}

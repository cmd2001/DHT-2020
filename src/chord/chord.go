package chord

import (
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"
)

const (
	Len    = 160
	Second = 1000 * time.Microsecond
)

type Edge struct {
	ip string
	id big.Int
}

type Storage struct {
	data map[string]string
	lock sync.Mutex // for multithreading
}

type Node struct {
	finger [Len]Edge
	pre    Edge
	suc    Edge
	fixing int
	lock   sync.Mutex

	sto    Storage
	stoPre Storage

	ip    string
	id    big.Int
	state bool // 1 for on

	Listen net.Listener
}

func (pos *Node) GetID(arg int, ret *big.Int) error {
	*ret = pos.id
	return nil
}

func (pos *Node) ClosestPrecedingNode(id *big.Int) Edge {
	for i := Len - 1; i >= 0; i-- {
		pos.lock.Lock()
		if inRange(&pos.id, id, &pos.finger[i].id) {
			ret := pos.finger[i]
			pos.lock.Unlock()
			if Ping(ret.ip) == nil {
				return ret
			}
		}
		pos.lock.Unlock()
	}
	return Edge{pos.ip, pos.id}
}

// return node ip for a query key.
func (pos *Node) FindSuccessor(h big.Int, ret *Edge) error {
	client := Dial(pos.suc.ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}
	var sucID big.Int
	err := client.Call("Node.GetID", 0, &sucID)
	client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	if inRange(&pos.id, &sucID, &h) {
		*ret = pos.suc
		return nil
	}

	nxt := pos.ClosestPrecedingNode(&h)
	if nxt.ip == pos.ip { // failed
		fmt.Print("Error(3):: Unable to Find Successor")
		return errors.New("Error(3):: Unable to Find Successor")
	}

	client = Dial(nxt.ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	err = client.Call("Node.FindSuccessor", h, ret)
	client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	return nil
}

func (pos *Node) QueryInside(key string) string {
	var value string
	var ok bool

	pos.sto.lock.Lock()
	value, ok = pos.sto.data[key]
	pos.sto.lock.Unlock()

	if ok {
		return value
	} else {
		fmt.Print("Error(0):: Value Not Found.")
		return ""
	}
}

func (pos *Node) QueryVal(key string, ret *string) error {
	var temp Edge
	err := pos.FindSuccessor(*hashStr(key), &temp) // fail when findNode returns node which does not store key.
	if err != nil {
		fmt.Print("Error(3):: Unable to Find Node for query")
		return errors.New("Error(3):: Unable to Find Node for query")
	}

	ip := temp.ip
	client := Dial(ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		*ret = ""
		return errors.New("Error(1):: Dial Connect Failure.")
	}
	err = client.Call("Node.QueryInside", key, ret)
	client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.")
		*ret = ""
		return errors.New("Error(2):: RPC Calling Failure.")
	}
	return nil
}

type KeyValue struct {
	key string
	val string
}

func (pos *Node) InsertInside(kv KeyValue, ret *string) error {
	pos.sto.lock.Lock()
	pos.sto.data[kv.key] = kv.val
	pos.sto.lock.Unlock()
	return nil
}

func (pos *Node) InsertKeyVal(key string, val string) error {
	var temp Edge
	err := pos.FindSuccessor(*hashStr(key), &temp)
	if err != nil {
		fmt.Print("Error(3):: Unable to Find Node for Insert")
		return errors.New("Error(3):: Unable to Find Node for Insert")
	}

	ip := temp.ip
	client := Dial(ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}
	err = client.Call("Node.InsertInside", KeyValue{key, val}, nil)
	client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}
	return nil
}

func (pos *Node) Create() error { // todo:: initialize fingers
	pos.suc = Edge{pos.ip, pos.id}
	return nil
}

func (pos *Node) Join(ip string) error { // todo:: initialize fingers
	client := Dial(ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	var ret Edge
	err := client.Call("Node.FindSuccessor", pos.id, &ret)
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	pos.suc = ret
	return nil
}

func (pos *Node) GetPredecessor(arg int, ret *Edge) error {
	*ret = pos.pre
	return nil
}

func (pos *Node) Notify(x Edge, ret *int) error {
	if pos.pre.ip == "" {
		pos.pre = x
	} else {
		client := Dial(pos.suc.ip)
		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.")
			return errors.New("Error(1):: Dial Connect Failure.")
		}

		var sucID big.Int
		err := client.Call("Node.GetID", 0, &sucID)
		client.Close()
		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.")
			return errors.New("Error(2):: RPC Calling Failure.")
		}

		if inRange(&pos.id, &sucID, &x.id) {
			pos.suc = x
		}

	}
	return nil
}

func (pos *Node) Stabilize() error {
	client := Dial(pos.suc.ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	var ret Edge
	err := client.Call("Node.GetPredecessor", 0, &ret)
	if err != nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	if ret.ip == "" { // nil pre
		return nil
	}

	var sucID big.Int
	err = client.Call("Node.GetID", 0, &sucID)
	client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	if inRange(&pos.id, &sucID, &ret.id) {
		pos.suc = ret
	}

	client = Dial(pos.suc.ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	err = client.Call("Node.Notify", Edge{pos.ip, pos.id}, nil)
	client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	return nil
}

func (pos *Node) FixFingers() error {
	pos.fixing = pos.fixing + 1
	var adder big.Int
	var ret Edge

	err := pos.FindSuccessor(*adder.Add(&pos.id, powTwo(int64(pos.fixing))), &ret)

	if err != nil {
		fmt.Print("Error(3):: Unable to Find Successor")
		return errors.New("Error(3):: Unable to Find Successor")
	}

	pos.finger[pos.fixing] = ret

	if pos.fixing == Len {
		pos.fixing = 0
	}

	return nil
}

func (pos *Node) CheckPredecessor() error {
	if pos.pre.ip != "" && Ping(pos.pre.ip) != nil {
		pos.pre.ip = ""
	}
	return nil
}

// FIXME: locks

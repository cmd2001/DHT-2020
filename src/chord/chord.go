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

	sto Storage

	Ip string
	id big.Int
	On bool // 1 for on

	Listen net.Listener
}

func (pos *Node) Init(_ip string) {
	pos.Ip = _ip
	pos.id = *hashStr(pos.Ip)
}

func (pos *Node) GetID(_ int, ret *big.Int) error {
	*ret = pos.id
	return nil
}

func (pos *Node) ClosestPrecedingNode(id *big.Int) Edge {
	pos.lock.Lock()
	for i := Len - 1; i >= 0; i-- {
		if inRange(&pos.id, id, &pos.finger[i].id) {
			ret := pos.finger[i]
			pos.lock.Unlock()
			if Ping(ret.ip) == nil {
				return ret
			}
		}
	}
	pos.lock.Unlock()
	return Edge{pos.Ip, pos.id}
}

// return node ip for a query key.
func (pos *Node) FindSuccessor(h big.Int, ret *Edge) error {
	pos.lock.Lock()
	client := Dial(pos.suc.ip)

	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}
	var sucID big.Int
	err := client.Call("Node.GetID", 0, &sucID)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	if inRange(&pos.id, &sucID, &h) {
		*ret = pos.suc
		pos.lock.Unlock()
		return nil
	}

	nxt := pos.ClosestPrecedingNode(&h)
	if nxt.ip == pos.Ip { // failed
		pos.lock.Unlock()
		fmt.Print("Error(3):: Unable to Find Successor")
		return errors.New("error(3):: Unable to Find Successor")
	}

	client = Dial(nxt.ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("Node.FindSuccessor", h, ret)
	_ = client.Close()

	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	return nil
}

func (pos *Node) QueryInside(key string, ret *string) error {
	var value string
	var ok bool

	pos.sto.lock.Lock()
	value, ok = pos.sto.data[key]
	pos.sto.lock.Unlock()

	if ok {
		*ret = value
	} else {
		fmt.Print("Error(0):: Value Not Found.")
		return errors.New("error(0):: Value Not Found")
	}
	return nil
}

func (pos *Node) QueryVal(key string, ret *string) error {
	var temp Edge
	pos.lock.Lock()

	err := pos.FindSuccessor(*hashStr(key), &temp) // fail when findNode returns node which does not store key.
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(3):: Unable to Find Node for query")
		return errors.New("error(3):: Unable to Find Node for query")
	}

	ip := temp.ip
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("Node.QueryInside", key, &ret)
	_ = client.Close()

	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.lock.Unlock()
	return nil
}

type KeyValue struct {
	key string
	val string
}

func (pos *Node) EraseInside(key string, _ *int) error {
	pos.sto.lock.Lock()
	if _, ok := pos.sto.data[key]; !ok {
		fmt.Print("Error(0):: Value Not Found.")
		return errors.New("error(0):: Value Not Found")
	}
	delete(pos.sto.data, key)
	pos.sto.lock.Unlock()
	return nil
}

func (pos *Node) EraseKey(key string) error {
	var temp Edge
	pos.lock.Lock()
	err := pos.FindSuccessor(*hashStr(key), &temp)
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(3):: Unable to Find Node for Insert")
		return errors.New("error(3):: Unable to Find Node for Insert")
	}

	ip := temp.ip
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}
	err = client.Call("Node.EraseInside", key, nil)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) InsertInside(kv KeyValue, _ *int) error {
	pos.sto.lock.Lock()
	pos.sto.data[kv.key] = kv.val
	pos.sto.lock.Unlock()
	return nil
}

func (pos *Node) InsertKeyVal(key string, val string) error {
	var temp Edge
	pos.lock.Lock()
	err := pos.FindSuccessor(*hashStr(key), &temp)
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(3):: Unable to Find Node for Insert")
		return errors.New("error(3):: Unable to Find Node for Insert")
	}

	ip := temp.ip
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}
	err = client.Call("Node.InsertInside", KeyValue{key, val}, nil)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) CreateNetwork() error { // todo:: initialize fingers
	pos.lock.Lock()
	pos.suc = Edge{pos.Ip, pos.id}
	for i := 0; i < Len; i++ {
		pos.finger[i] = Edge{pos.Ip, pos.id}
	}
	pos.lock.Unlock()
	return nil
}

func (pos *Node) MoveDataToPre(pr big.Int, ret *map[string]string) error {
	pos.sto.lock.Lock()
	for k, v := range pos.sto.data {
		if hashStr(k).Cmp(&pr) <= 0 {
			(*ret)[k] = v
		}
	}
	for k := range *ret {
		delete(pos.sto.data, k)
	}
	pos.sto.lock.Unlock()
	return nil
}

func (pos *Node) MoveDataFromPre(dat map[string]string, _ *int) error {
	pos.sto.lock.Lock()

	for k, v := range dat {
		pos.sto.data[k] = v
	}

	pos.sto.lock.Unlock()
	return nil
}

func (pos *Node) JoinNetwork(ip string) error { // todo:: initialize fingers
	pos.lock.Lock()
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}

	var ret Edge
	// fmt.Print("to call\n")
	err := client.Call("Node.FindSuccessor", pos.id, &ret)
	// fmt.Print(err)
	if err != nil {
		_ = client.Close()
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.suc = ret

	pos.sto.lock.Lock()
	err = client.Call("Node.MoveDataFromPre", pos.id, pos.sto.data)
	pos.sto.lock.Lock()
	if err != nil {
		_ = client.Close()
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	_ = client.Close()

	pos.lock.Unlock()
	return nil
}

func (pos *Node) GetPredecessor(_ int, ret *Edge) error {
	pos.lock.Lock()
	*ret = pos.pre
	pos.lock.Unlock()
	return nil
}

func (pos *Node) Notify(x Edge, _ *int) error {
	pos.lock.Lock()

	if pos.pre.ip == "" {
		pos.pre = x
	} else {
		client := Dial(pos.suc.ip)
		if client == nil {
			pos.lock.Unlock()
			fmt.Print("Error(1):: Dial Connect Failure.")
			return errors.New("error(1):: Dial Connect Failure")
		}

		var sucID big.Int
		err := client.Call("Node.GetID", 0, &sucID)
		_ = client.Close()
		if err != nil {
			pos.lock.Unlock()
			fmt.Print("Error(2):: RPC Calling Failure.")
			return errors.New("error(2):: RPC Calling Failure")
		}

		if inRange(&pos.id, &sucID, &x.id) {
			pos.suc = x
		}

	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) Stabilize() error {
	pos.lock.Lock()
	client := Dial(pos.suc.ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}

	var ret Edge
	err := client.Call("Node.GetPredecessor", 0, &ret)
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}

	if ret.ip == "" { // nil pre
		return nil
	}

	var sucID big.Int
	err = client.Call("Node.GetID", 0, &sucID)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	if inRange(&pos.id, &sucID, &ret.id) {
		pos.suc = ret
	}

	client = Dial(pos.suc.ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("Node.Notify", Edge{pos.Ip, pos.id}, nil)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) FixFingers() error {
	pos.lock.Lock()
	pos.fixing = pos.fixing + 1
	var adder big.Int
	var ret Edge

	err := pos.FindSuccessor(*adder.Add(&pos.id, powTwo(int64(pos.fixing))), &ret)

	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(3):: Unable to Find Successor")
		return errors.New("error(3):: Unable to Find Successor")
	}

	pos.finger[pos.fixing] = ret

	if pos.fixing == Len {
		pos.fixing = 0
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) CheckPredecessor() error {
	pos.lock.Lock()
	if pos.pre.ip != "" && Ping(pos.pre.ip) != nil {
		pos.pre.ip = ""
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) Quit() error {
	pos.lock.Lock()
	client := Dial(pos.suc.ip)
	pos.lock.Unlock()

	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}

	pos.sto.lock.Lock()
	err := client.Call("Node.MoveDataFromPre", pos.sto.data, nil)
	pos.sto.lock.Unlock()
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("error(1):: Dial Connect Failure")
	}

	return nil
}

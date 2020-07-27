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
	Ip string
	Id big.Int
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
}

func (pos *Node) Init(_ip string) {
	pos.Ip = _ip
	pos.id = *hashStr(pos.Ip)
	pos.sto.data = make(map[string]string)
}

func (pos *Node) GetID(_ *int, ret *big.Int) error {
	*ret = pos.id
	return nil
}

func (pos *Node) ClosestPrecedingNode(id *big.Int) Edge {
	for i := Len - 1; i >= 0; i-- {
		// fmt.Print(pos.id, *id, pos.finger[i].Id, "\n")
		// fmt.Print(id.Cmp(&pos.finger[i].Id), "\n")
		// fmt.Print(pos.finger[i].Id.Cmp(id), "\n")
		if inRange(&pos.id, id, &pos.finger[i].Id) {
			ret := pos.finger[i]
			if Ping(ret.Ip) == nil {
				return ret
			}
		}
	}
	return Edge{pos.Ip, pos.id}
}

// return node ip for a query key.
func (pos *Node) FindSuccessor(h *big.Int, ret *Edge) error {
	pos.lock.Lock()
	client := Dial(pos.suc.Ip)

	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}
	var sucID big.Int
	err := client.Call("RPCNode.GetID", 0, &sucID)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	// fmt.Print(pos.id.Cmp(h))
	// fmt.Print(h.Cmp(&sucID))
	// fmt.Print(pos.id.Cmp(&sucID), "\n")
	if inRange(&pos.id, &sucID, h) {
		*ret = pos.suc
		pos.lock.Unlock()
		return nil
	}

	nxt := pos.ClosestPrecedingNode(h)
	if nxt.Ip == pos.Ip { // failed
		pos.lock.Unlock()
		// fmt.Print("Same ip")
		fmt.Print("Error(3):: Unable to Find Successor\n")
		return errors.New("error(3):: Unable to Find Successor")
	}

	client = Dial(nxt.Ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("RPCNode.FindSuccessor", &h, ret)
	_ = client.Close()

	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.lock.Unlock()
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
		fmt.Print("Error(0):: Value Not Found.\n")
		return errors.New("error(0):: Value Not Found")
	}
	return nil
}

func (pos *Node) QueryVal(key string, ret *string) error {
	var temp Edge

	err := pos.FindSuccessor(hashStr(key), &temp) // fail when findNode returns node which does not store key.
	fmt.Print(err)
	if err != nil {
		fmt.Print("Error(3):: Unable to Find Node for Query\n")
		return errors.New("error(3):: Unable to Find Node for Query")
	}

	ip := temp.Ip
	client := Dial(ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("RPCNode.QueryInside", key, &ret)
	_ = client.Close()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	return nil
}

type KeyValue struct {
	Key string
	Val string
}

func (pos *Node) EraseInside(key string, _ *int) error {
	pos.sto.lock.Lock()
	if _, ok := pos.sto.data[key]; !ok {
		fmt.Print("Error(0):: Value Not Found.\n")
		return errors.New("error(0):: Value Not Found")
	}
	delete(pos.sto.data, key)
	pos.sto.lock.Unlock()
	return nil
}

func (pos *Node) EraseKey(key string) error {
	var temp Edge
	pos.lock.Lock()
	err := pos.FindSuccessor(hashStr(key), &temp)
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(3):: Unable to Find Node for Insert.\n")
		return errors.New("error(3):: Unable to Find Node for Insert")
	}

	ip := temp.Ip
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}
	err = client.Call("RPCNode.EraseInside", key, nil)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) InsertInside(kv KeyValue, _ *int) error {
	pos.sto.lock.Lock()
	pos.sto.data[kv.Key] = kv.Val
	pos.sto.lock.Unlock()
	return nil
}

func (pos *Node) InsertKeyVal(key string, val string) error {
	var temp Edge
	err := pos.FindSuccessor(hashStr(key), &temp)
	if err != nil {
		fmt.Print("Error(3):: Unable to Find Node for Insert.\n")
		return errors.New("error(3):: Unable to Find Node for Insert")
	}

	ip := temp.Ip
	client := Dial(ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}
	err = client.Call("RPCNode.InsertInside", KeyValue{key, val}, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

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

func (pos *Node) MoveDataToPre(pr *big.Int, ret *map[string]string) error {
	pos.sto.lock.Lock()
	for k, v := range pos.sto.data {
		if hashStr(k).Cmp(pr) <= 0 {
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
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	var ret Edge

	err := client.Call("RPCNode.FindSuccessor", &pos.id, &ret)
	if err != nil {
		_ = client.Close()
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.suc = ret
	pos.finger[0] = pos.suc

	pos.sto.lock.Lock()
	err = client.Call("RPCNode.MoveDataToPre", &pos.id, &pos.sto.data)

	pos.sto.lock.Unlock()
	if err != nil {
		_ = client.Close()
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	_ = client.Close()

	pos.lock.Unlock()
	return nil
}

func (pos *Node) GetPredecessor(_ *int, ret *Edge) error {
	pos.lock.Lock()
	*ret = pos.pre
	pos.lock.Unlock()
	return nil
}

func (pos *Node) Notify(x Edge, _ *int) error {
	pos.lock.Lock()

	if pos.pre.Ip == "" {
		pos.pre = x
	} else {
		client := Dial(pos.suc.Ip)
		if client == nil {
			pos.lock.Unlock()
			fmt.Print("Error(1):: Dial Connect Failure.\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		var sucID big.Int
		err := client.Call("RPCNode.GetID", 0, &sucID)
		_ = client.Close()
		if err != nil {
			pos.lock.Unlock()
			fmt.Print("Error(2):: RPC Calling Failure.\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		if inRange(&pos.id, &sucID, &x.Id) {
			pos.suc = x
		}

	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) Stabilize() error {
	pos.lock.Lock()
	client := Dial(pos.suc.Ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	var ret Edge
	err := client.Call("RPCNode.GetPredecessor", 0, &ret)
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	if ret.Ip == "" { // nil pre
		return nil
	}

	var sucID big.Int
	err = client.Call("RPCNode.GetID", 0, &sucID)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	if inRange(&pos.id, &sucID, &ret.Id) {
		pos.suc = ret
	}

	client = Dial(pos.suc.Ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("RPCNode.Notify", Edge{pos.Ip, pos.id}, nil)
	_ = client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.\n")
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

	err := pos.FindSuccessor(adder.Add(&pos.id, powTwo(int64(pos.fixing))), &ret)

	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(3):: Unable to Find Successor.\n")
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
	if pos.pre.Ip != "" && Ping(pos.pre.Ip) != nil {
		pos.pre.Ip = ""
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) Quit() error {
	pos.lock.Lock()
	client := Dial(pos.suc.Ip)
	pos.lock.Unlock()

	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	pos.sto.lock.Lock()
	err := client.Call("RPCNode.MoveDataFromPre", &pos.sto.data, nil)
	pos.sto.lock.Unlock()
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	return nil
}

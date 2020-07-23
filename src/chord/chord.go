package chord

import (
	"errors"
	"fmt"
	"image"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os/signal"
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

	Server *rpc.Server
	Listen *net.Listener
}

func (pos *Node) Init(_ip string) {
	pos.ip = _ip
}

func (pos *Node) GetID(arg int, ret *big.Int) error {
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
	return Edge{pos.ip, pos.id}
}

// return node ip for a query key.
func (pos *Node) FindSuccessor(h big.Int, ret *Edge) error {
	pos.lock.Lock()
	client := Dial(pos.suc.ip)

	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}
	var sucID big.Int
	err := client.Call("Node.GetID", 0, &sucID)
	client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	if inRange(&pos.id, &sucID, &h) {
		*ret = pos.suc
		pos.lock.Unlock()
		return nil
	}

	nxt := pos.ClosestPrecedingNode(&h)
	if nxt.ip == pos.ip { // failed
		pos.lock.Unlock()
		fmt.Print("Error(3):: Unable to Find Successor")
		return errors.New("Error(3):: Unable to Find Successor")
	}

	client = Dial(nxt.ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	err = client.Call("Node.FindSuccessor", h, ret)
	client.Close()

	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
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
		return errors.New("Error(0):: Value Not Found.")
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
		return errors.New("Error(3):: Unable to Find Node for query")
	}

	ip := temp.ip
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	err = client.Call("Node.QueryInside", key, &ret)
	client.Close()

	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	pos.lock.Unlock()
	return nil
}

type KeyValue struct {
	key string
	val string
}

func (pos *Node) EraseInside(key string, ret *string) error {
	pos.sto.lock.Lock()
	if _, ok := pos.sto.data[key]; !ok {
		fmt.Print("Error(0):: Value Not Found.")
		return errors.New("Error(0):: Value Not Found.")
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
		return errors.New("Error(3):: Unable to Find Node for Insert")
	}

	ip := temp.ip
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}
	err = client.Call("Node.EraseInside", key, nil)
	client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) InsertInside(kv KeyValue, ret *string) error {
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
		return errors.New("Error(3):: Unable to Find Node for Insert")
	}

	ip := temp.ip
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}
	err = client.Call("Node.InsertInside", KeyValue{key, val}, nil)
	client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	pos.lock.Unlock()
	return nil
}

func (pos *Node) CreateNetwork() error { // todo:: initialize fingers
	pos.lock.Lock()
	pos.suc = Edge{pos.ip, pos.id}
	pos.lock.Unlock()
	return nil
}

func (pos *Node) JoinNetwork(ip string) error { // todo:: initialize fingers
	pos.lock.Lock()
	client := Dial(ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	var ret Edge
	err := client.Call("Node.FindSuccessor", pos.id, &ret)
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	pos.suc = ret
	pos.lock.Unlock()
	return nil
}

func (pos *Node) GetPredecessor(arg int, ret *Edge) error {
	pos.lock.Lock()
	*ret = pos.pre
	pos.lock.Unlock()
	return nil
}

func (pos *Node) Notify(x Edge, ret *int) error {
	pos.lock.Lock()

	if pos.pre.ip == "" {
		pos.pre = x
	} else {
		client := Dial(pos.suc.ip)
		if client == nil {
			pos.lock.Unlock()
			fmt.Print("Error(1):: Dial Connect Failure.")
			return errors.New("Error(1):: Dial Connect Failure.")
		}

		var sucID big.Int
		err := client.Call("Node.GetID", 0, &sucID)
		client.Close()
		if err != nil {
			pos.lock.Unlock()
			fmt.Print("Error(2):: RPC Calling Failure.")
			return errors.New("Error(2):: RPC Calling Failure.")
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
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	var ret Edge
	err := client.Call("Node.GetPredecessor", 0, &ret)
	if err != nil {
		pos.lock.Unlock()
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
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
	}

	if inRange(&pos.id, &sucID, &ret.id) {
		pos.suc = ret
	}

	client = Dial(pos.suc.ip)
	if client == nil {
		pos.lock.Unlock()
		fmt.Print("Error(1):: Dial Connect Failure.")
		return errors.New("Error(1):: Dial Connect Failure.")
	}

	err = client.Call("Node.Notify", Edge{pos.ip, pos.id}, nil)
	client.Close()
	if err != nil {
		pos.lock.Unlock()
		fmt.Print("Error(2):: RPC Calling Failure.")
		return errors.New("Error(2):: RPC Calling Failure.")
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
		return errors.New("Error(3):: Unable to Find Successor")
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

func (pos *Node) Run() { // todo: net listening
	pos.state = true
}

func (pos *Node) Create() {
	pos.CreateNetwork()
}

func (pos *Node) Join(ip string) {
	pos.JoinNetwork(ip)
}

func (pos *Node) Quit() { // todo
}

func (pos *Node) ForceQuit() { // todo

}

func (pos *Node) Ping(addr string) bool {
	return Ping(addr) == nil
}

func (pos *Node) Put(key string, value string) bool {
	return pos.InsertKeyVal(key, value) == nil
}

func (pos *Node) Get(key string) (bool, string) {
	var ret string
	err := pos.QueryVal(key, &ret)
	return err == nil, ret
}

func (pos *Node) Delete(key string) bool {
	return pos.EraseKey(key) == nil
}

func (pos *Node) Serve(l net.Listener) {
	http.Serve(l, nil)
}

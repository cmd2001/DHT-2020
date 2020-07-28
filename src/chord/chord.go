package chord

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"
)

const (
	Len            = 160
	SucListLen     = 10
	maintainPeriod = 1 * time.Second
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
	finger  [Len]Edge
	pre     Edge
	sucList [SucListLen]Edge
	fixing  int
	lock    sync.Mutex

	sto Storage

	Ip     string
	id     big.Int
	On     bool // 1 for on
	inited bool
}

func (pos *Node) Init(_ip string) {
	pos.Ip = _ip
	pos.id = *hashStr(pos.Ip)
	pos.sto.data = make(map[string]string)
	pos.inited = false
}

func (pos *Node) GetID(_ *int, ret *big.Int) error {
	*ret = pos.id
	return nil
}

func (pos *Node) ClosestPrecedingNode(id *big.Int) Edge {
	pos.lock.Lock()
	for i := Len - 1; i >= 0; i-- {
		if inRange(&pos.id, id, &pos.finger[i].Id) {
			ret := pos.finger[i]
			if Ping(ret.Ip) == nil {
				pos.lock.Unlock()
				return ret
			}
		}
	}
	pos.lock.Unlock()
	return Edge{pos.Ip, pos.id}
}

// return node ip for a query key.
func (pos *Node) FindSuccessor(h *big.Int, ret *Edge) error {
	if pos.FixList() != nil {
		fmt.Print("Error(5):: All Successor has Failed.\n")
		return errors.New("error(5):: All Successor has Failed")
	}
	pos.lock.Lock()
	client := Dial(pos.sucList[0].Ip)
	pos.lock.Unlock()

	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}
	var sucID big.Int
	err := client.Call("RPCNode.GetID", 0, &sucID)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	if inRange(&pos.id, &sucID, h) {
		*ret = pos.sucList[0]
		return nil
	}

	nxt := pos.ClosestPrecedingNode(h)
	if nxt.Ip == pos.Ip { // failed
		fmt.Print("Error(3):: Unable to Find Successor\n")
		return errors.New("error(3):: Unable to Find Successor")
	}

	client = Dial(nxt.Ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("RPCNode.FindSuccessor", h, ret)
	_ = client.Close()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
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
		// fmt.Print("Error(0):: Value Not Found.\n")
		return errors.New("error(0):: Value Not Found")
	}
	return nil
}

func (pos *Node) QueryVal(key string, ret *string) error {
	var temp Edge

	err := pos.FindSuccessor(hashStr(key), &temp) // fail when findNode returns node which does not store key.
	// fmt.Print(err)
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
		if err.Error() == "error(0):: Value Not Found" {
			return err
		}
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
		pos.sto.lock.Unlock()
		fmt.Print("Error(0):: Value Not Found.\n")
		return errors.New("error(0):: Value Not Found")
	}
	delete(pos.sto.data, key)
	pos.sto.lock.Unlock()
	return nil
}

func (pos *Node) EraseKey(key string) error {
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
	err = client.Call("RPCNode.EraseInside", key, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

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

func (pos *Node) CreateNetwork() error {
	pos.lock.Lock()
	pos.sucList[0] = Edge{pos.Ip, pos.id}
	pos.pre = Edge{pos.Ip, pos.id}
	for i := 0; i < Len; i++ {
		pos.finger[i] = Edge{pos.Ip, pos.id}
	}
	pos.lock.Unlock()

	pos.inited = true
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

func (pos *Node) JoinNetwork(ip string) error {
	pos.lock.Lock()
	client := Dial(ip)
	pos.lock.Unlock()

	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	var ret Edge

	err := client.Call("RPCNode.FindSuccessor", &pos.id, &ret)
	if err != nil {
		_ = client.Close()
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.lock.Lock()
	pos.sucList[0] = ret
	pos.lock.Unlock()
	for i := 0; i < 160; i++ {
		var t big.Int
		t.Add(&pos.id, powTwo(int64(i)))
		t.Mod(&t, mod)
		var temp Edge
		_ = pos.FindSuccessor(&t, &temp)

		pos.lock.Lock()
		pos.finger[i] = temp
		pos.lock.Unlock()
	}

	pos.sto.lock.Lock()
	err = client.Call("RPCNode.MoveDataToPre", &pos.id, &pos.sto.data)
	_ = client.Close()
	pos.sto.lock.Unlock()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	pos.inited = true
	return nil
}

func (pos *Node) GetPredecessor(_ *int, ret *Edge) error {
	pos.lock.Lock()
	*ret = pos.pre
	pos.lock.Unlock()
	return nil
}

func (pos *Node) Notify(x Edge, _ *int) error {
	// fmt.Print("in Notify pos = ", pos.Ip, " pre = ", x.Ip, "\n")
	if pos.pre.Ip == "" {
		pos.lock.Lock()
		pos.pre = x
		pos.lock.Unlock()
	} else {
		if pos.FixList() != nil {
			fmt.Print("Error(5):: All Successor has Failed.\n")
			return errors.New("error(5):: All Successor has Failed")
		}
		pos.lock.Lock()
		client := Dial(pos.sucList[0].Ip)
		pos.lock.Unlock()

		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		var sucID big.Int
		err := client.Call("RPCNode.GetID", 0, &sucID)
		_ = client.Close()
		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		if inRange(&pos.id, &sucID, &x.Id) {
			pos.insertSuc(x)
		}

	}

	return nil
}

func (pos *Node) Stabilize() error {
	if pos.FixList() != nil {
		fmt.Print("Error(5):: All Successor has Failed.\n")
		return errors.New("error(5):: All Successor has Failed")
	}
	pos.lock.Lock()
	client := Dial(pos.sucList[0].Ip)
	pos.lock.Unlock()

	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	var ret Edge
	err := client.Call("RPCNode.GetPredecessor", 0, &ret)
	if err != nil {
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
		pos.insertSuc(ret)
	}

	pos.lock.Lock()
	client = Dial(pos.sucList[0].Ip)
	pos.lock.Unlock()

	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("RPCNode.Notify", &Edge{pos.Ip, pos.id}, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	return nil
}

func (pos *Node) FixFingers() error {
	pos.fixing = (pos.fixing + 1) % Len

	var adder big.Int
	var ret Edge

	err := pos.FindSuccessor(adder.Add(&pos.id, powTwo(int64(pos.fixing))), &ret)

	if err != nil {
		fmt.Print("Error(3):: Unable to Find Successor.\n")
		return errors.New("error(3):: Unable to Find Successor")
	}

	pos.lock.Lock()
	pos.finger[pos.fixing] = ret
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

func (pos *Node) Maintain() {
	for pos.On {
		if pos.inited {
			fmt.Print("ip = ", pos.Ip, " Called Maintain\n")
			pos.CheckPredecessor()
			pos.Stabilize()
		}
		time.Sleep(maintainPeriod)
	}
}

func (pos *Node) FixList() error {
	pos.lock.Lock()
	p := -1
	for i := 0; i < SucListLen; i++ {
		if Ping(pos.sucList[i].Ip) == nil {
			p = i
			break
		}
	}
	if p == -1 {
		pos.lock.Unlock()
		fmt.Print("Error(5):: All Successor has Failed.\n")
		return errors.New("error(5):: All Successor has Failed")
	}
	for i := 0; p < SucListLen; {
		pos.sucList[i] = pos.sucList[p]
		i++
		p++
	}
	pos.lock.Unlock()
	return nil
}

func (pos *Node) insertSuc(newSuc Edge) {
	pos.lock.Lock()
	for i := SucListLen - 1; i > 0; i-- {
		pos.sucList[i] = pos.sucList[i-1]
	}
	pos.sucList[0] = newSuc
	pos.lock.Unlock()
}

func (pos *Node) Quit() error {
	var temp Edge
	pos.FindSuccessor(&pos.id, &temp)
	pos.insertSuc(temp)
	/* fmt.Print(temp.Ip, "\nsucList = ")
	for i := 0; i < SucListLen; i++ {
		fmt.Print(pos.sucList[i].Ip, " ")
	}
	fmt.Print("\nfinger = ")
	for i := 0; i < SucListLen; i++ {
		fmt.Print(pos.finger[i].Ip, " ")
	}
	fmt.Print("\n")
	panic("") */

	if pos.FixList() != nil {
		fmt.Print("Error(5):: All Successor has Failed.\n")
		return errors.New("error(5):: All Successor has Failed")
	}

	if pos.sucList[0].Ip == pos.Ip { // self-ring
		return nil
	}

	pos.lock.Lock()
	client := Dial(pos.sucList[0].Ip)
	pos.lock.Unlock()

	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	// move data
	pos.sto.lock.Lock()
	err := client.Call("RPCNode.MoveDataFromPre", &pos.sto.data, nil)
	_ = client.Close()
	pos.sto.lock.Unlock()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	if pos.pre.Ip != "" {
		// notify pre
		client = Dial(pos.pre.Ip)
		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.\n")
			return errors.New("error(1):: Dial Connect Failure")
		}
		pos.lock.Lock()
		err = client.Call("RPCNode.InsertSuc", &pos.sucList[0], nil)
		_ = client.Close()
		pos.lock.Unlock()
		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		// notify suc
		client = Dial(pos.sucList[0].Ip)
		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.\n")
			return errors.New("error(1):: Dial Connect Failure")
		}
		pos.lock.Lock()
		err = client.Call("RPCNode.UpdatePrv", &pos.pre, nil)
		_ = client.Close()
		pos.lock.Unlock()
		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.\n")
			return errors.New("error(2):: RPC Calling Failure")
		}
	}

	pos.inited = false
	return nil
}

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
	maintainPeriod = time.Second / 2
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

	sto     Storage
	dataPre Storage

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
	var blocked map[string]bool
	blocked = make(map[string]bool)

	for i := Len - 1; i >= 0; i-- {
		if _, err := blocked[pos.finger[i].Ip]; err {
			pos.finger[i] = Edge{pos.Ip, pos.id}
			continue
		}
		if inRange(&pos.id, id, &pos.finger[i].Id) {
			ret := pos.finger[i]
			if Ping(ret.Ip) == nil {
				pos.lock.Unlock()
				return ret
			} else {
				blocked[pos.finger[i].Ip] = true
				pos.finger[i] = Edge{pos.Ip, pos.id}
			}
		}
	}
	pos.lock.Unlock()
	return Edge{"", *new(big.Int)}
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
	if nxt.Ip == "" && inRange(&pos.id, h, &sucID) { // chain query
		nxt = pos.sucList[0]
	}

	if nxt.Ip == "" { // failed
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

	// for force quit
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

	err := client.Call("RPCNode.RemoveDataPre", key, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

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
	// fmt.Print("value inserted at", pos.Ip, "\n")
	pos.sto.data[kv.Key] = kv.Val
	pos.sto.lock.Unlock()

	// for force quit
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

	err := client.Call("RPCNode.InsertDataPre", kv, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

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
	if pos.pre.Ip == "" {
		pos.lock.Lock()
		pos.pre = x
		pos.lock.Unlock()

		// for force quit
		client := Dial(x.Ip)
		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		var temp map[string]string
		err := client.Call("RPCNode.GetData", 0, &temp)
		_ = client.Close()

		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		pos.FillDataPre(temp)

	} else {
		pos.lock.Lock()
		client := Dial(pos.pre.Ip)
		pos.lock.Unlock()

		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		var preID big.Int
		err := client.Call("RPCNode.GetID", 0, &preID)
		_ = client.Close()
		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		if inRange(&preID, &pos.id, &x.Id) {
			pos.lock.Lock()
			pos.pre = x
			pos.lock.Unlock()

			// for force quit
			client := Dial(x.Ip)
			if client == nil {
				fmt.Print("Error(1):: Dial Connect Failure.\n")
				return errors.New("error(1):: Dial Connect Failure")
			}

			var temp map[string]string
			err := client.Call("RPCNode.GetData", 0, &temp)
			_ = client.Close()

			if err != nil {
				fmt.Print("Error(2):: RPC Calling Failure.\n")
				return errors.New("error(2):: RPC Calling Failure")
			}

			pos.FillDataPre(temp)
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
		// for force quit
		pos.MergeDataPre()
		pos.pre.Ip = ""
	}
	pos.lock.Unlock()
	return nil
}

func (pos *Node) Maintain() {
	for pos.On {
		if pos.inited {
			_ = pos.CheckPredecessor()
			_ = pos.Stabilize()
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
	if pos.FixList() != nil {
		fmt.Print("Error(5):: All Successor has Failed.\n")
		return errors.New("error(5):: All Successor has Failed")
	}

	fmt.Print(pos.sucList[0].Ip, "\n")

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
	pos.sto.lock.Unlock()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	// for force quit
	pos.dataPre.lock.Lock()
	err = client.Call("RPCNode.FillDataPre", pos.dataPre.data, nil)
	_ = client.Close()
	pos.dataPre.lock.Unlock()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	if pos.pre.Ip != "" {
		// notify pre
		client = Dial(pos.pre.Ip)
		fmt.Print(pos.pre.Ip, "\n")
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

// for forceQuit
func (pos *Node) InsertDataPre(kv KeyValue) {
	pos.dataPre.lock.Lock()
	pos.dataPre.data[kv.Key] = kv.Val
	pos.dataPre.lock.Unlock()
}

func (pos *Node) RemoveDataPre(key string) {
	pos.dataPre.lock.Lock()
	if _, err := pos.dataPre.data[key]; !err {
		delete(pos.dataPre.data, key)
	}
	pos.dataPre.lock.Unlock()
}

func (pos *Node) MergeDataPre() {
	pos.dataPre.lock.Lock()
	pos.sto.lock.Lock()
	for key, value := range pos.dataPre.data {
		pos.sto.data[key] = value
	}
	pos.dataPre.lock.Unlock()
	pos.sto.lock.Unlock()
}

func (pos *Node) FillDataPre(mp map[string]string) {
	pos.dataPre.lock.Lock()
	pos.dataPre.data = mp
	pos.dataPre.lock.Unlock()
}

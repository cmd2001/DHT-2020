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
	SucListLen     = 20
	maintainPeriod = 50 * time.Millisecond
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
	var blocked map[string]bool
	blocked = make(map[string]bool)

	pos.lock.Lock()
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
		fmt.Print("Error(1):: Dial Connect Failure.(91)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}
	var sucID big.Int
	err := client.Call("RPCNode.GetID", 0, &sucID)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(99)\n")
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
		fmt.Print("Error(1):: Dial Connect Failure.(119)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("RPCNode.FindSuccessor", h, ret)
	_ = client.Close()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(128)\n")
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
		fmt.Print("Error(1):: Dial Connect Failure.(162)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err = client.Call("RPCNode.QueryInside", key, &ret)
	_ = client.Close()

	if err != nil {
		if err.Error() == "error(0):: Value Not Found" {
			return err
		}
		fmt.Print("Error(2):: RPC Calling Failure.(173)\n")
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
		fmt.Print("Error(1):: Dial Connect Failure.(205)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err := client.Call("RPCNode.RemoveDataPre", key, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(212)\n")
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
		fmt.Print("Error(1):: Dial Connect Failure.(230)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}
	err = client.Call("RPCNode.EraseInside", key, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(236)\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	return nil
}

func (pos *Node) InsertInside(kv KeyValue, _ *int) error {
	pos.sto.lock.Lock()
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
		fmt.Print("Error(1):: Dial Connect Failure.(259)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	err := client.Call("RPCNode.InsertDataPre", kv, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(265)\n")
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
		fmt.Print("Error(1):: Dial Connect Failure.(284)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}
	err = client.Call("RPCNode.InsertInside", KeyValue{key, val}, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(289)\n")
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
	pos.dataPre.lock.Lock()
	for k, v := range pos.sto.data {
		if !inRange(pr, &pos.id, hashStr(k)) {
			(*ret)[k] = v
		}
	}
	for k := range *ret {
		pos.dataPre.data[k] = pos.sto.data[k]
		delete(pos.sto.data, k)
	}
	pos.sto.lock.Unlock()
	pos.dataPre.lock.Unlock()
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
		fmt.Print("Error(1):: Dial Connect Failure.(341)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	var ret Edge

	err := client.Call("RPCNode.FindSuccessor", &pos.id, &ret)
	if err != nil {
		_ = client.Close()
		fmt.Print("Error(2):: RPC Calling Failure.(352)\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	_ = pos.insertSuc(ret)
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

	pos.lock.Lock()
	_ = client.Close()
	client = Dial(pos.sucList[0].Ip)
	pos.lock.Unlock()

	temp := make(map[string]string)
	err = client.Call("RPCNode.MoveDataToPre", &pos.id, &temp)
	_ = client.Close()

	pos.sto.lock.Lock()
	pos.sto.data = temp
	pos.sto.lock.Unlock()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(383)\n")
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
			fmt.Print("Error(1):: Dial Connect Failure.(397)\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		var temp map[string]string
		err := client.Call("RPCNode.GetData", 0, &temp)
		_ = client.Close()

		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.(416)\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		pos.FillDataPre(temp)

	} else {
		pos.lock.Lock()
		client := Dial(pos.pre.Ip)
		pos.lock.Unlock()

		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.(418)\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		var preID big.Int
		err := client.Call("RPCNode.GetID", 0, &preID)
		_ = client.Close()
		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.(436)\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		if inRange(&preID, &pos.id, &x.Id) {
			pos.lock.Lock()
			pos.pre = x
			pos.lock.Unlock()

			// for force quit
			client := Dial(x.Ip)
			if client == nil {
				fmt.Print("Error(1):: Dial Connect Failure.(438)\n")
				return errors.New("error(1):: Dial Connect Failure")
			}

			var temp map[string]string
			err := client.Call("RPCNode.GetData", 0, &temp)
			_ = client.Close()

			if err != nil {
				fmt.Print("Error(2):: RPC Calling Failure.(457)\n")
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
		fmt.Print("Error(1):: Dial Connect Failure.(469)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	var ret Edge
	err := client.Call("RPCNode.GetPredecessor", 0, &ret)
	if err != nil {
		fmt.Print("Error(1):: Dial Connect Failure.(476)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	if Ping(ret.Ip) != nil { // nil pre
		return nil
	}

	var sucID big.Int
	err = client.Call("RPCNode.GetID", 0, &sucID)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(498)\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	if inRange(&pos.id, &sucID, &ret.Id) {
		_ = pos.insertSuc(ret)
	}

	pos.lock.Lock()
	client = Dial(pos.sucList[0].Ip)
	pos.lock.Unlock()

	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.(502)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}
	err = client.Call("RPCNode.Notify", &Edge{pos.Ip, pos.id}, nil)
	_ = client.Close()
	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(517)\n")
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
	pip := pos.pre.Ip
	pos.lock.Unlock()

	if pip != "" && Ping(pip) != nil {
		pos.MergeDataPre()

		if pos.FixList() != nil {
			fmt.Print("Error(5):: All Successor has Failed.\n")
			return errors.New("error(5):: All Successor has Failed")
		}
		pos.lock.Lock()
		client := Dial(pos.sucList[0].Ip)
		pos.lock.Unlock()

		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.(555)\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		pos.sto.lock.Lock()
		temp := pos.sto.data
		pos.sto.lock.Unlock()

		err := client.Call("RPCNode.FillDataPre", temp, nil)
		_ = client.Close()

		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.(573)\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		pos.pre.Ip = ""
	}

	return nil
}

func (pos *Node) MaintainSuccessorList() error {
	pos.lock.Lock()
	pip := pos.pre.Ip
	pos.lock.Unlock()

	if pip != "" && pip != pos.Ip {
		client := Dial(pip)
		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.(584)\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		var temp [SucListLen]Edge
		pos.lock.Lock()
		for i := 0; i < SucListLen; i++ {
			temp[i] = pos.sucList[i]
		}
		pos.lock.Unlock()

		err := client.Call("RPCNode.CopySuccessorList", &temp, nil)
		_ = client.Close()

		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.(606)\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

	}
	return nil
}

func (pos *Node) Maintain() {
	for pos.On {
		if pos.inited {
			_ = pos.CheckPredecessor()
			_ = pos.Stabilize()
			_ = pos.MaintainSuccessorList()
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
		fmt.Print("Pos = ", pos.Ip, " sucList = ")
		for i := 0; i < SucListLen; i++ {
			fmt.Print(pos.sucList[i].Ip, " ")
		}
		fmt.Print("\n")
		return errors.New("error(5):: All Successor has Failed")
	} else {
		var bak Edge
		if p != -1 {
			bak = pos.sucList[p]
		}
		flag := p != 0

		for i := 0; p < SucListLen; {
			pos.sucList[i] = pos.sucList[p]
			i++
			p++
		}
		pos.lock.Unlock()

		if flag {
			time.Sleep(maintainPeriod * 6 / 5) // wait for suc do mergeDataPre First.
			client := Dial(bak.Ip)
			if client == nil {
				fmt.Print("Error(1):: Dial Connect Failure.(639)\n")
				return errors.New("error(1):: Dial Connect Failure")
			}

			// Notify.
			err := client.Call("RPCNode.Notify", &Edge{pos.Ip, pos.id}, nil)
			_ = client.Close()

			if err != nil {
				fmt.Print("Error(2):: RPC Calling Failure.(669)\n")
				return errors.New("error(2):: RPC Calling Failure")
			}
		}
		return nil
	}
}

func (pos *Node) insertSuc(newSuc Edge) error {
	pos.lock.Lock()
	if newSuc.Ip == pos.sucList[0].Ip {
		pos.lock.Unlock()
		return nil
	}
	// fmt.Print("pos = ", pos.Ip, " suc = ", newSuc.Ip, "\n")
	for i := SucListLen - 1; i > 0; i-- {
		pos.sucList[i] = pos.sucList[i-1]
	}
	pos.sucList[0] = newSuc
	pos.lock.Unlock()

	// for force quit
	client := Dial(newSuc.Ip)
	if client == nil {
		fmt.Print("Error(1):: Dial Connect Failure.(678)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	pos.sto.lock.Lock()
	temp := pos.sto.data
	pos.sto.lock.Unlock()

	err := client.Call("RPCNode.FillDataPre", temp, nil)
	_ = client.Close()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(705)\n")
		return errors.New("error(2):: RPC Calling Failure")
	}
	return nil
}

func (pos *Node) Quit() error {
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
		fmt.Print("Error(1):: Dial Connect Failure.(673)\n")
		return errors.New("error(1):: Dial Connect Failure")
	}

	// move data
	pos.sto.lock.Lock()
	temp := pos.sto.data
	pos.sto.lock.Unlock()
	err := client.Call("RPCNode.MoveDataFromPre", temp, nil)

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(737)\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	// for force quit
	pos.dataPre.lock.Lock()
	temp2 := pos.dataPre.data
	pos.dataPre.lock.Unlock()

	err = client.Call("RPCNode.FillDataPre", temp2, nil)
	_ = client.Close()

	if err != nil {
		fmt.Print("Error(2):: RPC Calling Failure.(750)\n")
		return errors.New("error(2):: RPC Calling Failure")
	}

	if pos.pre.Ip != "" {
		// notify pre
		client = Dial(pos.pre.Ip)
		fmt.Print(pos.pre.Ip, "\n")
		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.(703)\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		pos.lock.Lock()
		temp3 := pos.sucList[0]
		pos.lock.Unlock()

		err = client.Call("RPCNode.InsertSuc", &temp3, nil)
		_ = client.Close()

		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.(774)\n")
			return errors.New("error(2):: RPC Calling Failure")
		}

		// notify suc
		client = Dial(pos.sucList[0].Ip)
		if client == nil {
			fmt.Print("Error(1):: Dial Connect Failure.(720)\n")
			return errors.New("error(1):: Dial Connect Failure")
		}

		pos.lock.Lock()
		temp4 := pos.pre
		pos.lock.Unlock()

		err = client.Call("RPCNode.UpdatePrv", &temp4, nil)
		_ = client.Close()

		if err != nil {
			fmt.Print("Error(2):: RPC Calling Failure.(793)\n")
			return errors.New("error(2):: RPC Calling Failure")
		}
	}
	pos.inited = false
	return nil
}

// for force quit

func (pos *Node) CopySuccessorList(sucList *[SucListLen]Edge) {
	pos.lock.Lock()
	for i := 1; i < SucListLen; i++ {
		pos.sucList[i] = sucList[i-1]
	}
	pos.lock.Unlock()
}

func (pos *Node) InsertDataPre(kv KeyValue) {
	pos.dataPre.lock.Lock()
	pos.dataPre.data[kv.Key] = kv.Val
	pos.dataPre.lock.Unlock()
}

func (pos *Node) RemoveDataPre(key string) {
	pos.dataPre.lock.Lock()
	if _, ok := pos.dataPre.data[key]; ok {
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

func (pos *Node) PrintLink() {
	fmt.Print("Pos = ", pos.Ip, " Suc = ", pos.sucList[0].Ip, " Pre = ", pos.pre.Ip, "\n")
}

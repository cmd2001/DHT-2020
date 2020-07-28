package chord

import (
	"math/big"
	"net"
)

// Node used in RPC Calls

type RPCNode struct {
	Data *Node

	Listen net.Listener
}

/*
function to implement:
GetID, FindSuccessor, QueryInside, EraseInside, InsertInside, MoveDataFromPre, GetPredecessor, Notify, MoveDataToPre
*/

func (pos *RPCNode) GetID(_ *int, ret *big.Int) error {
	pos.Data.GetID(nil, ret)
	return nil
}

func (pos *RPCNode) FindSuccessor(h *big.Int, ret *Edge) error {
	return pos.Data.FindSuccessor(h, ret)
}

func (pos *RPCNode) QueryInside(key string, ret *string) error {
	return pos.Data.QueryInside(key, ret)
}

func (pos *RPCNode) EraseInside(key string, _ *int) error {
	return pos.Data.EraseInside(key, nil)
}

func (pos *RPCNode) InsertInside(kv KeyValue, _ *int) error {
	return pos.Data.InsertInside(kv, nil)
}

func (pos *RPCNode) MoveDataFromPre(dat map[string]string, _ *int) error {
	return pos.Data.MoveDataFromPre(dat, nil)
}

func (pos *RPCNode) GetPredecessor(_ *int, ret *Edge) error {
	return pos.Data.GetPredecessor(nil, ret)
}

func (pos *RPCNode) Notify(x *Edge, _ *int) error {
	return pos.Data.Notify(*x, nil)
}

func (pos *RPCNode) MoveDataToPre(pr *big.Int, ret *map[string]string) error {
	return pos.Data.MoveDataToPre(pr, ret)
}

func (pos *RPCNode) InsertSuc(newSuc *Edge, _ *int) error {
	pos.Data.insertSuc(*newSuc)
	return nil
}

func (pos *RPCNode) UpdatePrv(newPre *Edge, _ *int) error {
	pos.Data.lock.Lock()
	pos.Data.pre = *newPre
	pos.Data.lock.Unlock()
	return nil
}

package chord

import (
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
	tar string
	val big.Int
}

type Storage struct {
	Map  map[string]string
	lock sync.Mutex // for multithreading
}

type Node struct {
	es        [Len]Edge
	pre       Edge
	suc       Edge
	data_lock sync.Mutex

	sto    Storage
	stoPre Storage

	state bool // 1 for on
}

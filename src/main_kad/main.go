package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"
)

func randStr() string {
	var ret string
	for i := 0; i <= 5; i++ {
		x := rand.Int() % 26
		ret = ret + string(x+'a')
	}
	return ret
}

const (
	testGroup  = 5
	nodeLen    = 50
	quitSize   = 2
	insertSize = 1000
	randomSize = 512
	sleepTime  = time.Second * 6 / 10
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // use all CPUs
	// seed := time.Now().UnixNano()
	seed := int64(1596632838873651343)
	fmt.Print("Random Seed = ", seed, "\n")
	rand.Seed(seed)

	fmt.Print("This is Main\n")

	var nodes [nodeLen]dhtNode
	deleted := make(map[int]int)

	for i := 0; i < nodeLen; i++ {
		nodes[i] = NewNode(2333 + i)
		nodes[i].Run()
		time.Sleep(time.Second / 10)
	}
	fmt.Println("Nodes inited")
	nodes[0].Create()
	for i := 1; i < nodeLen; i++ {
		fmt.Print("Joining Node ", i, "\n")
		nodes[i].Join(":2333")
		// time.Sleep(sleepTime)
	}
	fmt.Print("ALL Nodes Joined\n")

	for T := 0; T < testGroup; T++ {
		fmt.Print("Conducting Test Group: ", T+1, "\n")
		mp := make(map[int]string)

		fmt.Print("Conducting Insertion Test\n")
		for i := 0; i < insertSize; i++ {
			str := randStr()
			mp[rand.Int()%randomSize] = str

			id := rand.Int() % nodeLen
			for _, ok := deleted[id]; ok; {
				id = rand.Int() % nodeLen
				_, ok = deleted[id]
			}
			nodes[id].Put(str, str)

			id = rand.Int() % nodeLen
			for _, ok := deleted[id]; ok; {
				id = rand.Int() % nodeLen
				_, ok = deleted[id]
			}

			ok, val := nodes[id].Get(str)
			if !ok || val != str {
				panic("Wrong Answer!")
			}
			if (i+1)%100 == 0 {
				fmt.Print("Insertion Tests Passed: ", i+1, "\n")
			}
		}

		for i := 0; i < quitSize; i++ {
			id := rand.Int() % nodeLen
			for _, ok := deleted[id]; ok; {
				id = rand.Int() % nodeLen
				_, ok = deleted[id]
			}

			if rand.Int()%2 != 0 {
				fmt.Print("Quiting Node ", id, "\n")
				nodes[id].Quit()
			} else {
				fmt.Print("ForceQuiting Node ", id, "\n")
				nodes[id].ForceQuit()
			}
			deleted[id] = id
		}

		fmt.Print("Conducting Random Get Test\n")

		for _, str := range mp {
			id := rand.Int() % nodeLen
			for _, ok := deleted[id]; ok; {
				id = rand.Int() % nodeLen
				_, ok = deleted[id]
			}
			ok, val := nodes[id].Get(str)
			if !ok || val != str {
				fmt.Print("ok = ", ok, " val = ", val, " str = ", str, "\n")
				panic("Wrong Answer!")
			}
		}
	}
	fmt.Print("Congratulations! ALL Pressure Tests Passed!")
}

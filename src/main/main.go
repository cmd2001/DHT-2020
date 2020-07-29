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
	nodeLen    = 100
	quitSize   = 20
	testSize   = 2000
	randomSize = 512
	sleepTime  = time.Second * 6 / 10
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // use all CPUs
	rand.Seed(time.Now().UnixNano())

	fmt.Print("This is Main\n")
	var nodes [nodeLen]dhtNode
	for i := 0; i < nodeLen; i++ {
		nodes[i] = NewNode(2333 + i)
		nodes[i].Run()
	}
	nodes[0].Create()
	for i := 1; i < nodeLen; i++ {
		fmt.Print("Joining Node ", i, "\n")
		nodes[i].Join(":2333")
		time.Sleep(sleepTime)
	}
	fmt.Print("ALL Joined\n")

	mp := make(map[int]string)
	for i := 0; i < testSize; i++ {
		str := randStr()
		mp[rand.Int()%randomSize] = str

		var id int

		id = rand.Int() % nodeLen
		nodes[id].Put(str, str)

		id = rand.Int() % nodeLen
		ok, val := nodes[id].Get(str)
		if !ok || val != str {
			panic("Wrong Answer!")
		}
		if i%100 == 0 {
			fmt.Print("Tests Passed: ", i, "\n")
		}
	}

	deleted := make(map[int]int)

	for i := 0; i < quitSize; i++ {
		id := rand.Int() % nodeLen
		for _, ok := deleted[id]; ok; {
			id = rand.Int() % nodeLen
			_, ok = deleted[id]
		}
		fmt.Print("Quiting Node ", id, "\n")
		nodes[id].Quit()
		deleted[id] = id
		time.Sleep(sleepTime)
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
			panic("Wrong Answer!")
		}
	}

	fmt.Print("Conducting Random Erase Test\n")

	for _, str := range mp {
		id := rand.Int() % nodeLen
		for _, ok := deleted[id]; ok; {
			id = rand.Int() % nodeLen
			_, ok = deleted[id]
		}
		ok := nodes[id].Delete(str)
		if !ok {
			panic("Failed to Delete!")
		}

		id = rand.Int() % nodeLen
		for _, ok := deleted[id]; ok; {
			id = rand.Int() % nodeLen
			_, ok = deleted[id]
		}

		ok, _ = nodes[id].Get(str)
		if ok {
			panic("Deleted Value can be Found!")
		}
	}

}

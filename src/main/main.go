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

/* func main() {
	fmt.Print("This is Main\n")
	a := NewNode(2333)
	a.Run()
	a.Create()
	b := NewNode(2334)
	b.Run()
	b.Join(":2333")
	fmt.Print("B Joined\n")

	c := NewNode(2335)
	c.Join(":2333")
	fmt.Print("C Joined\n")


	for i:= 0; i <= 100; i++ {
		str := randStr()
		if rand.Int() % 2 != 0 {
			a.Put(str, str)
			fmt.Print("put finished\n")
			fmt.Print(b.Get(str))
			fmt.Print("\n")
		} else {
			b.Put(str, str)
			fmt.Print("put finished\n")
			fmt.Print(a.Get(str))
			fmt.Print("\n")
		}
	}
} */

const nodeLen = 2

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
	}
	fmt.Print("ALL Joined")

	mp := make(map[int]string)
	for i := 0; i < 1000; i++ {
		str := randStr()
		mp[rand.Int()%255] = str

		var id int

		id = rand.Int() % nodeLen
		nodes[id].Put(str, str)

		id = rand.Int() % nodeLen
		ok, val := nodes[id].Get(str)
		if !ok || val != str {
			fmt.Print("i = ", i, "\n")
			panic("Wrong Answer!")
		}
		// fmt.Print("Test Passed: ", str, "\n")
		if i%100 == 0 {
			fmt.Print("Tests Passed: ", i, "\n")
		}
	}

	/* fmt.Print("Waiting for maintain")
	time.Sleep(time.Second * 15) */

	/* for i := 0; i < nodeLen / 10; i++ {
		id := rand.Int() % nodeLen
		nodes[id].Quit()
	} */

	/* nodes[0].Quit()
	fmt.Print("NODE0 QUITED\n") */

	for _, str := range mp {
		id := rand.Int() % nodeLen
		for id == 0 {
			id = rand.Int() % nodeLen
		}

		ok, val := nodes[id].Get(str)
		if !ok || val != str {
			panic("Wrong Answer!")
		}
	}

	for _, str := range mp {
		id := rand.Int() % nodeLen
		for id == 0 {
			id = rand.Int() % nodeLen
		}

		ok := nodes[id].Delete(str)
		if !ok {
			panic("Failed to Delete!")
		}

		id = rand.Int() % nodeLen
		for id == 0 {
			id = rand.Int() % nodeLen
		}

		ok, _ = nodes[id].Get(str)
		if ok {
			panic("Deleted Value can be Found!")
		}
	}
}

package main

import (
	"fmt"
	"math/rand"
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

const nodeLen = 200

func main() {
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

	for i := 0; i < 10000; i++ {
		str := randStr()

		var id int

		id = rand.Int() % nodeLen
		nodes[id].Put(str, str)

		id = rand.Int() % nodeLen
		ok, val := nodes[id].Get(str)
		if !ok || val != str {
			panic("Wrong Answer!")
		}
		// fmt.Print("Test Passed: ", str, "\n")
		if i%100 == 0 {
			fmt.Print("Tests Passed: ", i, "\n")
		}
	}

}

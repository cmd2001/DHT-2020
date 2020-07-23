package main

import "fmt"

func main() {
	fmt.Print("This is Main")
	a := NewNode(2333)
	a.Run()
	a.Create()
	b := NewNode(2334)
	b.Create()
	b.Join(":2333")

}

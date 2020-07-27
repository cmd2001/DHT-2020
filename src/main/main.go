package main

import "fmt"

func main() {
	fmt.Print("This is Main\n")
	a := NewNode(2333)
	a.Run()
	a.Create()
	b := NewNode(2334)
	b.Create()
	fmt.Print("Created\n")
	b.Join(":2333")
	fmt.Print("Joined\n")

	/* a.Put("abc", "abc")
	fmt.Print("put finished\n")
	fmt.Print(b.Get("abc")) */

	b.Put("fuck", "fuck")
	fmt.Print("put finished\n")
	fmt.Print(a.Get("fuck"))
}

/* package main

import (
	"flag"
	"math/rand"
	"os"
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
	testGroup  = 1
	nodeLen    = 10
	quitSize   = 5
	insertSize = 2000
	randomSize = 512
	sleepTime  = time.Second * 6 / 10
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // use all CPUs
	seed := time.Now().UnixNano()
	fmt.Print("Random Seed = ", seed, "\n")
	rand.Seed(seed)

	fmt.Print("This is Main\n")

	var nodes [nodeLen]dhtNode
	deleted := make(map[int]int)

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
			time.Sleep(sleepTime * 5)
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
	fmt.Print("Congratulations! ALL Pressure Tests Passed!")
} */

package main

import (
	"flag"
	"math/rand"
	"os"
	"time"
)

var (
	help     bool
	testName string
)

func init() {
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&testName, "test", "", "which test(s) do you want to run: basic/advance/all")

	flag.Usage = usage
	flag.Parse()

	if help || (testName != "basic" && testName != "advance" && testName != "all") {
		flag.Usage()
		os.Exit(0)
	}

	rand.Seed(time.Now().UnixNano())
}

func main() {
	_, _ = yellow.Println("Welcome to DHT-2020 Test Program!\n")

	var basicFailRate float64
	//var advanceFailRate float64

	switch testName {
	case "all":
		fallthrough
	case "basic":
		_, _ = yellow.Println("Basic Test Begins:")
		basicFailedCnt, basicTotalCnt := basicTest()
		basicFailRate = float64(basicFailedCnt) / float64(basicTotalCnt)
		if basicFailRate > basicTestMaxFailRate {
			_, _ = red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
		} else {
			_, _ = green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
		}

		if testName == "basic" {
			break
		}
		fallthrough
	case "advance":
		_, _ = cyan.Println("Advance Test Begins:")
		_, _ = red.Println("To be added...")
	}
	_, _ = cyan.Println("\nFinal print:")
	if basicFailRate > basicTestMaxFailRate {
		_, _ = red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
	} else {
		_, _ = green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
	}
}

func usage() {
	flag.PrintDefaults()
}

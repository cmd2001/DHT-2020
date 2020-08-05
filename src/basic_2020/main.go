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

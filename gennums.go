package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		fmt.Printf("%09d\n", r.Int63()/10000000000)
	}
}

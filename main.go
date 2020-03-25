package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const port = "8888"
const acceptedTokenPattern = `^[0-9]{9}`
const file = "numbers.log"

var saveMap sync.Map
var lock sync.Mutex
var fileWriter *bufio.Writer
var accepteds int64
var acceptedTotal int64
var duplicates int64

func main() {
	setupExitHandler()

	saveFile, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	check(err, "File opened for save")
	saveFile.Truncate(0)
	fileWriter = bufio.NewWriter(saveFile)

	defer saveFile.Close()

	queue := make(chan net.Conn)
	defer close(queue)

	// start the reporter
	go func() {
		for {
			time.Sleep(10 * time.Second)
			oldDups := atomic.SwapInt64(&duplicates, 0)
			oldAccepteds := atomic.SwapInt64(&accepteds, 0)
			atomic.AddInt64(&acceptedTotal, oldAccepteds)
			fmt.Printf("Received %v unique numbers, %v duplicates. Unique total: %v\n",
				oldAccepteds, oldDups, acceptedTotal)
		}
	}()

	for i := 0; i < 5; i++ {
		go acceptCode(queue)
	}

	ln, err := net.Listen("tcp", ":"+port)
	check(err, "Listening on port "+port+"...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Bad accept:", err)
			conn.Close()
		} else {
			select {
			case queue <- conn:
				fmt.Println("accepted")
				// ok = true
			default:
				fmt.Println("Rejected connection, queue full")
			}
		}
	}
}

func acceptCode(queue chan net.Conn) {
	validRegEx := regexp.MustCompile(acceptedTokenPattern)
	for {
		conn, ok := <-queue
		if !ok {
			fmt.Println("Queue is closed, done")
			break
		}

		reader := bufio.NewScanner(conn)
		for reader.Scan() {
			tok := reader.Text()
			if err := reader.Err(); err != nil {
				fmt.Printf("Client stopped [%v] : %v\n", tok, err)
				conn.Close()
				break
			}

			if validRegEx.MatchString(tok) {
				save(tok)
			} else {
				fmt.Printf("Rejected not a num: [%v]\n", tok)
				conn.Close()
				break
			}
		}
	}
}

func save(token string) {
	_, exists := saveMap.LoadOrStore(token, nil)
	if exists {
		//fmt.Println("Duplicate:", token)
		atomic.AddInt64(&duplicates, 1)
	} else {
		//fmt.Printf("Accepted: %v\n", token)
		atomic.AddInt64(&accepteds, 1)
		// and append to file
		lock.Lock()
		defer lock.Unlock()

		if _, err := fileWriter.Write([]byte(token + "\n")); err != nil {
			log.Fatal(err)
		}
		fileWriter.Flush()
	}
}

func setupExitHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\n- Exiting")
		os.Exit(1)
	}()
}

func check(err error, message string) {
	if err != nil {
		panic(err)
	}
	fmt.Println(message)
}

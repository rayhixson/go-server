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
const terminateString = "terminate"
const acceptedTokenPattern = `^\d{9}$`
const fileName = "numbers.log"
const workerCount = 5

var saveMap sync.Map
var saveFile *os.File
var accepteds int64
var acceptedTotal int64
var duplicates int64
var waitGroup sync.WaitGroup
var running = true

type WorkerFunc func(net.Conn)

var workerQueue = make(chan WorkerFunc, workerCount)
var writerQueue = make(chan string)

func main() {
	setupExitHandler()

	var err error
	saveFile, err = os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	saveFile.Truncate(0)

	defer saveFile.Close()
	defer close(workerQueue)

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

	// do all file writes from one routing
	go func() {
		for {
			token := <-writerQueue
			// write until we get the terminate signal
			if token == terminateString {
				break
			}
			if _, err := saveFile.Write([]byte(token + "\n")); err != nil {
				log.Fatalf("Failed to save [%v] -> %v", token, err)
			}
		}
	}()

	for i := 0; i < workerCount; i++ {
		workerQueue <- acceptCode
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Listening on port [%v] ...\n", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Bad accept:", err)
			conn.Close()
		} else {
			select {
			case worker := <-workerQueue:
				fmt.Println("accepted")
				waitGroup.Add(1)
				go worker(conn)
			default:
				fmt.Println("Rejected connection, queue full")
			}
		}
	}
}

func acceptCode(conn net.Conn) {
	defer waitGroup.Done()

	validRegEx := regexp.MustCompile(acceptedTokenPattern)
	reader := bufio.NewScanner(conn)
	for reader.Scan() && running {
		tok := reader.Text()
		if err := reader.Err(); err != nil {
			fmt.Printf("Client stopped [%v] : %v\n", tok, err)
			conn.Close()
			break
		}

		if tok == terminateString {
			defer exit()
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

	workerQueue <- acceptCode
}

func save(token string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered in save, token [%v] -> %v", token, r)
		}
	}()

	_, exists := saveMap.LoadOrStore(token, nil)
	if exists {
		atomic.AddInt64(&duplicates, 1)
	} else {
		atomic.AddInt64(&accepteds, 1)

		// and append to file
		writerQueue <- token
	}
}

func setupExitHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		exit()
	}()
}

func exit() {
	// signal all threads and main loop to end
	running = false

	waitGroup.Wait()

	// terminate the writer AFTER the handlers are done reading their last token
	writerQueue <- terminateString

	os.Exit(1)
}

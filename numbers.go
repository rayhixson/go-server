package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
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

var saveMap map[uint32]bool
var mutex sync.Mutex
var accepteds atomic.Int64
var acceptedTotal atomic.Int64
var duplicates atomic.Int64
var waitGroup sync.WaitGroup
var running = true
var logger *bufio.Writer

type WorkerFunc func(net.Conn)

var workerQueue = make(chan WorkerFunc, workerCount)
var writerQueue = make(chan uint32)
var terminate = make(chan bool)

func main() {
	setupExitHandler()
	saveMap = make(map[uint32]bool, 100000000)

	saveFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer saveFile.Close()
	saveFile.Truncate(0)
	logger = bufio.NewWriter(saveFile)

	// start the reporter
	go func() {
		for {
			time.Sleep(10 * time.Second)
			oldDups := duplicates.Swap(0)
			oldAccepteds := accepteds.Swap(0)
			newTotal := acceptedTotal.Add(oldAccepteds)
			fmt.Printf("Received %v unique numbers, %v duplicates. Unique total: %v\n",
				oldAccepteds, oldDups, newTotal)
		}
	}()

	// do all file writes from one routine
	go func() {
		for {
			select {
			case token := <-writerQueue:
				if _, err := logger.WriteString(strconv.Itoa(int(token)) + "\n"); err != nil {
					log.Fatalf("Failed to save [%d] -> %v", token, err)
				}
			case <-terminate:
				break
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

		itok, err := strconv.Atoi(tok)
		if err == nil && len(tok) == 9 {
			save(uint32(itok))
		} else {
			fmt.Printf("Rejected not a num: [%v]\n", tok)
			conn.Close()
			break
		}
	}

	workerQueue <- acceptCode
}

func save(token uint32) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered in save, token [%v] -> %v", token, r)
			debug.PrintStack()
			os.Exit(2)
		}
	}()

	mutex.Lock()
	_, exists := saveMap[token]
	if !exists {
		saveMap[token] = true
		mutex.Unlock()

		accepteds.Add(1)

		// and append to file
		writerQueue <- token
	} else {
		mutex.Unlock()
		duplicates.Add(1)
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

	// terminate the writer AFTER the handlers are done with their last token
	terminate <- true

	close(workerQueue)
	close(writerQueue)
	logger.Flush()

	os.Exit(1)
}

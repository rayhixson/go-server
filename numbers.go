package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
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

// var saveMap sync.Map
var saveMap map[uint32]bool
var mutex sync.Mutex
var accepteds int64
var acceptedTotal int64
var duplicates int64
var waitGroup sync.WaitGroup
var running = true
var logger *bufio.Writer

type WorkerFunc func(net.Conn)

var workerQueue = make(chan WorkerFunc, workerCount)
var writerQueue = make(chan string)

var m0, m1 runtime.MemStats

func memUsage(m1, m2 *runtime.MemStats) {
	fmt.Println("Alloc:", m2.Alloc-m1.Alloc,
		"TotalAlloc:", m2.TotalAlloc-m1.TotalAlloc,
		"HeapAlloc:", m2.HeapAlloc-m1.HeapAlloc)
}

func main() {
	runtime.ReadMemStats(&m0)
	saveMap = make(map[uint32]bool, 100000000)
	setupExitHandler()

	saveFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer saveFile.Close()
	saveFile.Truncate(0)
	logger = bufio.NewWriter(log.New(saveFile, "", 0).Writer())

	// start the reporter
	go func() {
		for {
			time.Sleep(10 * time.Second)
			oldDups := atomic.SwapInt64(&duplicates, 0)
			oldAccepteds := atomic.SwapInt64(&accepteds, 0)
			atomic.AddInt64(&acceptedTotal, oldAccepteds)
			fmt.Printf("Received %v unique numbers, %v duplicates. Unique total: %v\n",
				oldAccepteds, oldDups, acceptedTotal)
			//runtime.ReadMemStats(&m1)
			//memUsage(&m0, &m1)
		}
	}()

	// do all file writes from one routine
	go func() {
		for {
			token := <-writerQueue
			// write until we get the terminate signal
			if token == terminateString {
				break
			}
			if _, err := logger.WriteString(token + "\n"); err != nil {
				log.Fatalf("Failed to save [%s] -> %v", token, err)
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
			debug.PrintStack()
			os.Exit(2)
		}
	}()

	itoken, err := strconv.Atoi(token)
	if err != nil {
		panic(err)
	}

	exists := false
	mutex.Lock()
	_, exists = saveMap[uint32(itoken)]
	if !exists {
		saveMap[uint32(itoken)] = true
		mutex.Unlock()

		atomic.AddInt64(&accepteds, 1)

		// and append to file
		writerQueue <- token
	} else {
		mutex.Unlock()
		atomic.AddInt64(&duplicates, 1)
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
	writerQueue <- terminateString

	close(workerQueue)
	close(writerQueue)
	logger.Flush()

	os.Exit(1)
}

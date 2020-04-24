package main

import (
	"bufio"
	"fmt"
	"io"
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
const workerCount = 5

var saveMap sync.Map
var numbersFileWriter *bufio.Writer
var lock sync.Mutex
var accepteds int64
var acceptedTotal int64
var duplicates int64

var running = true
var doneQueue = make(chan string)
var workerQueue = make(chan net.Conn)

func main() {
	setupExitHandler()

	saveFile, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	check(err, "File opened for save: "+file)
	saveFile.Truncate(0)
	numbersFileWriter = bufio.NewWriter(saveFile)

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

	for i := 0; i < workerCount; i++ {
		go acceptCode(workerQueue, fmt.Sprintf("Routine-%v", i))
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
			case workerQueue <- conn:
				fmt.Println("accepted")
				// ok = true
			default:
				fmt.Println("Rejected connection, queue full")
			}
		}
	}
}

func acceptCode(queue chan net.Conn, name string) {
	myFile := file + "." + name + ".tmp"
	saveFile, err := os.OpenFile(myFile, os.O_CREATE|os.O_RDWR, 0644)
	check(err, "File opened for save: "+myFile)
	saveFile.Truncate(0)
	fileWriter := bufio.NewWriter(saveFile)

	defer func() {
		// write my file to the final numbers file
		fileWriter.Flush()
		_, err = saveFile.Seek(0, 0)
		if err != nil {
			log.Println("Failed to seek to beginning of this routines file for saving:", myFile, err)
		} else {
			lock.Lock()
			_, err := io.Copy(numbersFileWriter, saveFile)
			if err != nil {
				log.Println("Failed to copy contents to final file: %v", err)
			}
			numbersFileWriter.Flush()
			lock.Unlock()

			saveFile.Close()
			err = os.Remove(myFile)
			if err != nil {
				log.Println("Failed to remove temp file but did save it: ", err)
			}
		}

		doneQueue <- name
	}()

	validRegEx := regexp.MustCompile(acceptedTokenPattern)
	for running {
		conn, ok := <-queue
		if !ok {
			fmt.Println("Queue is closed, done")
			break
		}

		reader := bufio.NewScanner(conn)
		for reader.Scan() && running {
			tok := reader.Text()
			if err := reader.Err(); err != nil {
				fmt.Printf("Client stopped [%v] : %v\n", tok, err)
				conn.Close()
				break
			}

			if validRegEx.MatchString(tok) {
				save(tok, fileWriter)
			} else {
				fmt.Printf("Rejected not a num: [%v]\n", tok)
				conn.Close()
				break
			}
		}
	}
}

func save(token string, fileWriter io.Writer) {
	_, exists := saveMap.LoadOrStore(token, nil)
	if exists {
		//fmt.Println("Duplicate:", token)
		atomic.AddInt64(&duplicates, 1)
	} else {
		//fmt.Printf("Accepted: %v\n", token)
		atomic.AddInt64(&accepteds, 1)
		// and append to file

		if _, err := fileWriter.Write([]byte(token + "\n")); err != nil {
			log.Fatal(err)
		}
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
	running = false
	close(workerQueue)

	routinesDone := 0
	for routinesDone < workerCount {
		select {
		case routineName := <-doneQueue:
			routinesDone++
			fmt.Println("Routine done: " + routineName)
		}
	}

	// grab all files and write to one
	numbersFileWriter.Flush()
	fmt.Printf("\n- Exiting\n")
	os.Exit(1)
}

func check(err error, message string) {
	if err != nil {
		panic(err)
	}
	fmt.Println(message)
}

package main

import (
	"fmt"
	"log"
	"net"
	"time"
	"bufio"
	"strings"
	"strconv"
)

var version string = "0.1.0"

type Metric struct {
	Prefix    string       `json:"prefix,omitempty"`
	Path      string       `json:"path,omitempty"`
	Value     string       `json:"value,omitempty"`
	Timestamp int64        `json:"timestamp,omitempty"`
	Tenant    string       `json:"tenant,omitempty"`
}

func runReceiver(address string, port int, inputChan chan Metric) {
	netAddress := fmt.Sprintf("%s:%d", address, port)

	log.Printf("Starting Receiver at: '%s'.\n", netAddress)

	ln, err := net.Listen("tcp", netAddress)
	if err != nil {
		log.Print(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Print(err)
		} else {
			go readMetric(conn, inputChan)
		}
	}
}

func readMetric(connection net.Conn, inputChan chan Metric) {
	connection.SetReadDeadline(time.Now().Add(5*time.Second))

	metricString, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		log.Print(err)
		return
	}

	metricString = strings.TrimSuffix(metricString, "\n")
	log.Printf("In: '%s'.", metricString)

	metricSlice := strings.Split(metricString, " ")

	if err != nil {
		log.Print(err)
		return
	}

	metric := Metric{}

	switch len(metricSlice) {
		case 3:
			metric.Tenant = ""
		case 4:
			metric.Tenant = metricSlice[3]
		default:
			log.Printf("Bad metric: '%s'.", metricString)
			connection.Close()
			return
	}

	timestamp, err := strconv.ParseInt(metricSlice[2], 10, 64)

	metric.Prefix = ""
	metric.Path = metricSlice[0]
	metric.Value =  metricSlice[1]
	metric.Timestamp = timestamp


	inputChan <- metric

	connection.Close()
}

func runSender(outputChan chan Metric) {
	log.Printf("Starting Sender.\n")

	for {
		time.Sleep(5000 * time.Millisecond)
		metric := <-outputChan
		log.Printf("Out: '%s'.\n", metric)
	}
}

func runTranformer(inputChan chan Metric, outputChan chan Metric) {
	log.Printf("Starting Transformer.\n")

	for {
		time.Sleep(50 * time.Millisecond)
		metric := <-inputChan
		log.Printf("Transform: '%s'.\n", metric)
		outputChan <- metric
	}
}

func main() {
	log.Println("Groxy rocks!")

	inputChan := make(chan Metric, 1000000)
	outputChan := make(chan Metric, 1000000)

	go runReceiver("127.0.0.1", 2003, inputChan)
	go runTranformer(inputChan, outputChan)
	runSender(outputChan)
}

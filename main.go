package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

var version string = "0.1.0"

type Metric struct {
	Prefix    string `json:"prefix,omitempty"`
	Path      string `json:"path,omitempty"`
	Value     string `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Tenant    string `json:"tenant,omitempty"`
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
	connection.SetReadDeadline(time.Now().Add(5 * time.Second))

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
	metric.Value = metricSlice[1]
	metric.Timestamp = timestamp

	inputChan <- metric

	connection.Close()
}

func runSender(outputChan chan Metric) {
	log.Printf("Starting Sender.\n")

	for {
		time.Sleep(50 * time.Millisecond)
		metric := <-outputChan
		metricString := fmt.Sprintf(
			"%s%s %s %d %s",
			metric.Prefix,
			metric.Path,
			metric.Value,
			metric.Timestamp,
			metric.Tenant,
		)
		log.Printf("Out: '%s'.\n", metricString)
	}
}

func runTransformer(
	inputChan chan Metric,
	outputChan chan Metric,
	tenant string,
	prefix string,
	immutablePrefix string) {
	log.Printf("Starting Transformer.\n")

	for {
		time.Sleep(50 * time.Millisecond)
		metric := <-inputChan
		go transformMetric(metric, outputChan, tenant, prefix, immutablePrefix)
	}
}

func transformMetric(
	metric Metric,
	outputChan chan Metric,
	tenant string,
	prefix string,
	immutablePrefix string) {
	metric.Tenant = tenant

	if strings.HasPrefix(metric.Path, immutablePrefix) == false {
		if prefix != "" {
			metric.Prefix = prefix + "."
		}
	}

	log.Printf("Transformed: '%s'.\n", metric)
	outputChan <- metric
}

func main() {
	log.Println("Groxy rocks!")

	var tenant string
	var prefix string
	var immutablePrefix string

	flag.StringVar(&tenant, "tenant", "", "Graphite project name to store metrics in")
	flag.StringVar(&prefix, "prefix", "", "Prefix to add to any metric")
	flag.StringVar(&immutablePrefix, "immutablePrefix", "", "Do not add prefix to metrics start with")

	flag.Parse()

	inputChan := make(chan Metric, 1000000)
	outputChan := make(chan Metric, 1000000)

	go runReceiver("127.0.0.1", 2003, inputChan)
	go runTransformer(inputChan, outputChan, tenant, prefix, immutablePrefix)
	runSender(outputChan)
}

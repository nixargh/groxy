package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

var version string = "0.1.1"

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

func runSender(host string, port int, outputChan chan Metric, ignoreCert bool) {
	log.Printf("Starting Sender to '%s:%d'.\n", host, port)

	for {
		time.Sleep(5000 * time.Millisecond)

		var metrics [100]string
		// resolve
		addresses, err := net.LookupHost(host)
		if err != nil {
			log.Print(err)
			return
		}
		log.Printf("Addresses: '%s'.", addresses)

		// do for every address
		for a := 0; a < len(addresses); a++ {
			// collect a pack of metrics
			for i := 0; i < len(metrics); i++ {
				select {
				case metric := <-outputChan:
					metrics[i] = fmt.Sprintf(
						"%s%s %s %d %s",
						metric.Prefix,
						metric.Path,
						metric.Value,
						metric.Timestamp,
						metric.Tenant,
					)
				default:
					metrics[i] = ""
				}
			}

			// send the pack
			go sendMetric(metrics, addresses[a], port, ignoreCert)
		}
	}
}

func sendMetric(metrics [100]string, address string, port int, ignoreCert bool) {
	netAddress := fmt.Sprintf("%s:%d", address, port)
	log.Printf("Sending a pack to: '%s'.\n", netAddress)

	timeout, err := time.ParseDuration("5s")

	dialer := &net.Dialer{Timeout: timeout}

	connection, err := tls.DialWithDialer(
		dialer,
		"tcp",
		netAddress,
		&tls.Config{InsecureSkipVerify: ignoreCert})
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i < len(metrics); i++ {
		if metrics[i] != "" {
			log.Printf("Out: '%s'.\n", metrics[i])
			fmt.Fprintf(connection, metrics[i]+"\n")
		}
	}

	connection.Close()
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

	if prefix != "" {
		if immutablePrefix == "" || strings.HasPrefix(metric.Path, immutablePrefix) == false {
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
	var graphiteAddress string
	var graphitePort int
	var address string
	var port int
	var ignoreCert bool

	flag.StringVar(&tenant, "tenant", "", "Graphite project name to store metrics in")
	flag.StringVar(&prefix, "prefix", "", "Prefix to add to any metric")
	flag.StringVar(&immutablePrefix, "immutablePrefix", "", "Do not add prefix to metrics start with")
	flag.StringVar(&graphiteAddress, "graphiteAddress", "", "Graphite server DNS name")
	flag.IntVar(&graphitePort, "graphitePort", 2003, "Graphite server DNS name")
	flag.StringVar(&address, "address", "127.0.0.1", "Proxy bind address")
	flag.IntVar(&port, "port", 2003, "Proxy bind port")
	flag.BoolVar(&ignoreCert, "ignoreCert", false, "Do not verify Graphite server certificate")

	flag.Parse()

	inputChan := make(chan Metric, 1000000)
	outputChan := make(chan Metric, 1000000)

	go runReceiver(address, port, inputChan)
	go runTransformer(inputChan, outputChan, tenant, prefix, immutablePrefix)
	go runSender(graphiteAddress, graphitePort, outputChan, ignoreCert)

	log.Println("Starting a wating loop.")
	for {
		time.Sleep(10000 * time.Millisecond)
	}
}

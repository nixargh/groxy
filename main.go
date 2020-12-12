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
	//	"github.com/pkg/profile"
)

var version string = "0.2.0"

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

	ln, err := net.Listen("tcp4", netAddress)
	if err != nil {
		log.Fatalf("Listener error: '%s'.", err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Reader accept error: '%s'.", err)
			return
		}
		go readMetric(conn, inputChan)
	}
}

func readMetric(connection net.Conn, inputChan chan Metric) {
	connection.SetReadDeadline(time.Now().Add(600 * time.Second))

	scanner := bufio.NewScanner(connection)
	for scanner.Scan() {
		metricString := scanner.Text()

		log.Printf("In: '%s'.", metricString)
		metricSlice := strings.Split(metricString, " ")

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
		if err != nil {
			log.Printf("Timestamp error: '%s'.", err)
			return
		}

		metric.Prefix = ""
		metric.Path = metricSlice[0]
		metric.Value = metricSlice[1]
		metric.Timestamp = timestamp

		inputChan <- metric
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading input:", err)
	}

	//	metricString = strings.TrimSuffix(metricString, "\n")

	connection.Close()
}

func runSender(host string, port int, outputChan chan Metric, TLS bool, ignoreCert bool) {
	log.Printf("Starting Sender to '%s:%d'.\n", host, port)

	for {
		var metrics [100]string
		// resolve
		/*		addresses, err := net.LookupHost(host)
				if err != nil {
					log.Print(err)
					return
				}
				log.Printf("Addresses: '%s'.", addresses)

				// do for every address
				for a := 0; a < len(addresses); a++ {
		*/
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
				time.Sleep(100 * time.Millisecond)
			}
		}

		// send the pack
		go sendMetric(metrics, host, port, TLS, ignoreCert)
		//		}
	}
}

func sendMetric(metrics [100]string, address string, port int, TLS bool, ignoreCert bool) {
	netAddress := fmt.Sprintf("%s:%d", address, port)
	log.Printf("Sending a pack to: '%s'.\n", netAddress)
	errors := 0

	timeout, err := time.ParseDuration("10s")
	dialer := &net.Dialer{Timeout: timeout}

	var connection net.Conn

	if TLS {
		connection, err = tls.DialWithDialer(
			dialer,
			"tcp4",
			netAddress,
			&tls.Config{InsecureSkipVerify: ignoreCert})
	} else {
		connection, err = dialer.Dial("tcp4", netAddress)
	}
	if err != nil {
		log.Printf("Dialer error: '%s'.", err)
		return
	}
	defer connection.Close()
	connection.SetReadDeadline(time.Now().Add(30 * time.Second))

	log.Printf("Connection remote address: '%s'.\n", connection.RemoteAddr())

	for i := 0; i < len(metrics); i++ {
		if metrics[i] != "" {
			dataLength, err := connection.Write([]byte(metrics[i] + "\n"))
			if err != nil {
				log.Printf("Connection write error: '%s'.", err)
				errors++
			} else {
				log.Printf("[%d] Out (%d bytes): '%s'.\n", i, dataLength, metrics[i])
			}
		}
	}
	log.Printf("The pack is sent with %d error(s).\n", errors)
}

func runTransformer(
	inputChan chan Metric,
	outputChan chan Metric,
	tenant string,
	prefix string,
	immutablePrefix string) {
	log.Printf("Starting Transformer.\n")

	for {
		select {
		case metric := <-inputChan:
			go transformMetric(metric, outputChan, tenant, prefix, immutablePrefix)
		default:
			time.Sleep(100 * time.Millisecond)
		}
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
	//	defer profile.Start().Stop()

	log.Printf("Groxy rocks! (v%s)\n", version)

	var tenant string
	var prefix string
	var immutablePrefix string
	var graphiteAddress string
	var graphitePort int
	var address string
	var port int
	var TLS bool
	var ignoreCert bool

	flag.StringVar(&tenant, "tenant", "", "Graphite project name to store metrics in")
	flag.StringVar(&prefix, "prefix", "", "Prefix to add to any metric")
	flag.StringVar(&immutablePrefix, "immutablePrefix", "", "Do not add prefix to metrics start with")
	flag.StringVar(&graphiteAddress, "graphiteAddress", "", "Graphite server DNS name")
	flag.IntVar(&graphitePort, "graphitePort", 2003, "Graphite server DNS name")
	flag.StringVar(&address, "address", "127.0.0.1", "Proxy bind address")
	flag.IntVar(&port, "port", 2003, "Proxy bind port")
	flag.BoolVar(&TLS, "TLS", false, "Use TLS encrypted connection")
	flag.BoolVar(&ignoreCert, "ignoreCert", false, "Do not verify Graphite server certificate")

	flag.Parse()

	inputChan := make(chan Metric, 1000000)
	outputChan := make(chan Metric, 1000000)

	go runReceiver(address, port, inputChan)
	go runTransformer(inputChan, outputChan, tenant, prefix, immutablePrefix)
	go runSender(graphiteAddress, graphitePort, outputChan, TLS, ignoreCert)

	log.Println("Starting a waiting loop.")
	for {
		time.Sleep(20 * time.Second)
	}
}

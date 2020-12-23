package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	//	"github.com/pkg/profile"
)

var version string = "0.4.1"

type Metric struct {
	Prefix    string `json:"prefix,omitempty"`
	Path      string `json:"path,omitempty"`
	Value     string `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Tenant    string `json:"tenant,omitempty"`
}

type State struct {
	In              int64 `json:"in"`
	Out             int64 `json:"out"`
	Transformed     int64 `json:"transformed"`
	Bad             int64 `json:"bad"`
	SendError       int64 `json:"send_error"`
	InMpm           int64 `json:"in_mpm"`
	OutMpm          int64 `json:"out_mpm"`
	BadMpm          int64 `json:"bad_mpm"`
	TransformedMpm  int64 `json:"transformed_mpm"`
	Connection      int64 `json:"connection"`
	ConnectionAlive int64 `json:"connection_alive"`
	ConnectionError int64 `json:"connection_error"`
	OutQueue        int64 `json:"out_queue"`
	TransformQueue  int64 `json:"transform_queue"`
}

var state State

func runRouter(address string, port int) {
	netAddress := fmt.Sprintf("%s:%d", address, port)
	log.Printf("Starting Stats server at: '%s'.\n", netAddress)

	// Create HTTP router
	router := mux.NewRouter()
	router.HandleFunc("/stats", getState).Methods("GET")

	log.Fatal(http.ListenAndServe(netAddress, router))
}

func getState(w http.ResponseWriter, req *http.Request) {
	json.NewEncoder(w).Encode(state)
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
			state.Bad++
			connection.Close()
			return
		}

		timestamp, err := strconv.ParseInt(metricSlice[2], 10, 64)
		if err != nil {
			log.Printf("Timestamp error: '%s'.", err)
			state.Bad++
			return
		}

		metric.Prefix = ""
		metric.Path = metricSlice[0]
		metric.Value = metricSlice[1]
		metric.Timestamp = timestamp

		inputChan <- metric
		state.In++
		state.TransformQueue++
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading input:", err)
	}

	connection.Close()
}

func runSender(host string, port int, outputChan chan Metric, TLS bool, ignoreCert bool) {
	log.Printf("Starting Sender to '%s:%d'.\n", host, port)

	for {
		var connections [4]net.Conn

		// Create connections
		for n := 0; n < len(connections); {
			// Create a new one
			connection, err := createConnection(host, port, TLS, ignoreCert)
			if err != nil {
				log.Printf("Can't create connection: '%s'.", err)
				state.ConnectionError++
			} else {
				connections[n] = connection
				n++
				state.Connection++
				state.ConnectionAlive++
			}
		}

		// Send some packs of metrics
		for pack := 0; pack < 100; pack++ {
			metrics := make([]Metric, 0)

			n := pack % len(connections)

			// collect a pack of metrics
			for i := 0; i < 100; i++ {
				select {
				case metric := <-outputChan:
					metrics = append(metrics, metric)
				default:
					time.Sleep(100 * time.Millisecond)
				}
			}

			// send the pack
			if len(metrics) > 0 {
				go sendMetric(metrics, connections[n], outputChan)
			}
		}

		// Close connections
		for n := 0; n < len(connections); n++ {
			// Close the old one
			connections[n].Close()
			state.ConnectionAlive--
		}
	}
}

func sendMetric(metrics []Metric, connection net.Conn, outputChan chan Metric) {
	sent := 0
	returned := 0
	connectionAlive := true

	for i := 0; i < len(metrics); i++ {
		metricString := fmt.Sprintf(
			"%s%s %s %d %s",
			metrics[i].Prefix,
			metrics[i].Path,
			metrics[i].Value,
			metrics[i].Timestamp,
			metrics[i].Tenant,
		)

		// If connection is dead - just return metrics to outputChan
		if connectionAlive {
			dataLength, err := connection.Write([]byte(metricString + "\n"))
			if err != nil {
				log.Printf("Connection write error: '%s'.", err)
				state.SendError++

				connectionAlive = false

				// Here we must return metric to ResendQueue
				outputChan <- metrics[i]
				returned++
			} else {
				log.Printf("[%d] Out (%d bytes): '%s'.\n", i, dataLength, metricString)
				sent++
				state.Out++
				state.OutQueue--
			}
		} else {
			outputChan <- metrics[i]
			returned++
		}
	}
	log.Printf("The pack is finished: sent=%d; returned=%d.\n", sent, returned)
}

func createConnection(address string, port int, TLS bool, ignoreCert bool) (net.Conn, error) {
	netAddress := fmt.Sprintf("%s:%d", address, port)
	log.Printf("Connecting to: '%s'.\n", netAddress)

	timeout, _ := time.ParseDuration("10s")
	dialer := &net.Dialer{Timeout: timeout}

	var connection net.Conn
	var err error

	if TLS {
		connection, err = tls.DialWithDialer(
			dialer,
			"tcp4",
			netAddress,
			&tls.Config{
				InsecureSkipVerify:          ignoreCert,
				MinVersion:                  tls.VersionTLS12,
				MaxVersion:                  tls.VersionTLS12,
				DynamicRecordSizingDisabled: false,
			},
		)
	} else {
		connection, err = dialer.Dial("tcp4", netAddress)
	}
	if err != nil {
		log.Printf("Dialer error: '%s'.", err)
		return connection, err
	}
	connection.SetDeadline(time.Now().Add(1200 * time.Second))

	log.Printf("Connection remote address: '%s'.\n", connection.RemoteAddr())
	return connection, err
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

	// Update state
	state.Transformed++
	state.TransformQueue--
	state.OutQueue++
}

func main() {
	//	defer profile.Start().Stop()

	log.Printf("Groxy rocks! (v%s)\n", version)

	var tenant string
	var prefix string
	var immutablePrefix string
	var graphiteAddress string
	var graphitePort int
	var statsAddress string
	var statsPort int
	var address string
	var port int
	var TLS bool
	var ignoreCert bool

	flag.StringVar(&tenant, "tenant", "", "Graphite project name to store metrics in")
	flag.StringVar(&prefix, "prefix", "", "Prefix to add to any metric")
	flag.StringVar(&immutablePrefix, "immutablePrefix", "", "Do not add prefix to metrics start with")
	flag.StringVar(&graphiteAddress, "graphiteAddress", "", "Graphite server DNS name")
	flag.IntVar(&graphitePort, "graphitePort", 2003, "Graphite server DNS name")
	flag.StringVar(&statsAddress, "statsAddress", "127.0.0.1", "Proxy stats bind address")
	flag.IntVar(&statsPort, "statsPort", 3003, "Proxy stats port")
	flag.StringVar(&address, "address", "127.0.0.1", "Proxy bind address")
	flag.IntVar(&port, "port", 2003, "Proxy bind port")
	flag.BoolVar(&TLS, "TLS", false, "Use TLS encrypted connection")
	flag.BoolVar(&ignoreCert, "ignoreCert", false, "Do not verify Graphite server certificate")

	flag.Parse()

	inputChan := make(chan Metric, 10000000)
	outputChan := make(chan Metric, 10000000)

	go runReceiver(address, port, inputChan)
	go runTransformer(inputChan, outputChan, tenant, prefix, immutablePrefix)
	go runSender(graphiteAddress, graphitePort, outputChan, TLS, ignoreCert)
	go runRouter(statsAddress, statsPort)

	log.Println("Starting a waiting loop.")
	for {
		in := state.In
		out := state.Out
		transformed := state.Transformed
		bad := state.Bad

		time.Sleep(60 * time.Second)

		// Calculate MPMs
		state.InMpm = state.In - in
		state.OutMpm = state.Out - out
		state.TransformedMpm = state.Transformed - transformed
		state.BadMpm = state.Bad - bad

		log.Printf("State: %s.", state)
	}
}

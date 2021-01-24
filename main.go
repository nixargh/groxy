package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	//	"github.com/pkg/profile"
)

var version string = "0.7.0"

var clog, slog, rlog, tlog, stlog *log.Entry

type Metric struct {
	Prefix    string `json:"prefix,omitempty"`
	Path      string `json:"path,omitempty"`
	Value     string `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Tenant    string `json:"tenant,omitempty"`
}

type State struct {
	Version         string `json:"version"`
	In              int64  `json:"in"`
	Out             int64  `json:"out"`
	Transformed     int64  `json:"transformed"`
	Bad             int64  `json:"bad"`
	SendError       int64  `json:"send_error"`
	InMpm           int64  `json:"in_mpm"`
	OutMpm          int64  `json:"out_mpm"`
	BadMpm          int64  `json:"bad_mpm"`
	TransformedMpm  int64  `json:"transformed_mpm"`
	Connection      int64  `json:"connection"`
	ConnectionAlive int64  `json:"connection_alive"`
	ConnectionError int64  `json:"connection_error"`
	OutQueue        int64  `json:"out_queue"`
	TransformQueue  int64  `json:"transform_queue"`
}

var state State

func runRouter(address string, port int) {
	stlog = clog.WithFields(log.Fields{
		"address": address,
		"port":    port,
		"thread":  "stats",
	})
	netAddress := fmt.Sprintf("%s:%d", address, port)
	stlog.Info("Starting Stats server.")

	// Create HTTP router
	router := mux.NewRouter()
	router.HandleFunc("/stats", getState).Methods("GET")

	stlog.Fatal(http.ListenAndServe(netAddress, router))
}

func getState(w http.ResponseWriter, req *http.Request) {
	json.NewEncoder(w).Encode(state)
}

func runReceiver(address string, port int, inputChan chan Metric) {
	rlog = clog.WithFields(log.Fields{
		"address": address,
		"port":    port,
		"thread":  "receiver",
	})
	netAddress := fmt.Sprintf("%s:%d", address, port)

	rlog.Info("Starting Receiver.")

	ln, err := net.Listen("tcp4", netAddress)
	if err != nil {
		rlog.WithFields(log.Fields{"error": err}).Fatal("Listener error.")
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			rlog.WithFields(log.Fields{"error": err}).Fatal("Reader accept error.")
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

		rlog.WithFields(log.Fields{"metric": metricString}).Debug("Metric received.")
		metricSlice := strings.Split(metricString, " ")

		metric := Metric{}

		switch len(metricSlice) {
		case 3:
			metric.Tenant = ""
		case 4:
			metric.Tenant = metricSlice[3]
		default:
			rlog.WithFields(log.Fields{"metric": metricString}).Error("Bad metric.")
			state.Bad++
			connection.Close()
			return
		}

		timestamp, err := strconv.ParseInt(metricSlice[2], 10, 64)
		if err != nil {
			rlog.WithFields(log.Fields{
				"metric":          metricString,
				"timestampString": metricSlice[2],
				"error":           err,
			}).Error("Timestamp error.")
			state.Bad++
			return
		}

		metric.Prefix = ""
		metric.Path = metricSlice[0]
		metric.Value = metricSlice[1]
		metric.Timestamp = timestamp

		inputChan <- metric
		state.In++
	}
	if err := scanner.Err(); err != nil {
		rlog.WithFields(log.Fields{"error": err}).Error("Error reading input.")
	}

	connection.Close()
}

func runSender(host string, port int, outputChan chan Metric, TLS bool, ignoreCert bool) {
	slog = clog.WithFields(log.Fields{
		"host":       host,
		"port":       port,
		"thread":     "sender",
		"tls":        TLS,
		"ignoreCert": ignoreCert,
	})
	slog.Info("Starting Sender.")

	for {
		// Create output connection
		connection, err := createConnection(host, port, TLS, ignoreCert)
		if err != nil {
			slog.WithFields(log.Fields{"error": err}).Fatal("Can't create connection.")
			state.ConnectionError++
			time.Sleep(5 * time.Second)
			continue
		} else {
			state.Connection++
			state.ConnectionAlive++
		}

		// collect a pack of metrics
		var metrics [1000]Metric

		for i := 0; i < len(metrics); i++ {
			select {
			case metric := <-outputChan:
				metrics[i] = metric
			default:
				metrics[i] = Metric{}
				time.Sleep(100 * time.Millisecond)
			}
		}

		// send the pack
		if len(metrics) > 0 {
			go sendMetric(&metrics, connection, outputChan)
		}
	}
}

func sendMetric(metrics *[1000]Metric, connection net.Conn, outputChan chan Metric) {
	sent := 0
	returned := 0
	connectionAlive := true

	emptyMetric := Metric{}

	for i := 0; i < len(metrics); i++ {
		if metrics[i] != emptyMetric {
			// If connection is dead - just return metrics to outputChan
			if connectionAlive == false {
				outputChan <- metrics[i]
				returned++
				continue
			}

			metricString := fmt.Sprintf(
				"%s%s %s %d %s",
				metrics[i].Prefix,
				metrics[i].Path,
				metrics[i].Value,
				metrics[i].Timestamp,
				metrics[i].Tenant,
			)

			dataLength, err := connection.Write([]byte(metricString + "\n"))
			if err != nil {
				slog.WithFields(log.Fields{"error": err}).Error("Connection write error.")
				state.SendError++

				connectionAlive = false

				// Here we must return metric to ResendQueue
				outputChan <- metrics[i]
				returned++
			} else {
				slog.WithFields(log.Fields{
					"bytes":  dataLength,
					"metric": metricString,
					"number": i,
				}).Debug("Metric sent.")
				sent++
				state.Out++
			}
		}
	}

	connection.Close()
	state.ConnectionAlive--
	slog.WithFields(log.Fields{
		"sent":     sent,
		"returned": returned,
	}).Info("The pack is finished.")
}

func createConnection(host string, port int, TLS bool, ignoreCert bool) (net.Conn, error) {
	netAddress := fmt.Sprintf("%s:%d", host, port)
	slog.Debug("Connecting to Graphite.")

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
		slog.WithFields(log.Fields{"error": err}).Error("Dialer error.")
		return connection, err
	}
	connection.SetDeadline(time.Now().Add(600 * time.Second))

	slog.WithFields(log.Fields{
		"remoteAddress": connection.RemoteAddr(),
	}).Info("Connection to Graphite established.")
	return connection, err
}

func runTransformer(
	inputChan chan Metric,
	outputChan chan Metric,
	tenant string,
	prefix string,
	immutablePrefix []string) {

	tlog = clog.WithFields(log.Fields{
		"thread":          "transformer",
		"tenant":          tenant,
		"prefix":          prefix,
		"immutablePrefix": immutablePrefix,
	})
	tlog.Info("Starting Transformer.")

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
	immutablePrefix []string) {
	metric.Tenant = tenant

	if prefix != "" {
		mutate := true
		for i := range immutablePrefix {
			if strings.HasPrefix(metric.Path, immutablePrefix[i]) == true {
				mutate = false
			}
		}
		if mutate {
			metric.Prefix = prefix + "."
		}
	}

	tlog.WithFields(log.Fields{"metric": metric}).Debug("Metric transformed.")
	outputChan <- metric

	// Update state
	state.Transformed++
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func updateQueue(sleepSeconds int) {
	clog.WithFields(log.Fields{"sleepSeconds": sleepSeconds}).Info("Starting queues update loop.")
	for {
		time.Sleep(time.Duration(sleepSeconds) * time.Second)

		state.TransformQueue = state.In - state.Transformed
		state.OutQueue = state.In - state.Out
	}
}

func main() {
	//	defer profile.Start().Stop()

	var tenant string
	var prefix string
	var immutablePrefix arrayFlags
	var graphiteAddress string
	var graphitePort int
	var statsAddress string
	var statsPort int
	var address string
	var port int
	var TLS bool
	var ignoreCert bool
	var jsonLog bool
	var debug bool
	var logCaller bool

	flag.StringVar(&tenant, "tenant", "", "Graphite project name to store metrics in")
	flag.StringVar(&prefix, "prefix", "", "Prefix to add to any metric")
	flag.Var(&immutablePrefix, "immutablePrefix", "Do not add prefix to metrics start with. Could be set many times")
	flag.StringVar(&graphiteAddress, "graphiteAddress", "", "Graphite server DNS name")
	flag.IntVar(&graphitePort, "graphitePort", 2003, "Graphite server DNS name")
	flag.StringVar(&statsAddress, "statsAddress", "127.0.0.1", "Proxy stats bind address")
	flag.IntVar(&statsPort, "statsPort", 3003, "Proxy stats port")
	flag.StringVar(&address, "address", "127.0.0.1", "Proxy bind address")
	flag.IntVar(&port, "port", 2003, "Proxy bind port")
	flag.BoolVar(&TLS, "TLS", false, "Use TLS encrypted connection")
	flag.BoolVar(&ignoreCert, "ignoreCert", false, "Do not verify Graphite server certificate")
	flag.BoolVar(&jsonLog, "jsonLog", false, "Log in JSON format")
	flag.BoolVar(&debug, "debug", false, "Log debug messages")
	flag.BoolVar(&logCaller, "logCaller", false, "Log message caller (file and line number)")

	flag.Parse()

	// Setup logging
	log.SetOutput(os.Stdout)

	if jsonLog == true {
		log.SetFormatter(&log.JSONFormatter{})
	} else {
		log.SetFormatter(&log.TextFormatter{
			//		FullTimestamp: true,
		})
	}

	if debug == true {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.SetReportCaller(logCaller)

	clog = log.WithFields(log.Fields{
		"pid":     os.Getpid(),
		"thread":  "main",
		"version": version,
	})

	state.Version = version
	clog.Info("Groxy rocks!")

	// Validate variables
	if graphiteAddress == "" {
		clog.Fatal("You must set '-graphiteAddress'.")
	}

	inputChan := make(chan Metric, 10000000)
	outputChan := make(chan Metric, 10000000)

	go runReceiver(address, port, inputChan)
	go runTransformer(inputChan, outputChan, tenant, prefix, immutablePrefix)
	go runSender(graphiteAddress, graphitePort, outputChan, TLS, ignoreCert)
	go runRouter(statsAddress, statsPort)
	go updateQueue(1)

	sleepSeconds := 60
	clog.WithFields(log.Fields{"sleepSeconds": sleepSeconds}).Info("Starting a waiting loop.")
	for {
		in := state.In
		out := state.Out
		transformed := state.Transformed
		bad := state.Bad

		time.Sleep(time.Duration(sleepSeconds) * time.Second)

		// Calculate MPMs
		state.InMpm = state.In - in
		state.OutMpm = state.Out - out
		state.TransformedMpm = state.Transformed - transformed
		state.BadMpm = state.Bad - bad

		clog.WithFields(log.Fields{"state": state}).Info("Dumping state.")
	}
}

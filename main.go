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
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	//	"github.com/pkg/profile"
)

var version string = "0.10.0"

var clog, slog, rlog, tlog, stlog *log.Entry
var hostname string

type Metric struct {
	Prefix    string `json:"prefix,omitempty"`
	Path      string `json:"path,omitempty"`
	Value     string `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Tenant    string `json:"tenant,omitempty"`
}

type State struct {
	Version            string `json:"version"`
	In                 int64  `json:"in"`
	Bad                int64  `json:"bad"`
	Transformed        int64  `json:"transformed"`
	Out                int64  `json:"out"`
	ReadError          int64  `json:"read_error"`
	SendError          int64  `json:"send_error"`
	InMpm              int64  `json:"in_mpm"`
	BadMpm             int64  `json:"bad_mpm"`
	TransformedMpm     int64  `json:"transformed_mpm"`
	OutMpm             int64  `json:"out_mpm"`
	Connection         int64  `json:"connection"`
	ConnectionAlive    int64  `json:"connection_alive"`
	ConnectionError    int64  `json:"connection_error"`
	TransformQueue     int64  `json:"transform_queue"`
	OutQueue           int64  `json:"out_queue"`
	Queue              int64  `json:"queue"`
	NegativeQueueError int64  `json:"negative_queue_error"`
	PacksOverflewError int64  `json:"packs_overflew_error"`
}

var emptyMetric *Metric
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

func runReceiver(address string, port int, inputChan chan *Metric) {
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

func readMetric(connection net.Conn, inputChan chan *Metric) {
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
			atomic.AddInt64(&state.Bad, 1)
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
			atomic.AddInt64(&state.Bad, 1)
			return
		}

		metric.Prefix = ""
		metric.Path = metricSlice[0]
		metric.Value = metricSlice[1]
		metric.Timestamp = timestamp

		inputChan <- &metric
		atomic.AddInt64(&state.In, 1)
	}
	if err := scanner.Err(); err != nil {
		rlog.WithFields(log.Fields{"error": err}).Error("Error reading input.")
		atomic.AddInt64(&state.ReadError, 1)
	}

	connection.Close()
}

func limitRefresher(limitPerSec *int) {
	savedLimit := *limitPerSec
	slog.WithFields(log.Fields{"limitPerSec": savedLimit}).Info("Starting Packs Limit Refresher.")

	for {
		time.Sleep(1 * time.Second)
		*limitPerSec = savedLimit
	}
}

func runSender(host string, port int, outputChan chan *Metric, TLS bool, ignoreCert bool, limitPerSec int) {
	slog = clog.WithFields(log.Fields{
		"host":       host,
		"port":       port,
		"thread":     "sender",
		"tls":        TLS,
		"ignoreCert": ignoreCert,
	})
	slog.Info("Starting Sender.")

	// Start limit refresher thread
	go limitRefresher(&limitPerSec)

	for {
		curLimit := limitPerSec
		slog.WithFields(log.Fields{"limit": curLimit}).Debug("Current limit.")
		if curLimit > 0 {
			// Create output connection
			connection, err := createConnection(host, port, TLS, ignoreCert)
			if err != nil {
				slog.WithFields(log.Fields{"error": err}).Fatal("Can't create connection.")
				atomic.AddInt64(&state.ConnectionError, 1)
				time.Sleep(5 * time.Second)
				continue
			} else {
				atomic.AddInt64(&state.Connection, 1)
				atomic.AddInt64(&state.ConnectionAlive, 1)
			}

			// collect a pack of metrics
			var metrics [1000]*Metric

			for i := 0; i < len(metrics); i++ {
				select {
				case metric := <-outputChan:
					metrics[i] = metric
				default:
					metrics[i] = emptyMetric
					time.Sleep(100 * time.Millisecond)
				}
			}

			// send the pack
			if len(metrics) > 0 {
				go sendMetric(&metrics, connection, outputChan)
				limitPerSec--
			}
		} else {
			slog.Warning("Limit of metric packs per second overflew.")
			atomic.AddInt64(&state.PacksOverflewError, 1)
			time.Sleep(1 * time.Second)
		}
	}
}

func sendMetric(metrics *[1000]*Metric, connection net.Conn, outputChan chan *Metric) {
	sent := 0
	returned := 0
	connectionAlive := true

	for i := 0; i < len(metrics); i++ {
		if metrics[i] == emptyMetric {
			continue
		}

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
			atomic.AddInt64(&state.SendError, 1)

			connectionAlive = false

			// Here we must return metric to out outputChan
			outputChan <- metrics[i]
			returned++
		} else {
			slog.WithFields(log.Fields{
				"bytes":  dataLength,
				"metric": metricString,
				"number": i,
			}).Debug("Metric sent.")
			sent++
			atomic.AddInt64(&state.Out, 1)
		}
	}

	connection.Close()
	atomic.AddInt64(&state.ConnectionAlive, -1)
	slog.WithFields(log.Fields{
		"sent":     sent,
		"returned": returned,
	}).Info("Pack is finished.")
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
	inputChan chan *Metric,
	outputChan chan *Metric,
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
	metric *Metric,
	outputChan chan *Metric,
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
	atomic.AddInt64(&state.Transformed, 1)
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

		state.TransformQueue = atomic.LoadInt64(&state.In) - atomic.LoadInt64(&state.Transformed)
		state.OutQueue = atomic.LoadInt64(&state.Transformed) - atomic.LoadInt64(&state.Out)
		state.Queue = atomic.LoadInt64(&state.In) - atomic.LoadInt64(&state.Out)

		if state.Queue < 0 {
			clog.WithFields(log.Fields{"queue": state.Queue}).Error("Queue value is negative.")
			atomic.AddInt64(&state.NegativeQueueError, 1)
		}
	}
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		clog.WithFields(log.Fields{"error": err}).Fatal("Can't get hostname.")
	}

	hostnameSplit := strings.Split(hostname, ".")
	hostname = hostnameSplit[0]
	clog.WithFields(log.Fields{"hostname": hostname}).Info("Got hostname.")
	return hostname
}

func sendStateMetrics(instance string, inputChan chan *Metric) {
	clog.Info("Sending state metrics.")
	stateSnapshot := state
	timestamp := time.Now().Unix()

	value := reflect.ValueOf(stateSnapshot)
	for i := 0; i < value.NumField(); i++ {
		structfield := value.Type().Field(i)
		name := structfield.Name
		value := value.Field(i)
		tag := structfield.Tag.Get("json")

		metric := Metric{}

		if name == "Version" {
			metric.Value = strings.Replace(value.String(), ".", "", 2)
		} else {
			metric.Value = fmt.Sprintf("%d", value.Int())
		}

		clog.WithFields(log.Fields{
			"name":  name,
			"tag":   tag,
			"value": metric.Value,
		}).Debug("State field.")

		metric.Path = fmt.Sprintf("%s.groxy.%s.state.%s", hostname, instance, tag)
		metric.Timestamp = timestamp

		// Pass to transformation
		inputChan <- &metric
		atomic.AddInt64(&state.In, 1)
	}
}

func main() {
	//	defer profile.Start().Stop()

	var instance string
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
	var limitPerSec int

	flag.StringVar(&instance, "instance", "default", "Groxy instance name (for log and metrics)")
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
	flag.IntVar(&limitPerSec, "limitPerSec", 10, "Maximum number of metric packs (<=1000 metrics per pack) sent per second")

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
		"instance": instance,
		"pid":      os.Getpid(),
		"thread":   "main",
		"version":  version,
	})

	state.Version = version
	clog.Info("Groxy rocks!")

	// Validate variables
	if graphiteAddress == "" {
		clog.Fatal("You must set '-graphiteAddress'.")
	}

	inputChan := make(chan *Metric, 10000000)
	outputChan := make(chan *Metric, 10000000)

	hostname = getHostname()

	go runReceiver(address, port, inputChan)
	go runTransformer(inputChan, outputChan, tenant, prefix, immutablePrefix)
	go runSender(graphiteAddress, graphitePort, outputChan, TLS, ignoreCert, limitPerSec)
	go runRouter(statsAddress, statsPort)
	go updateQueue(1)

	sleepSeconds := 60
	clog.WithFields(log.Fields{"sleepSeconds": sleepSeconds}).Info("Starting a waiting loop.")
	for {
		in := atomic.LoadInt64(&state.In)
		out := atomic.LoadInt64(&state.Out)
		transformed := atomic.LoadInt64(&state.Transformed)
		bad := atomic.LoadInt64(&state.Bad)

		time.Sleep(time.Duration(sleepSeconds) * time.Second)

		// Calculate MPMs
		state.InMpm = atomic.LoadInt64(&state.In) - in
		state.OutMpm = atomic.LoadInt64(&state.Out) - out
		state.TransformedMpm = atomic.LoadInt64(&state.Transformed) - transformed
		state.BadMpm = atomic.LoadInt64(&state.Bad) - bad

		clog.WithFields(log.Fields{"state": state}).Info("Dumping state.")

		sendStateMetrics(instance, inputChan)
	}
}

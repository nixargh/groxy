package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync/atomic"
	"time"
	//	"github.com/pkg/profile"
)

var version string = "1.4.0"

var clog, slog, rlog, tlog, stlog *log.Entry

type Metric struct {
	Prefix    string `json:"prefix,omitempty"`
	Path      string `json:"path,omitempty"`
	Value     string `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Tenant    string `json:"tenant,omitempty"`
}

var emptyMetric *Metric
var state State

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
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
	var systemTenant string
	var systemPrefix string
	var hostname string
	var compressedOutput bool
	var compressedInput bool

	flag.StringVar(&instance, "instance", "default", "Groxy instance name (for log and metrics)")
	flag.StringVar(&tenant, "tenant", "", "Graphite project name to store metrics in")
	flag.StringVar(&prefix, "prefix", "", "Prefix to add to any metric")
	flag.Var(&immutablePrefix, "immutablePrefix", "Do not add prefix to metrics start with. Could be set many times")
	flag.StringVar(&graphiteAddress, "graphiteAddress", "", "Graphite server DNS name")
	flag.IntVar(&graphitePort, "graphitePort", 2003, "Graphite server TCP port")
	flag.StringVar(&statsAddress, "statsAddress", "127.0.0.1", "Proxy stats bind address")
	flag.IntVar(&statsPort, "statsPort", 3003, "Proxy stats port")
	flag.StringVar(&address, "address", "127.0.0.1", "Proxy bind address")
	flag.IntVar(&port, "port", 2003, "Proxy bind port")
	flag.BoolVar(&TLS, "TLS", false, "Use TLS encrypted connection")
	flag.BoolVar(&ignoreCert, "ignoreCert", false, "Do not verify Graphite server certificate")
	flag.BoolVar(&jsonLog, "jsonLog", false, "Log in JSON format")
	flag.BoolVar(&debug, "debug", false, "Log debug messages")
	flag.BoolVar(&logCaller, "logCaller", false, "Log message caller (file and line number)")
	flag.BoolVar(&compressedOutput, "compressedOutput", false, "Compress messages when sending")
	flag.BoolVar(&compressedInput, "compressedInput", false, "Read compressed messages when receiving")
	flag.IntVar(&limitPerSec, "limitPerSec", 2, "Maximum number of metric packs (<=10000 metrics per pack) sent per second")
	flag.StringVar(&systemTenant, "systemTenant", "", "Graphite project name to store SELF metrics in. By default is equal to 'tenant'")
	flag.StringVar(&systemPrefix, "systemPrefix", "", "Prefix to add to any SELF metric. By default is equal to 'prefix'")
	flag.StringVar(&hostname, "hostname", "", "Hostname of a computer running Groxy")

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

	if systemTenant == "" {
		systemTenant = tenant
	}

	if systemPrefix == "" {
		systemPrefix = prefix
	}

	if hostname == "" {
		hostname = getHostname()
	}

	inputChan := make(chan *Metric, 10000000)
	outputChan := make(chan *Metric, 10000000)

	go runReceiver(address, port, inputChan, compressedInput)
	go runTransformer(inputChan, outputChan, tenant, prefix, immutablePrefix)
	go runSender(graphiteAddress, graphitePort, outputChan, TLS, ignoreCert, limitPerSec, compressedOutput)
	go runRouter(statsAddress, statsPort)
	go updateQueue(1)

	sleepSeconds := 60
	clog.WithFields(log.Fields{"sleepSeconds": sleepSeconds}).Info("Starting a waiting loop.")
	for {
		in := atomic.LoadInt64(&state.In)
		out := atomic.LoadInt64(&state.Out)
		transformed := atomic.LoadInt64(&state.Transformed)
		bad := atomic.LoadInt64(&state.Bad)
		out_bytes := atomic.LoadInt64(&state.OutBytes)

		time.Sleep(time.Duration(sleepSeconds) * time.Second)

		// Calculate MPMs
		state.InMpm = atomic.LoadInt64(&state.In) - in
		state.OutMpm = atomic.LoadInt64(&state.Out) - out
		state.TransformedMpm = atomic.LoadInt64(&state.Transformed) - transformed
		state.BadMpm = atomic.LoadInt64(&state.Bad) - bad
		state.OutBpm = atomic.LoadInt64(&state.OutBytes) - out_bytes

		clog.WithFields(log.Fields{"state": state}).Info("Dumping state.")

		sendStateMetrics(instance, hostname, systemTenant, systemPrefix, inputChan)
	}
}

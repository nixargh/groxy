package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"strconv"

	log "github.com/sirupsen/logrus"
	//	"github.com/pkg/profile"
)

var version string = "2.2.0"

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
	var forceTenant bool
	var prefix string
	var immutablePrefix arrayFlags
	var graphiteAddress arrayFlags
	var statsAddress string
	var statsPort int
	var address string
	var port int
	var tlsOutput bool
	var tlsInput bool
	var mtlsOutput bool
	var mtlsInput bool
	var tlsInputCaCert string
	var tlsInputCert string
	var tlsInputKey string
	var tlsOutputCaCert string
	var tlsOutputCert string
	var tlsOutputKey string
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
	var showVersion bool

	flag.StringVar(&instance, "instance", "default", "Groxy instance name (for log and metrics)")
	flag.StringVar(&tenant, "tenant", "", "Graphite project name to store metrics in")
	flag.BoolVar(&forceTenant, "forceTenant", false, "Overwrite metrics tenant even if it is already set")
	flag.StringVar(&prefix, "prefix", "", "Prefix to add to any metric")
	flag.Var(&immutablePrefix, "immutablePrefix", "Do not add prefix to metrics start with. Could be set many times")
	flag.Var(&graphiteAddress, "graphiteAddress", "Graphite server DNS name : Graphite server TCP port")
	flag.StringVar(&statsAddress, "statsAddress", "127.0.0.1", "Proxy stats bind address")
	flag.IntVar(&statsPort, "statsPort", 3003, "Proxy stats port")
	flag.StringVar(&address, "address", "127.0.0.1", "Proxy bind address")
	flag.IntVar(&port, "port", 2003, "Proxy bind port")
	flag.BoolVar(&tlsOutput, "tlsOutput", false, "Send metrics via TLS encrypted connection")
	flag.BoolVar(&tlsInput, "tlsInput", false, "Receive metrics via TLS encrypted connection")
	flag.BoolVar(&mtlsOutput, "mtlsOutput", false, "Send metrics via mutual TLS encrypted connection")
	flag.BoolVar(&mtlsInput, "mtlsInput", false, "Receive metrics via mutual TLS encrypted connection")
	flag.StringVar(&tlsInputCaCert, "tlsInputCaCert", "ca.crt", "TLS CA certificate for receiver")
	flag.StringVar(&tlsInputCert, "tlsInputCert", "groxy.crt", "TLS certificate for receiver")
	flag.StringVar(&tlsInputKey, "tlsInputKey", "groxy.key", "TLS key for receiver")
	flag.StringVar(&tlsOutputCaCert, "tlsOutputCaCert", "ca.crt", "TLS CA certificate for sender")
	flag.StringVar(&tlsOutputCert, "tlsOutputCert", "groxy.crt", "TLS certificate for sender")
	flag.StringVar(&tlsOutputKey, "tlsOutputKey", "groxy.key", "TLS key for sender")
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
	flag.BoolVar(&showVersion, "version", false, "Groxy version")

	flag.Parse()

	// Setup logging
	log.SetOutput(os.Stdout)

	if showVersion {
		fmt.Println(version)
		os.Exit(1)
	}

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
	if graphiteAddress == nil {
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

	go runReceiver(
		address,
		port,
		inputChan,
		tlsInput,
		mtlsInput,
		tlsInputCaCert,
		tlsInputCert,
		tlsInputKey,
		ignoreCert,
		compressedInput)

	for i := range graphiteAddress {
		outputChan := make(chan *Metric, 10000000)

		ga := strings.Split(graphiteAddress[i], ":")
		host := ga[0]
		port, err := strconv.Atoi(ga[1])
		if err != nil {
			clog.Fatal("Can't get integer port from '-graphiteAddress'.")
		}

		go runSender(
			host,
			port,
			outputChan,
			tlsOutput,
			mtlsOutput,
			tlsOutputCaCert,
			tlsOutputCert,
			tlsOutputKey,
			ignoreCert,
			limitPerSec,
			compressedOutput)

	}

	go runTransformer(inputChan, outputChan, tenant, forceTenant, prefix, immutablePrefix)
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

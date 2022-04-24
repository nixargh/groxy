package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
	//	"github.com/pkg/profile"
)

type State struct {
	Version            string `json:"version"`
	In                 int64  `json:"in"`
	Bad                int64  `json:"bad"`
	Transformed        int64  `json:"transformed"`
	Out                int64  `json:"out"`
	OutBytes           int64  `json:"out_bytes"`
	ReadError          int64  `json:"read_error"`
	SendError          int64  `json:"send_error"`
	InMpm              int64  `json:"in_mpm"`
	BadMpm             int64  `json:"bad_mpm"`
	TransformedMpm     int64  `json:"transformed_mpm"`
	OutMpm             int64  `json:"out_mpm"`
	OutBpm             int64  `json:"out_bpm"`
	Connection         int64  `json:"connection"`
	ConnectionAlive    int64  `json:"connection_alive"`
	ConnectionError    int64  `json:"connection_error"`
	TransformQueue     int64  `json:"transform_queue"`
	OutQueue           int64  `json:"out_queue"`
	Queue              int64  `json:"queue"`
	NegativeQueueError int64  `json:"negative_queue_error"`
	PacksOverflewError int64  `json:"packs_overflew_error"`
}

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

	// JSON endpoint
	router.HandleFunc("/stats", getState).Methods("GET")

	// Prometheus endpoint
	router.Path("/prometheus").Handler(promhttp.Handler())

	stlog.Fatal(http.ListenAndServe(netAddress, router))
}

func getState(w http.ResponseWriter, req *http.Request) {
	json.NewEncoder(w).Encode(state)
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

func sendStateMetrics(instance string, hostname string, systemTenant string, systemPrefix string, inputChan chan *Metric) {
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
		metric.Tenant = systemTenant
		metric.Prefix = systemPrefix

		if metric.Prefix != "" {
			metric.Prefix = metric.Prefix + "."
		}

		// Pass to transformation
		inputChan <- &metric
		atomic.AddInt64(&state.In, 1)
	}
}

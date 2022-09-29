package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gorilla/mux"
	//	"github.com/pkg/profile"
)

type State struct {
	Version            string  `json:"version" type:"string"`
	In                 int64   `json:"in" type:"int"`
	Bad                int64   `json:"bad" type:"int"`
	Transformed        int64   `json:"transformed" type:"int"`
	Out                []int64 `json:"out" type:"slice"`
	OutBytes           []int64 `json:"out_bytes" type:"slice"`
	Returned           []int64 `json:"returned" type:"slice"`
	ReadError          int64   `json:"read_error" type:"int"`
	SendError          int64   `json:"send_error" type:"int"`
	InMpm              int64   `json:"in_mpm" type:"int"`
	BadMpm             int64   `json:"bad_mpm" type:"int"`
	TransformedMpm     int64   `json:"transformed_mpm" type:"int"`
	OutMpm             int64   `json:"out_mpm" type:"int"`
	OutBpm             int64   `json:"out_bpm" type:"int"`
	Connection         int64   `json:"connection" type:"int"`
	ConnectionAlive    int64   `json:"connection_alive" type:"int"`
	ConnectionError    int64   `json:"connection_error" type:"int"`
	TransformQueue     int64   `json:"transform_queue" type:"int"`
	OutQueue           []int64 `json:"out_queue" type:"slice"`
	Queue              []int64 `json:"queue" type:"slice"`
	NegativeQueueError []int64 `json:"negative_queue_error" type:"slice"`
	PacksOverflewError int64   `json:"packs_overflew_error" type:"int"`
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
	router.HandleFunc("/stats", getState).Methods("GET")

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

		// Work with multiple Out queues
		for id := 0; id < len(state.Out); id++ {
			state.OutQueue[id] = atomic.LoadInt64(&state.Transformed) - atomic.LoadInt64(&state.Out[id])
			state.Queue[id] = atomic.LoadInt64(&state.In) - atomic.LoadInt64(&state.Out[id])

			if state.Queue[id] < 0 {
				clog.WithFields(log.Fields{"queue": state.Queue[id]}).Error("Queue value is negative.")
				atomic.AddInt64(&state.NegativeQueueError[id], 1)
			}
		}
	}
}

func sendStateMetrics(instance string, hostname string, systemTenant string, systemPrefix string, inputChan chan *Metric) {
	clog.Info("Sending state metrics.")
	stateSnapshot := state
	timestamp := time.Now().Unix()

	values := reflect.ValueOf(stateSnapshot)
	for i := 0; i < values.NumField(); i++ {
		structfield := values.Type().Field(i)
		name := structfield.Name
		vtype := structfield.Tag.Get("type")
		metricName := structfield.Tag.Get("json")
		valueField := values.Field(i)

		if name == "Version" {
			value := strings.Replace(valueField.String(), ".", "", 2)
			doSend(hostname, instance, name, metricName, value, timestamp, systemTenant, systemPrefix, inputChan)
		} else {
			switch vtype {
			case "int":
				value := fmt.Sprintf("%d", valueField.Int())
				doSend(hostname, instance, name, metricName, value, timestamp, systemTenant, systemPrefix, inputChan)
			case "slice":
				for id := 0; id < valueField.Len(); id++ {
					metricName = fmt.Sprintf("%s_%d", structfield.Tag.Get("json"), id)
					value := fmt.Sprintf("%d", valueField.Index(id).Int())
					doSend(hostname, instance, name, metricName, value, timestamp, systemTenant, systemPrefix, inputChan)
				}
			default:
				stlog.WithFields(log.Fields{"vtype": vtype}).Fatal("Unknown state value type.")
			}
		}
	}
}

func doSend(
	instance string,
	hostname string,
	name string,
	metricName string,
	value string,
	timestamp int64,
	systemTenant string,
	systemPrefix string,
	inputChan chan *Metric) {

	metric := Metric{}

	metric.Path = fmt.Sprintf("%s.groxy.%s.state.%s", hostname, instance, metricName)
	metric.Value = value
	metric.Timestamp = timestamp
	metric.Tenant = systemTenant
	metric.Prefix = systemPrefix

	if metric.Prefix != "" {
		metric.Prefix = metric.Prefix + "."
	}

	stlog.WithFields(log.Fields{
		"stateName":       name,
		"metricName":      metricName,
		"metricValue":     metric.Value,
		"metricPath":      metric.Path,
		"metricTimestamp": metric.Timestamp,
		"metricTenant":    metric.Tenant,
		"metricPrefix":    metric.Prefix,
	}).Info("Sending state field.")

	// Pass to transformation
	inputChan <- &metric
	atomic.AddInt64(&state.In, 1)
}

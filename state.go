package main

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	//	"github.com/pkg/profile"
)

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
		state.OutQueue = atomic.LoadInt64(&state.Transformed) - atomic.LoadInt64(&state.Out)
		state.Queue = atomic.LoadInt64(&state.In) - atomic.LoadInt64(&state.Out)

		if state.Queue < 0 {
			clog.WithFields(log.Fields{"queue": state.Queue}).Error("Queue value is negative.")
			atomic.AddInt64(&state.NegativeQueueError, 1)
		}
	}
}

func sendStateMetrics(instance string, systemTenant string, systemPrefix string, inputChan chan *Metric) {
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
		metric.Prefix = systemPrefix + "."

		// Pass to transformation
		inputChan <- &metric
		atomic.AddInt64(&state.In, 1)
	}
}

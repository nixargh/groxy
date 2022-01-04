package main

import (
	log "github.com/sirupsen/logrus"
	"strings"
	"sync/atomic"
	"time"
	//	"github.com/pkg/profile"
)

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

	// Set tenant only if it is empty
	if metric.Tenant == "" {
		metric.Tenant = tenant
	}

	if metric.Prefix == "" && prefix != "" {
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

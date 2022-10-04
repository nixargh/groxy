package main

import (
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	//	"github.com/pkg/profile"
)

func runTransformer(
	inputChan chan *Metric,
	outputChans []chan *Metric,
	tenant string,
	forceTenant bool,
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
			go transformMetric(metric, outputChans, tenant, forceTenant, prefix, immutablePrefix)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func transformMetric(
	metric *Metric,
	outputChans []chan *Metric,
	tenant string,
	forceTenant bool,
	prefix string,
	immutablePrefix []string) {

	// Set tenant only if it is empty
	if forceTenant == true {
		metric.Tenant = tenant
	} else {
		if metric.Tenant == "" {
			metric.Tenant = tenant
		}
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

	for i := 0; i < len(outputChans); i++ {
		outputChans[i] <- metric
	}

	// Update state
	atomic.AddInt64(&state.Transformed, 1)
}

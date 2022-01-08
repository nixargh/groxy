package main

import (
	"bytes"
	"compress/zlib"
	"crypto/tls"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"sync/atomic"
	"time"
	//	"github.com/pkg/profile"
)

func limitRefresher(limitPerSec *int) {
	savedLimit := *limitPerSec
	slog.WithFields(log.Fields{"limitPerSec": savedLimit}).Info("Starting Packs Limit Refresher.")

	for {
		time.Sleep(1 * time.Second)
		*limitPerSec = savedLimit
	}
}

func runSender(
	host string,
	port int,
	outputChan chan *Metric,
	TLS bool,
	ignoreCert bool,
	limitPerSec int,
	compress bool) {
	slog = clog.WithFields(log.Fields{
		"host":       host,
		"port":       port,
		"tls":        TLS,
		"ignoreCert": ignoreCert,
		"compress":   compress,
		"thread":     "sender",
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
				slog.WithFields(log.Fields{"error": err}).Error("Can't create connection.")
				atomic.AddInt64(&state.ConnectionError, 1)
				time.Sleep(5 * time.Second)
				continue
			} else {
				atomic.AddInt64(&state.Connection, 1)
				atomic.AddInt64(&state.ConnectionAlive, 1)
			}

			// collect a pack of metrics
			var metrics [10000]*Metric

			for i := 0; i < len(metrics); i++ {
				select {
				case metric := <-outputChan:
					metrics[i] = metric
				default:
					metrics[i] = emptyMetric
					time.Sleep(5 * time.Millisecond)
				}
			}

			// send the pack
			if len(metrics) > 0 {
				go sendMetric(&metrics, connection, outputChan, compress)
				limitPerSec--
			}
		} else {
			slog.Warning("Limit of metric packs per second overflew.")
			atomic.AddInt64(&state.PacksOverflewError, 1)
			time.Sleep(1 * time.Second)
		}
	}
}

func sendMetric(metrics *[10000]*Metric, connection net.Conn, outputChan chan *Metric, compress bool) {
	buffered := 0
	sent := 0
	returned := 0
	connectionAlive := true
	var buf bytes.Buffer
	compressedBuf := zlib.NewWriter(&buf)
	var dataLength int
	var err error

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
			"%s%s %s %d %s\n",
			metrics[i].Prefix,
			metrics[i].Path,
			metrics[i].Value,
			metrics[i].Timestamp,
			metrics[i].Tenant,
		)

		if compress == true {
			dataLength, err = compressedBuf.Write([]byte(metricString))
			if err != nil {
				slog.WithFields(log.Fields{"error": err}).Error("Compression buffer write error.")
			} else {
				buffered++
			}
		} else {
			dataLength, err = buf.Write([]byte(metricString))
			slog.WithFields(log.Fields{
				"bytes":  dataLength,
				"metric": metricString,
				"number": i,
			}).Debug("Metric buffered.")
			buffered++
		}
	}

	compressedBuf.Close()

	// Now the time to dump all buffer into connection
	packDataLength, err := buf.WriteTo(connection)

	if err != nil {
		slog.WithFields(log.Fields{"error": err}).Error("Connection write error.")
		atomic.AddInt64(&state.SendError, 1)

		connectionAlive = false

		// Here we must return metric to out outputChan
		for i := 0; i < len(metrics); i++ {
			if metrics[i] == emptyMetric {
				continue
			}

			outputChan <- metrics[i]
			returned += 1
		}
	} else {
		slog.WithFields(log.Fields{
			"bytes":  packDataLength,
			"number": buffered,
		}).Debug("Metrics pack sent.")

		sent += buffered

		atomic.AddInt64(&state.OutBytes, int64(packDataLength))
		atomic.AddInt64(&state.Out, int64(buffered))
	}

	connection.Close()
	atomic.AddInt64(&state.ConnectionAlive, -1)
	slog.WithFields(log.Fields{
		"sent_bytes":   packDataLength,
		"sent_num":     sent,
		"returned_num": returned,
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
	} else {
		connection.SetDeadline(time.Now().Add(600 * time.Second))

		slog.WithFields(log.Fields{
			"remoteAddress": connection.RemoteAddr(),
		}).Info("Connection to Graphite established.")
	}

	return connection, err
}

package main

import (
	"bufio"
	"compress/zlib"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	//	"github.com/pkg/profile"
)

func runReceiver(address string, port int, inputChan chan *Metric, compress bool) {
	rlog = clog.WithFields(log.Fields{
		"address":  address,
		"port":     port,
		"compress": compress,
		"thread":   "receiver",
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
			rlog.WithFields(log.Fields{"error": err}).Fatal("Reader connection acception error.")
		}
		go readMetric(conn, inputChan, compress)
	}
}

func readMetric(connection net.Conn, inputChan chan *Metric, compress bool) {
	connection.SetReadDeadline(time.Now().Add(600 * time.Second))
	var uncompressedConn io.ReadCloser
	var err error

	if compress == true {
		uncompressedConn, err = zlib.NewReader(connection)
		if err != nil {
			rlog.WithFields(log.Fields{"error": err}).Error("Decompression error.")
			atomic.AddInt64(&state.ReadError, 1)
			return
		}
	} else {
		uncompressedConn = connection
	}

	scanner := bufio.NewScanner(uncompressedConn)
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

	uncompressedConn.Close()
}

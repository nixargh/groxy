package main

import (
	"bufio"
	"compress/zlib"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	//	"github.com/pkg/profile"
)

func runReceiver(
	address string,
	port int,
	inputChan chan *Metric,
	TLS bool,
	mutualTLS bool,
	tlsCaCert string,
	tlsCert string,
	tlsKey string,
	ignoreCert bool,
	compress bool) {
	rlog = clog.WithFields(log.Fields{
		"address":    address,
		"port":       port,
		"tls":        TLS,
		"mutualTLS":  mutualTLS,
		"ignoreCert": ignoreCert,
		"compress":   compress,
		"thread":     "receiver",
	})
	netAddress := fmt.Sprintf("%s:%d", address, port)

	rlog.Info("Starting Receiver.")

	var listener net.Listener
	var err error

	if TLS || mutualTLS {
		certs, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
		if err != nil {
			rlog.WithFields(log.Fields{"error": err}).Fatal("TLS Listener server certificates error.")
		}

		var config *tls.Config

		if mutualTLS {
			caCert, err := ioutil.ReadFile(tlsCaCert)
			if err != nil {
				rlog.WithFields(log.Fields{"error": err}).Fatal("TLS Listener CA certificate error.")
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			config = &tls.Config{
				ClientCAs:                   caCertPool,
				ClientAuth:                  tls.RequireAndVerifyClientCert,
				Certificates:                []tls.Certificate{certs},
				MinVersion:                  tls.VersionTLS12,
				MaxVersion:                  tls.VersionTLS12,
				DynamicRecordSizingDisabled: false,
				PreferServerCipherSuites:    true,
			}
		} else {
			config = &tls.Config{
				Certificates:                []tls.Certificate{certs},
				InsecureSkipVerify:          ignoreCert,
				MinVersion:                  tls.VersionTLS12,
				MaxVersion:                  tls.VersionTLS12,
				DynamicRecordSizingDisabled: false,
				PreferServerCipherSuites:    true,
			}
		}

		listener, err = tls.Listen("tcp4", netAddress, config)
		if err != nil {
			rlog.WithFields(log.Fields{"error": err}).Fatal("TLS Listener error.")
		}
	} else {
		listener, err = net.Listen("tcp4", netAddress)
		if err != nil {
			rlog.WithFields(log.Fields{"error": err}).Fatal("Listener error.")
		}
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
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

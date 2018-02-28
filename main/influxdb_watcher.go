// Copyright 2018 Hewlett Packard Enterprise Development LP
//
//    Licensed under the Apache License, Version 2.0 (the "License"); you may
//    not use this file except in compliance with the License. You may obtain
//    a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//    License for the specific language governing permissions and limitations
//    under the License.

package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	configEnv "github.com/caarlos0/env"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/monasca/monasca-watchers/watcher"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

type watcherConfiguration struct {
	InfluxdbAddress    string `env:"INFLUXDB_ADDRESS" envDefault:"http://localhost:8086"`
	Username           string `env:"INFLUXDB_USERNAME" envDefault:"influxdb_watcher"`
	Password           string `env:"INFLUXDB_PASSWORD" envDefault:"password"`
	InfluxdbDatabase   string `env:"INFLUXDB_DATABASE" envDefault:"mon"`
	PrometheusEndpoint string `env:"PROMETHEUS_ENDPOINT" envDefault:"0.0.0.0:8080"`
	Period             int64  `env:"WATCHER_PERIOD" envDefault:"600"`
	Timeout            int64  `env:"WATCHER_TIMEOUT" envDefault:"60"`
}

// InfluxdbBroker Watcher for InfluxDB
type InfluxdbBroker struct {
	MonascaDB    string
	Connection   client.Client
	MessagesSent [3]time.Time
}

// WriteMessage Write a point to Influxdb with UUID and SentTime
func (broker *InfluxdbBroker) WriteMessage(byteMessage []byte) error {
	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  broker.MonascaDB,
		Precision: "s",
	})
	if err != nil {
		log.Error(err)
		return err
	}

	// Create a point and add to batch
	timestamp := time.Now().UTC()
	broker.MessagesSent[2] = broker.MessagesSent[1]
	broker.MessagesSent[1] = broker.MessagesSent[0]
	broker.MessagesSent[0] = timestamp
	fields := map[string]interface{}{
		"message":   string(byteMessage),
		"timestamp": timestamp.Format(time.RFC3339Nano),
	}

	tags := map[string]string{"watcher": "influxdb"}
	pt, err := client.NewPoint("watcher.influxdb", tags, fields, time.Now())
	if err != nil {
		log.Error(err)
		return err
	}

	bp.AddPoint(pt)

	// Write the batch
	err = broker.Connection.Write(bp)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// ReadMessage query InfluxDB for latest measurement
func (broker *InfluxdbBroker) ReadMessage(timeout time.Duration) (*[]byte, error) {
	cmd := "SELECT \"message\",\"timestamp\" FROM \"watcher.influxdb\" order by desc limit 3"
	ch := make(chan []client.Result)
	cherr := make(chan error)
	go queryDB(broker, cmd, ch, cherr)
	select {
	case response := <-ch:
		for _, series := range response[0].Series {
			layout := "2006-01-02T15:04:05.999999999Z07:00"
			timestamp, err := time.Parse(layout, series.Values[0][2].(string))
			if err != nil {
				log.Infof("Failed to parse timestamp from InfluxDB. Skipping. %s", err)
				continue
			}
			for _, ts := range broker.MessagesSent {
				if timestamp == ts {
					message := []byte(series.Values[0][1].(string))
					return &message, nil
				}
			}
		}
		return nil, fmt.Errorf("Messages read do not match any sent recently by this watcher")
	case err := <-cherr:
		return nil, err
	case <-time.After(timeout):
		return nil, fmt.Errorf("Read timeout %d exceeded", timeout)
	}
}

// queryDB convenience function to query the database
func queryDB(broker *InfluxdbBroker, cmd string, ch chan []client.Result, cherr chan error) {
	q := client.Query{
		Command:  cmd,
		Database: broker.MonascaDB,
	}
	response, err := broker.Connection.Query(q)
	if err != nil {
		cherr <- err
		return
	}
	if response.Error() != nil {
		cherr <- response.Error()
		return
	}
	ch <- response.Results
}

func (broker *InfluxdbBroker) checkDatabaseExists(timeout time.Duration) bool {
	cmd := "SHOW DATABASES"
	ch := make(chan []client.Result)
	cherr := make(chan error)
	go queryDB(broker, cmd, ch, cherr)
	select {
	case response := <-ch:
		for _, s := range response[0].Series {
			for _, db := range s.Values {
				if db[0] == broker.MonascaDB {
					log.Infof("Database %s exists", broker.MonascaDB)
					return true
				}
			}
		}
	case <-time.After(timeout):
	case _ = <-cherr:
	}
	log.Infof("Database %s does not exist yet", broker.MonascaDB)
	return false
}

func main() {
	configuration := watcherConfiguration{}
	err := configEnv.Parse(&configuration)
	if err != nil {
		log.Fatalf("Failed to parse environment variables. %s", err)
	}

	influxdbAddress := configuration.InfluxdbAddress
	watcher.ValidateConfString("INFLUXDB_ADDRESS", influxdbAddress)
	influxdbDatabase := configuration.InfluxdbDatabase
	watcher.ValidateConfString("INFLUXDB_DATABASE", influxdbDatabase)
	username := configuration.Username
	watcher.ValidateConfString("INFLUXDB_USERNAME", username)
	password := configuration.Password
	watcher.ValidateConfString("INFLUXDB_PASSWORD", password)
	prometheusEndpoint := configuration.PrometheusEndpoint
	watcher.ValidateConfString("PROMETHEUS_ENDPOINT", prometheusEndpoint)

	log.Infof("Using InfluxDB Address %s", influxdbAddress)

	influxdbBroker := InfluxdbBroker{MonascaDB: influxdbDatabase}
	period := time.Duration(configuration.Period) * time.Second
	timeout := time.Duration(configuration.Timeout) * time.Second

	// Create the watcher now so it has NOT_STARTED status while we are initializing
	// the connections to InfluxDB
	watcher := watcher.CreateWatcher(&influxdbBroker, period, 1, timeout, "influxdb")

	go func() {
		// Start prometheus endpoint
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(prometheusEndpoint, nil))
	}()
	log.Infof("Serving metrics on %s/metrics", prometheusEndpoint)

	connection, err := startConnect(influxdbAddress, username, password)
	influxdbBroker.Connection = connection
	if err != nil {
		log.Fatalf("Failed to set up client")
	}
	for {
		_, _, err := connection.Ping(time.Duration(15) * time.Second)
		if err != nil {
			log.Infof("Connecting to InfluxDB failed: %s", err)
			time.Sleep(time.Duration(10) * time.Second)
		} else {
			break
		}
	}
	for {
		databaseExists := influxdbBroker.checkDatabaseExists(time.Duration(15) * time.Second)
		if databaseExists {
			break
		}
		time.Sleep(time.Duration(10) * time.Second)
	}
	log.Infof("Successfully connected to InfluxDB")
	watcher.Start()

	log.Info("Started influxdb-watcher")

	wg := sync.WaitGroup{}
	wg.Add(1)
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer wg.Done()
		defer watcher.Stop()
		sig := <-sigchan

		log.Fatalf("Caught signal %v: terminating", sig)
	}()

	wg.Wait()
}

func startConnect(influxdbAddress string, username string, password string) (client.Client, error) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     influxdbAddress,
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	log.Infof("Started connection to InfluxDB at %s", influxdbAddress)

	return c, nil
}

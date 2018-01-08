// Copyright 2017-2018 Hewlett Packard Enterprise Development LP
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
	configEnv "github.com/caarlos0/env"
	"github.com/monasca/monasca-watchers/watcher"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type watcherConfiguration struct {
	HealthCheckPath    string `env:"HEALTH_CHECK_PATH" envDefault:"zookeeper-health-check"`
	ZookeeperServers   string `env:"ZOOKEEPER_SERVERS" envDefault:"localhost"`
	PrometheusEndpoint string `env:"PROMETHEUS_ENDPOINT" envDefault:"0.0.0.0:8080"`
	Period             int64  `env:"WATCHER_PERIOD" envDefault:"600"`
	Timeout            int64  `env:"WATCHER_TIMEOUT" envDefault:"60"`
}

type ZookeeperBroker struct {
	Path       string
	Connection *zk.Conn
	PathExists bool
}

func (broker *ZookeeperBroker) WriteMessage(message []byte) error {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	if !broker.PathExists {
		data, stat, err := broker.Connection.Get(broker.Path)
		log.Infof("Path Read returned %s %s %s %s", broker.Path, data, stat, err)
		if err != nil {
			if err != zk.ErrNoNode {
				log.Errorf("Creating %s failed: %s", broker.Path, err)
				return err
			}
			_, err = broker.Connection.Create(broker.Path, nil, flags, acl)
			if err != nil {
				log.Errorf("Creating %s failed: %s", broker.Path, err)
				return err
			}
		}
		broker.PathExists = true
	}
	messagePath := fmt.Sprintf("%s/%s", broker.Path, timestamp)

	log.Infof("Trying to create %s", messagePath)
	path, err := broker.Connection.Create(messagePath, message, flags, acl)
	log.Infof("Create returned %s %s", path, err)

	if err != nil {
		return err
	}
	if path != messagePath {
		// Try to delete it since it did not go where we expected
		log.Errorf("Tried to create node at %s but was created at %s", messagePath, path)
		broker.Connection.Delete(path, -1)
		return fmt.Errorf("Zookeper Node Creation Failure")
	}
	log.Infof("Successfully created %s", messagePath)
	return nil
}

func (broker *ZookeeperBroker) ReadMessage(timeout time.Duration) (*[]byte, error) {
	start := time.Now()
	for time.Since(start) < timeout {
		children, _, err := broker.Connection.Children(broker.Path)
		if len(children) == 0 {
			log.Infof("No children in %s", broker.Path)
			delay := timeout / 10
			if time.Since(start) > (timeout + delay) {
				break
			}
			time.Sleep(delay)
			continue
		}
		log.Infof("Found %s in %s", children, broker.Path)
		sort.Strings(children)
		first := children[0]
		messagePath := fmt.Sprintf("%s/%s", broker.Path, first)
		data, stat, err := broker.Connection.Get(messagePath)
		if err != nil {
			return nil, err
		}
		err = broker.Connection.Delete(messagePath, stat.Version)
		if err != nil {
			log.Errorf("Failed to delete old message at %s", messagePath)
			// Nothing really to do so go on
		}
		return &data, nil
	}

	// Timeout
	return nil, fmt.Errorf("No data after %s", time.Since(start))
}

func main() {
	configuration := watcherConfiguration{}
	err := configEnv.Parse(&configuration)
	if err != nil {
		log.Fatalf("Failed to parse envrionment variables. %s", err)
	}

	healthCheckPath := configuration.HealthCheckPath
	validateConfigurationString("HEALTH_CHECK_PATH", healthCheckPath)
	if !strings.HasPrefix(healthCheckPath, "/") {
		healthCheckPath = "/" + healthCheckPath
	}
	zookeeperServers := configuration.ZookeeperServers
	validateConfigurationString("ZOOKEEPER_SERVERS", zookeeperServers)
	prometheusEndpoint := configuration.PrometheusEndpoint
	validateConfigurationString("PROMETHEUS_ENDPOINT", prometheusEndpoint)

	log.Infof("Using Zookeeper path %s with ZookeeperServers %s", healthCheckPath, zookeeperServers)

	zookeeperBroker := ZookeeperBroker{Path: healthCheckPath}
	period := time.Duration(configuration.Period) * time.Second
	timeout := time.Duration(configuration.Timeout) * time.Second

	// Create the watcher now so it has NOT_STARTED status while we are initializing
	// the connections to Zookeeper
	watcher := watcher.CreateWatcher(&zookeeperBroker, period, 1, timeout, "zookeeper")

	go func() {
		// Start prometheus endpoint
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(prometheusEndpoint, nil))
	}()

	var connection *zk.Conn
	var eventChannel <-chan zk.Event
	connection, eventChannel, err = startConnect(zookeeperServers)
	if err != nil {
		log.Fatalf("Starting connection to Zookeeper failed: %s", err)
	}

	waitForConnected(eventChannel)

	zookeeperBroker.Connection = connection
	go logEvents(eventChannel)

	watcher.Start()

	log.Info("Started zookeeper-watcher")

	wg := sync.WaitGroup{}
	wg.Add(1)
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer wg.Done()
		defer watcher.Stop()
		defer connection.Close()
		sig := <-sigchan

		log.Fatalf("Caught signal %v: terminating", sig)
	}()

	log.Infof("Serving metrics on %s/metrics", prometheusEndpoint)
	wg.Wait()
}

func waitForConnected(eventChannel <-chan zk.Event) {
	stateChannel := make(chan bool)
	go determineState(eventChannel, stateChannel)
	connected := false
	for true {
		stateChannel <- true
		currentState := <-stateChannel
		log.Infof("Current Connected state = %v", currentState)
		if currentState && connected {
			log.Infof("State is connected twice in a row, done waiting for connect")
			close(stateChannel)
			return
		}
		connected = currentState
		time.Sleep(time.Second)
	}
}

func determineState(eventChannel <-chan zk.Event, stateChannel chan bool) {
	connected := false
	for true {
		select {
		case ev := <-eventChannel:
			log.Infof("Event = %s", ev)
			connected = ev.State == zk.StateConnected || ev.State == zk.StateHasSession
			log.Infof("Zookeeper connected state is %v", connected)
		case _, ok := <-stateChannel:
			if !ok {
				// channel closed
				log.Infof("determineState exiting")
				return
			}
			stateChannel <- connected
		}
	}
}

// startConnect doesn't actually connect to the zookeeper server, it just
// starts a thread that tries the connect. As long as the name given resolves,
// zk.Connect() will not return an error. A StateConnected event will be sent
// on the returned channel when the servers are actually connected
func startConnect(zookeeperServers string) (*zk.Conn, <-chan zk.Event, error) {
	zks := strings.Split(zookeeperServers, ",")
	conn, eventChannel, err := zk.Connect(zks, time.Duration(10)*time.Second)

	if err != nil {
		return nil, nil, err
	}

	log.Infof("Started connecting to Zookeeper Servers %s", zookeeperServers)
	return conn, eventChannel, nil
}

func validateConfigurationString(name, value string) {
	if len(value) == 0 {
		log.Fatalf("Invalid %s, must not be empty", name)
	}
}

func logEvents(eventChannel <-chan zk.Event) {
	for true {
		ev := <-eventChannel
		log.Infof("Event = %s", ev)
	}
}

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
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/monasca/monasca-watchers/watcher"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type watcherConfiguration struct {
	HealthCheckTopic   string `env:"HEALTH_CHECK_TOPIC" envDefault:"kafka-health-check"`
	BootstrapServers   string `env:"BOOT_STRAP_SERVERS" envDefault:"localhost"`
	GroupID            string `env:"GROUP_ID" envDefault:"kafka_watcher"`
	PrometheusEndpoint string `env:"PROMETHEUS_ENDPOINT" envDefault:"0.0.0.0:8080"`
	Period             int64  `env:"WATCHER_PERIOD" envDefault:"600"`
	Timeout            int64  `env:"WATCHER_TIMEOUT" envDefault:"60"`
}

type KafkaBroker struct {
	Topic    string
	Consumer *kafka.Consumer
	Producer *kafka.Producer
}

func (broker *KafkaBroker) WriteMessage(message []byte) error {
	broker.Producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &broker.Topic,
			Partition: kafka.PartitionAny},
		Value: message}

	return nil
}

func (broker *KafkaBroker) ReadMessage(timeout time.Duration) (*[]byte, error) {
	timeoutChan := time.After(timeout)
	for true {
		select {
		case <-timeoutChan:
			log.Infof("Kafka Read Timed out")
			return nil, fmt.Errorf("Kafka Read Timed Out")
		case ev := <-broker.Consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				log.Infof("%% %v", e)
				broker.Consumer.Assign(e.Partitions)

			case kafka.RevokedPartitions:
				log.Infof("%% %v", e)
				broker.Consumer.Unassign()

			case *kafka.Message:
				message := []byte(e.Value)
				broker.Consumer.CommitMessage(e)
				return &message, nil

			case kafka.OffsetsCommitted:
				log.Infof("Commited offsets ", e)

			case kafka.PartitionEOF:
				//log.Infof("%% Reached %v", e)

			case kafka.Error:
				log.Fatalf("%% Error: %v", e)
			}
		}
	}

	return nil, nil
}

func main() {
	configuration := watcherConfiguration{}
	err := configEnv.Parse(&configuration)
	if err != nil {
		log.Fatalf("Failed to parse envrionment variables. %s", err)
	}

	healthCheckTopic := configuration.HealthCheckTopic
	bootstrapServers := configuration.BootstrapServers

	groupID := configuration.GroupID

	prometheusEndpoint := configuration.PrometheusEndpoint

	log.Infof("Using Kafka topic %s with bootstrapServer %s", healthCheckTopic, bootstrapServers)

	kafkaBroker := KafkaBroker{Topic: healthCheckTopic}
	period := time.Duration(configuration.Period) * time.Second
	timeout := time.Duration(configuration.Timeout) * time.Second

	// Create the watcher now so it has NOT_STARTED status while we are initializing
	// the connections to Kafka
	watcher := watcher.CreateWatcher(&kafkaBroker, period, 1, timeout, "kafka")

	go func() {
		// Start prometheus endpoint
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(prometheusEndpoint, nil))
	}()

	var consumer *kafka.Consumer
	for consumer == nil {
		consumer, err = initConsumer(groupID, bootstrapServers)
		if err != nil {
			log.Infof("Connecting to Kafka as Consumer failed: %s", err)
			time.Sleep(time.Duration(10) * time.Second)
		}
	}

	// Ensure the topic has been created
	for true {
		meta, err := consumer.GetMetadata(&healthCheckTopic, false, 2000)
		if err == nil {
			log.Infof("Topic Metadata = %s", meta)
			break
		}
		log.Infof("Getting topic %s Metadata as Consumer failed: %s", healthCheckTopic, err)
		time.Sleep(time.Duration(10) * time.Second)
	}

	kafkaBroker.Consumer = consumer

	for true {
		err = subscribeToTopic(consumer, healthCheckTopic, groupID)
		if err == nil {
			break
		}
		log.Infof("Subscribing to Kafka topic %s failed: %s", healthCheckTopic, err)
		time.Sleep(time.Duration(10) * time.Second)
	}

	var producer *kafka.Producer
	for producer == nil {
		producer, err = initProducer(bootstrapServers)
		if err != nil {
			log.Infof("Connecting to Kafka as Producer failed: %s", err)
			time.Sleep(time.Duration(10) * time.Second)
		}
	}
	kafkaBroker.Producer = producer

	go handleProducerEvents(producer)

	// Give the consumer time to get partitions assigned
	time.Sleep(time.Duration(30) * time.Second)

	watcher.Start()

	log.Info("Started kafka-watcher")

	wg := sync.WaitGroup{}
	wg.Add(1)
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer wg.Done()
		defer watcher.Stop()
		defer consumer.Close()
		defer producer.Close()
		sig := <-sigchan

		log.Fatalf("Caught signal %v: terminating", sig)
	}()

	log.Infof("Serving metrics on %s/metrics", prometheusEndpoint)
	wg.Wait()
}

func initConsumer(groupID, bootstrapServers string) (*kafka.Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               bootstrapServers,
		"group.id":                        groupID,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.auto.commit":              false,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
	})

	if err != nil {
		return nil, err
	}

	log.Infof("Created kafka consumer %v", c)
	return c, nil
}

func subscribeToTopic(c *kafka.Consumer, consumerTopic, groupID string) error {
	err := c.Subscribe(consumerTopic, nil)

	if err != nil {
		return err
	}
	log.Infof("Subscribed to topic %s as group %s", consumerTopic, groupID)

	return nil
}

func initProducer(bootstrapServers string) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		return nil, err
	}

	log.Infof("Created kafka producer %v", p)

	return p, nil
}

func handleProducerEvents(p *kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				log.Errorf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				log.Debugf("Delivered message to topic %s [%d] at offset %v\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}

		default:
			log.Debugf("Ignored event: %s\n", ev)
		}
	}
}

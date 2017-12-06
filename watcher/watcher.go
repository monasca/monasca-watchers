// Copyright 2017 Hewlett Packard Enterprise Development LP
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

package watcher

import (
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"time"
)

// Status of the Watchable
type Status int

const (
	NOT_STARTED = -1
	OK          = 0
	WARNING     = 1
	ERROR       = 2
)

var statuses = [...]string{
	"OK",
	"WARNING",
	"ERROR",
}

const (
	timeFormat            = time.RFC3339Nano
	RUNNING_AVERAGE_SLOTS = 5
)

func (status Status) String() string {
	return statuses[status]
}

type Watchable interface {
	WriteMessage([]byte) error
	ReadMessage(timeout time.Duration) (*[]byte, error)
}

type message struct {
	UUID     string
	SentTime time.Time
}

type Watcher struct {
	Status                            Status
	StatusMetric                      prometheus.Gauge
	MinRoundTripTime                  time.Duration
	MinRoundTripTimeMetric            prometheus.Gauge
	MaxRoundTripTime                  time.Duration
	MaxRoundTripTimeMetric            prometheus.Gauge
	Timeout                           time.Duration
	Period                            time.Duration
	ReadFailures                      int
	ReadFailuresMetric                prometheus.Counter
	WriteFailures                     int
	WriteFailuresMetric               prometheus.Counter
	DroppedMessagesMetric             prometheus.Counter
	MaxFailures                       int
	watchable                         Watchable
	stopChannel                       chan bool
	stoppedChannel                    chan bool
	stopNow                           bool
	outstanding                       map[string]message
	totalTime                         float64
	totalRoundTrips                   int64
	TotalRoundTripsMetric             prometheus.Counter
	AverageRoundTripTimeMetric        prometheus.Gauge
	lastTripTimes                     []float64
	nextTripTimeSlot                  int
	RunningAverageRoundTripTimeMetric prometheus.Gauge
}

func CreateWatcher(watchable Watchable, period time.Duration, maxFailures int, timeout time.Duration, serviceName string) *Watcher {
	prefix := serviceName + "_"
	statusMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: prefix + "watcher_status",
			Help: "Status of watcher: -1 = NOT_STARTED, 0 = OK, 1 = WARNING, 2 = ERROR"})
	statusMetric.Set(NOT_STARTED)
	minRoundTripTimeMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: prefix + "min_round_trip_time",
			Help: "Minimum Round Trip Time in seconds"})
	maxRoundTripTimeMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: prefix + "max_round_trip_time",
			Help: "Maximum Round Trip Time in seconds"})
	readFailuresMetric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: prefix + "read_failure_count",
			Help: "Number of failures reading messages"})
	writeFailuresMetric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: prefix + "write_failure_count",
			Help: "Number of failures writing messages"})
	droppedMessagesMetric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: prefix + "dropped_message_count",
			Help: "Number of messages that were dropped"})
	totalRoundTripsMetric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: prefix + "total_round_trips",
			Help: "Number of sucessful round trips"})
	averageRoundTripTimeMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: prefix + "average_round_trip_time",
			Help: "Average Round Trip Time in seconds"})
	runningAverageRoundTripTimeMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: prefix + "running_average_round_trip_time",
			Help: fmt.Sprintf("Running Average Round Trip Time in seconds for last %d messages",
				RUNNING_AVERAGE_SLOTS)})
	prometheus.MustRegister(statusMetric)
	prometheus.MustRegister(minRoundTripTimeMetric)
	prometheus.MustRegister(maxRoundTripTimeMetric)
	prometheus.MustRegister(readFailuresMetric)
	prometheus.MustRegister(writeFailuresMetric)
	prometheus.MustRegister(droppedMessagesMetric)
	prometheus.MustRegister(totalRoundTripsMetric)
	prometheus.MustRegister(averageRoundTripTimeMetric)
	prometheus.MustRegister(runningAverageRoundTripTimeMetric)
	watcher := Watcher{
		watchable:                         watchable,
		MaxFailures:                       maxFailures,
		Timeout:                           timeout,
		Period:                            period,
		stopChannel:                       make(chan bool, 10),
		stoppedChannel:                    make(chan bool, 1),
		Status:                            NOT_STARTED,
		StatusMetric:                      statusMetric,
		MinRoundTripTimeMetric:            minRoundTripTimeMetric,
		MinRoundTripTime:                  time.Duration(1000) * time.Hour,
		MaxRoundTripTimeMetric:            maxRoundTripTimeMetric,
		MaxRoundTripTime:                  0,
		ReadFailuresMetric:                readFailuresMetric,
		WriteFailuresMetric:               writeFailuresMetric,
		DroppedMessagesMetric:             droppedMessagesMetric,
		outstanding:                       make(map[string]message),
		TotalRoundTripsMetric:             totalRoundTripsMetric,
		AverageRoundTripTimeMetric:        averageRoundTripTimeMetric,
		RunningAverageRoundTripTimeMetric: runningAverageRoundTripTimeMetric,
	}

	return &watcher
}

// Start watching the Watchable
func (watcher *Watcher) Start() {
	log.Info("starting watch")
	go watcher.watch()
}

func (watcher *Watcher) watch() {
	log.Info("watcher started")
	watcher.Status = OK
	watcher.StatusMetric.Set(OK)
	var consecutiveReadFailures int
	var consecutiveWriteFailures int
	uuid := time.Now().Format(time.RFC3339Nano) // Not a true UUID, but close enough for this
	for !watcher.stopNow {
		now := time.Now()
		// Delete messages we're not going to wait for anymore
		for sentTimeString := range watcher.outstanding {
			sentTime, _ := time.Parse(timeFormat, sentTimeString)
			if now.Sub(sentTime) > time.Duration(10)*watcher.Period {
				log.Infof("Dropping old message with time %s", sentTimeString)
				watcher.DroppedMessagesMetric.Inc()
				delete(watcher.outstanding, sentTimeString)
			}
		}
		messageToSend := message{
			UUID:     uuid,
			SentTime: now,
		}
		bytes, err := json.Marshal(messageToSend)
		if err != nil {
			panic("json encoding failed")
		}
		log.Infof("Writing message %v", messageToSend)
		writeErr := watcher.watchable.WriteMessage(bytes)
		if writeErr == nil {
			log.Infof("Successfully sent message")
			watcher.outstanding[messageToSend.SentTime.Format(timeFormat)] = messageToSend
			consecutiveWriteFailures = 0
		} else {
			log.Infof("Failure writing message %s", writeErr)
			if watcher.Status == OK {
				watcher.Status = WARNING
			}
			watcher.WriteFailures++
			consecutiveWriteFailures++
		}
		watcher.StatusMetric.Set(float64(watcher.Status))
		for !watcher.stopNow && len(watcher.outstanding) > 0 {
			log.Debugf("Outstanding = %s", watcher.outstanding)
			messageBytes, readErr := watcher.watchable.ReadMessage(watcher.Timeout)
			if readErr != nil {
				log.Infof("ReadMessage failed: %v", readErr)
				watcher.ReadFailures++
				watcher.ReadFailuresMetric.Inc()
				consecutiveReadFailures++
				watcher.Status = WARNING
				break
			}
			var message message
			messageErr := json.Unmarshal(*messageBytes, &message)
			if messageErr == nil {
				if message.UUID != uuid {
					log.Infof("Received message from old watcher, expected %s was %s sentTime %s",
						uuid, message.UUID, message.SentTime)
					continue
				}
			}
			delta := time.Now().Sub(message.SentTime)
			watcher.updateMetrics(delta)

			sentTimeString := message.SentTime.Format(timeFormat)
			_, found := watcher.outstanding[sentTimeString]
			if found {
				delete(watcher.outstanding, sentTimeString)
			} else {
				log.Infof("Received old Message %s", message)
			}
			consecutiveReadFailures = 0
		}

		log.Debugf("consecutiveReadFailures = %d consecutiveWriteFailures = %d", consecutiveReadFailures,
			consecutiveWriteFailures)
		if consecutiveWriteFailures == 0 && consecutiveReadFailures == 0 {
			watcher.Status = OK
		} else if consecutiveReadFailures >= watcher.MaxFailures || consecutiveWriteFailures >= watcher.MaxFailures {
			watcher.Status = ERROR
		}
		watcher.StatusMetric.Set(float64(watcher.Status))

		waitForBool(watcher.stopChannel, watcher.Period)
	}
	log.Info("watch exiting")
	watcher.stoppedChannel <- true
}

func (watcher *Watcher) updateMetrics(delta time.Duration) {
	log.Infof("Message round trip = %s", delta)
	watcher.totalTime += float64(delta) / float64(time.Second)
	watcher.totalRoundTrips++
	watcher.TotalRoundTripsMetric.Inc()
	watcher.AverageRoundTripTimeMetric.Set(watcher.totalTime / float64(watcher.totalRoundTrips))
	deltaSeconds := float64(delta) / float64(time.Second)
	if watcher.MaxRoundTripTime < delta {
		watcher.MaxRoundTripTime = delta
		watcher.MaxRoundTripTimeMetric.Set(deltaSeconds)
	}
	if watcher.MinRoundTripTime > delta {
		watcher.MinRoundTripTime = delta
		watcher.MinRoundTripTimeMetric.Set(deltaSeconds)
	}
	if len(watcher.lastTripTimes) < RUNNING_AVERAGE_SLOTS {
		watcher.lastTripTimes = append(watcher.lastTripTimes, deltaSeconds)
		watcher.nextTripTimeSlot++
	} else {
		if watcher.nextTripTimeSlot >= RUNNING_AVERAGE_SLOTS {
			watcher.nextTripTimeSlot = 0
		}
		watcher.lastTripTimes[watcher.nextTripTimeSlot] = deltaSeconds
		watcher.nextTripTimeSlot++
	}
	var sum float64
	for _, value := range watcher.lastTripTimes {
		sum += float64(value)
	}
	watcher.RunningAverageRoundTripTimeMetric.Set(sum / float64(len(watcher.lastTripTimes)))
}

func waitForBool(c chan bool, timeout time.Duration) bool {
	timeoutChan := time.After(timeout)
	select {
	case <-c:
		return true
	case <-timeoutChan:
		return false
	}
	return false
}

// Stop watching the Watchable returns true if watcher routine exited
func (watcher *Watcher) Stop() bool {
	log.Info("Stop() called")
	watcher.stopNow = true
	watcher.stopChannel <- true
	return waitForBool(watcher.stoppedChannel, time.Second)
}

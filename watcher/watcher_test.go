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
	"fmt"
	"github.com/stretchr/testify/assert"
	log "github.hpe.com/kronos/kelog"
	"testing"
	"time"
)

type testBroker struct {
	written          chan []byte
	messageRead      chan bool
	returnErrOnWrite bool
	toRead           chan []byte
	t                *testing.T
}

func createTestBroker(returnErrOnWrite bool, t *testing.T) *testBroker {
	return &testBroker{
		written:          make(chan []byte),
		messageRead:      make(chan bool),
		toRead:           make(chan []byte),
		returnErrOnWrite: returnErrOnWrite,
		t:                t,
	}
}

var (
	twoHundredMilliseconds = time.Duration(200) * time.Millisecond
	tenMilliseconds        = time.Duration(10) * time.Millisecond
	twoMilliseconds        = time.Duration(2) * time.Millisecond
)

func (broker *testBroker) WriteMessage(message []byte) error {
	log.Errorf("Received a message of length %v", len(message))
	broker.written <- message
	if broker.returnErrOnWrite {
		return fmt.Errorf("Expected Error on Write")
	}
	return nil
}

func (broker *testBroker) ReadMessage(timeout time.Duration) (*[]byte, error) {
	timeoutChan := time.After(timeout)
	select {
	case message := <-broker.toRead:
		log.Infof("Read %v bytes", len(message))
		broker.messageRead <- true
		return &message, nil
	case <-timeoutChan:
		log.Infof("Timed out")
		return nil, fmt.Errorf("Read failed")
	}
	broker.t.Fatal("Something bad happened")
	// Not reached
	return nil, nil
}

func (broker *testBroker) waitForWrite(timeout time.Duration) (*[]byte, bool) {
	timeoutChan := time.After(timeout)
	select {
	case message := <-broker.written:
		return &message, true
	case <-timeoutChan:
		log.Infof("waitForWrite Timed out")
		return nil, false
	}
	broker.t.Fatal("Something bad happened")
	// Not reached
	return nil, false
}

func (broker *testBroker) waitForRead(timeout time.Duration) bool {
	timeoutChan := time.After(timeout)
	select {
	case <-broker.messageRead:
		return true
	case <-timeoutChan:
		log.Infof("waitForRead Timed out")
		return false
	}
	broker.t.Fatal("Something bad happened")
	// Not reached
	return false
}

func TestSimple(t *testing.T) {
	log.SetLevelString("info")
	testBroker := createTestBroker(false, t)
	watcher := CreateWatcher(testBroker, time.Duration(1)*time.Second, 1, twoHundredMilliseconds,
		"TestSimple")
	assert.Equal(t, Status(NOT_STARTED), watcher.Status, "Initial watcher.status")
	watcher.Start()

	testOneWriteRead(testBroker, watcher)

	watcher.Stop()
	assert.Equal(t, 0, watcher.ReadFailures, "watcher.ReadFailures")
	assert.Equal(t, 0, watcher.WriteFailures, "watcher.WriteFailures")
	assert.Equal(t, Status(OK), watcher.Status, "watcher.Status")
}

func testOneWriteRead(broker *testBroker, watcher *Watcher) {
	message, ok := broker.waitForWrite(twoHundredMilliseconds)
	if !ok {
		broker.t.Error("Did not get a write")
		broker.t.FailNow()
	}

	// Set message to be read
	log.Infof("Sending %v bytes back", len(*message))
	broker.toRead <- *message

	messageWasRead := broker.waitForRead(twoHundredMilliseconds)
	assert.True(broker.t, messageWasRead)
}

func TestReadFailure(t *testing.T) {
	log.SetLevelString("info")
	testBroker := createTestBroker(false, t)
	maxFailures := 2
	watcher := CreateWatcher(testBroker, tenMilliseconds, maxFailures, tenMilliseconds,
		"TestReadFailure")
	watcher.Start()

	testOneWriteRead(testBroker, watcher)

	for iter := 0; iter < maxFailures; iter++ {
		if iter > 0 {
			time.Sleep(tenMilliseconds)
			assert.Equal(t, Status(WARNING), watcher.Status, "watcher.Status")
		}
		message, ok := testBroker.waitForWrite(twoHundredMilliseconds)
		if !ok {

			t.Error("Did not get a write")
			t.FailNow()
		}

		time.Sleep(twoMilliseconds)
		assert.Equal(t, iter, watcher.ReadFailures, "watcher.ReadFailures")
		assert.Equal(t, 0, watcher.WriteFailures, "watcher.WriteFailures")
		log.Infof("Received message %s", message)
	}
	watcher.Stop()
	assert.Equal(t, maxFailures, watcher.ReadFailures, "watcher.ReadFailures")
	assert.Equal(t, 0, watcher.WriteFailures, "watcher.WriteFailures")
	assert.Equal(t, Status(ERROR), watcher.Status, "watcher.Status")
}

func TestWriteFailure(t *testing.T) {
	log.SetLevelString("info")
	testBroker := createTestBroker(false, t)
	maxFailures := 2
	watcher := CreateWatcher(testBroker, tenMilliseconds, maxFailures, tenMilliseconds,
		"TestWriteFailure")
	watcher.Start()

	testOneWriteRead(testBroker, watcher)
	testBroker.returnErrOnWrite = true

	for iter := 0; iter < maxFailures; iter++ {
		if iter > 0 {
			time.Sleep(tenMilliseconds)
			assert.Equal(t, Status(WARNING), watcher.Status, "watcher.Status")
		}
		message, ok := testBroker.waitForWrite(twoHundredMilliseconds)
		if !ok {
			t.Error("Did not get a write")
			t.FailNow()
		}

		time.Sleep(twoMilliseconds)
		assert.Equal(t, 0, watcher.ReadFailures, "watcher.ReadFailures")
		assert.Equal(t, iter+1, watcher.WriteFailures, "watcher.WriteFailures")
		log.Infof("Received message %s", message)
	}
	watcher.Stop()
	assert.Equal(t, 0, watcher.ReadFailures, "watcher.ReadFailures")
	assert.Equal(t, maxFailures, watcher.WriteFailures, "watcher.WriteFailures")
	assert.Equal(t, Status(ERROR), watcher.Status, "watcher.Status")
}

func TestWriteFailureRecovery(t *testing.T) {
	log.SetLevelString("info")
	testBroker := createTestBroker(false, t)
	maxFailures := 2
	watcher := CreateWatcher(testBroker, tenMilliseconds, maxFailures, tenMilliseconds,
		"TestWriteFailureRecovery")
	watcher.Start()

	testOneWriteRead(testBroker, watcher)
	testBroker.returnErrOnWrite = true

	_, ok := testBroker.waitForWrite(twoHundredMilliseconds)
	if !ok {
		t.Error("Did not get a write")
		t.FailNow()
	}

	testBroker.returnErrOnWrite = false
	time.Sleep(twoMilliseconds)

	assert.Equal(t, Status(WARNING), watcher.Status, "watcher.Status")

	testOneWriteRead(testBroker, watcher)

	watcher.Stop()
	assert.Equal(t, 0, watcher.ReadFailures, "watcher.ReadFailures")
	assert.Equal(t, 1, watcher.WriteFailures, "watcher.WriteFailures")
	assert.Equal(t, Status(OK), watcher.Status, "watcher.Status")
}

func TestReadFailureRecovery(t *testing.T) {
	log.SetLevelString("info")
	testBroker := createTestBroker(false, t)
	maxFailures := 2
	watcher := CreateWatcher(testBroker, twoHundredMilliseconds, maxFailures, tenMilliseconds,
		"TestReadFailureRecovery")
	watcher.Start()

	testOneWriteRead(testBroker, watcher)

	for iter := 0; iter < maxFailures; iter++ {
		time.Sleep(tenMilliseconds)
		time.Sleep(tenMilliseconds)
		if iter > 0 {
			assert.Equal(t, Status(WARNING), watcher.Status, "watcher.Status")
		}
		message, ok := testBroker.waitForWrite(twoHundredMilliseconds)
		if !ok {
			t.Error("Did not get a write")
			t.FailNow()
		}

		time.Sleep(twoMilliseconds)
		assert.Equal(t, iter, watcher.ReadFailures, "watcher.ReadFailures")
		assert.Equal(t, 0, watcher.WriteFailures, "watcher.WriteFailures")
		log.Infof("Received message %s", message)
	}

	time.Sleep(tenMilliseconds)
	time.Sleep(tenMilliseconds)

	assert.Equal(t, Status(ERROR), watcher.Status, "watcher.Status")

	testOneWriteRead(testBroker, watcher)

	time.Sleep(tenMilliseconds)
	time.Sleep(tenMilliseconds)
	watcher.Stop()

	// Will be WARNING because the one failed read is still outstanding
	assert.Equal(t, Status(WARNING), watcher.Status, "watcher.Status")
	assert.Equal(t, maxFailures+1, watcher.ReadFailures, "watcher.ReadFailures")
	assert.Equal(t, 0, watcher.WriteFailures, "watcher.WriteFailures")
}

# monasca-watchers

Golang components that watch Monasca components to show their health. Currently, only the Kafka
and Zookeeper Watchers are implemented.

## Watchers

Each Watcher periodically writes a message and then reads it back. It uses this cycle to
determine the health of what it is watching. The status and various other metrics are exposed as
Prometheus metrics. If there is a single read or write failure, the status goes to WARNING. Once
there have been two consecutive read or write failures, the status goes to ERROR.

### Kafka Watcher

The Kafka Watcher writes a message to a Kafka topic and then reads it back from the same
topic. This ensures the entire round trip functionality of Kafka is functioning.

### Zookeeper Watcher

The Zookeeper Watcher writes a message to a Zookeeper node and then reads it back from the
same node. This ensures the entire round trip functionality of Zookeeper is functioning.

## Run Tests

   # This is required to skip tests for the vendor directory
   go test $(go list ./... | grep -v /vendor/)

## Configuration

### Kafka Watcher

Several parameters can be specified using environment variables:

| Variable              | Default              | Description                               |
|-----------------------|----------------------|-------------------------------------------|
| `HEALTH_CHECK_TOPIC`  | `kafka-health-check` | Topic to use for health check read/writes |
| `BOOT_STRAP_SERVERS`  |`localhost`           | kafka brokers                             |
| `GROUP_ID`            |`kafka_watcher`       | Group Id for Consumer                     |
| `PROMETHEUS_ENDPOINT` | `0.0.0.0:8080`       | Endpoint for Prometheus metrics           |
| `WATCHER_PERIOD`      |`600`                 | How often to do a read/write cycle        |
| `WATCHER_TIMEOUT`     |`60`                  | How long to wait for message read         |

### Zookeeper Watcher

Several parameters can be specified using environment variables:

| Variable              | Default                  | Description                               |
|-----------------------|--------------------------|-------------------------------------------|
| `HEALTH_CHECK_PATH`   | `zookeeper-health-check` | Path to use for health check read/writes  |
| `ZOOKEEPER_SERVERS`   |`localhost`               | Zookeeper servers                         |
| `PROMETHEUS_ENDPOINT` | `0.0.0.0:8080`           | Endpoint for Prometheus metrics           |
| `WATCHER_PERIOD`      |`600`                     | How often to do a read/write cycle        |
| `WATCHER_TIMEOUT`     |`60`                      | How long to wait for message read         |

## Metrics

### Kafka Watcher

| Metric                        | Type      | Description                                       |
|-------------------------------|-----------|---------------------------------------------------|
| `kafka_average_round_trip_time` | `gauge` | Average Round Trip Time in seconds |
| `kafka_dropped_message_count` | `counter` | Number of messages that were dropped              |
| `kafka_max_round_trip_time`   | `gauge`   | Maximum Round Trip Time in seconds                |
| `kafka_min_round_trip_time`   | `gauge`   | Minimum Round Trip Time in seconds                |
| `kafka_read_failure_count`    | `counter` | Number of failures reading messages               |
| `kafka_running_average_round_trip_time` | `gauge` | Running Average Round Trip Time in seconds for last 5 messages |
| `kafka_watcher_status`        | `gauge`   | Status of watcher: -1 = NOT_STARTED, 0 = OK, 1 = WARNING, 2 = ERROR |
| `kafka_write_failure_count`   | `counter` | Number of failures writing messages               |


### Zookeeper Watcher

| Metric                            | Type      | Description                                       |
|-----------------------------------|-----------|---------------------------------------------------|
| `zookeeper_average_round_trip_time` | `gauge` | Average Round Trip Time in seconds |
| `zookeeper_dropped_message_count` | `counter` | Number of messages that were dropped              |
| `zookeeper_max_round_trip_time`   | `gauge`   | Maximum Round Trip Time in seconds                |
| `zookeeper_min_round_trip_time`   | `gauge`   | Minimum Round Trip Time in seconds                |
| `zookeeper_read_failure_count`    | `counter` | Number of failures reading messages               |
| `zookeeper_running_average_round_trip_time` | `gauge` | Running Average Round Trip Time in seconds for last 5 messages |
| `zookeeper_watcher_status`        | `gauge`   | Status of watcher: -1 = NOT_STARTED, 0 = OK, 1 = WARNING, 2 = ERROR |
| `zookeeper_write_failure_count`   | `counter` | Number of failures writing messages               |

## Install (using Docker)

See github.com/monasca/monasca-docker/tree/master/kafka-watcher and github.com/monasca/monasca-docker/tree/master/zookeeper-watcher

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.




# Kafka Log Driver  [![Build Status](https://travis-ci.org/MickayG/moby-kafka-logdriver.svg?branch=master)](https://travis-ci.org/MickayG/moby-kafka-logdriver)


An upcoming build of Moby/Docker (expected to be 17.05) will support log driver plugins which allows people to add extra functionality to handle logs coming out of docker containers.

This plugin allows users to route all Moby/Docker logs to Kafka.

## Installation

***This plugin requires at least Docker version 17.05***

There are two ways to install the plugin, via Dockerhub or building
from this repository.

### Install the plugin from Dockerhub:

Install the plugin but add the --disable flag so it does not start immediately. The kafka brokers must be set first.
```
docker plugin install --disable mickyg/kafka-logdriver:latest
```
Set the Kafka brokers and configure the plugin as per the configuration section.
In the example below the host 192.168.0.1 is a Kafka broker.
```
docker plugin set mickyg/kafka-logdriver:latest KAFKA_BROKER_ADDR="192.168.0.1:9092"
```
Then enable the plugin
```
docker plugin enable mickyg/kafka-logdriver:latest
```
Now it's installed! To use the log driver for a given container, use the `--logdriver` flag. For example, to start the hello-world
container with all of it's logs being sent to Kafka, run the command:
```
docker run --log-driver mickyg/kafka-logdriver:latest hello-world
```

### Install from source

Clone the project
```
git clone https://github.com/MickayG/moby-kafka-logdriver.git
cd moby-kafka-logdriver
```
Build the plugin and install it
```
make install
```
Set the KAFKA_BROKER_ARG variable. In the example below the host 192.168.0.1 is a Kafka broker.
```
docker plugin set mickyg/kafka-logdriver:latest KAFKA_BROKER_ADDR="192.168.0.1:9092"
```
Enable the plugin:
```
make enable
```
Now test it! Connect to your broker and consume from the "dockerlogs" topic
(Topic can be changed via environment variable, see below). Then launch a container:
```
docker run --log-driver mickyg/kafka-logdriver:latest hello-world
```

## Configuration

### Global Configuration

Docker logdriver plugins can be configured on global/plugin basis. The below configurations when applied act on all containers where the logdriver plugin is used.

Once the plugin has been installed, you can modify the below arguments with the command

`docker plugin set kafka-logdriver <OPTION>=<VALUE>`

For example, to change the topic to "logs"

`docker plugin set kafka-logdriver LOG_TOPIC=logs`


| Option | Description | Default |
| -------|------------| --------|
|KAFKA_BROKER_ADDR|**(Required)** Comma delimited list of Kafka brokers. | |
|LOG_TOPIC| Topic to which logs will be written to, can be set to `$CONTAINERNAME` to write to topics named after each container, or `$CONTAINERID` to write to topics named after each containers id. | dockerlogs |
|LOG_TAG| Docker logs tag| common |
|KEY_STRATEGY| Method in which Kafka methods should be keyed. Options are: <br>*key_by_timestamp* - Key each message by the timestamp of the log message <br>*key_by_container_id* - Key each message with the container id. | key_by_timestamp
|PARTITION_STRATEGY| Kafka partitioner type. Options are:<br>*round_robin* - Write to each partition one after another, i.e equally distributed<br>*key_hash* - Partition based on the hash of the message key|round_robin|
|LOG_LEVEL| Log level of the internal logger. Options: debug, info, warn, error|info|


### Per Container Configuration

Certain global configurations can be overridden on a per-container basis with container environment variables.
For example containers can be configured to write to different topics:
```
docker run --log-driver mickyg/kafka-logdriver:latest -e LOGGING_KAFKA_TOPIC=alphalogs --name "Alpha" hello-world
docker run --log-driver mickyg/kafka-logdriver:latest -e LOGGING_KAFKA_TOPIC=betalogs --name "Beta" hello-world
```
The logs from the container "Alpha" will be written to "alphalogs", the logs from container "beta" will be written to "betalogs"

The available container-level configurations are in the table below.

| Option | Description |
| -------| ----------- |
| LOG_TOPIC | Topic to which logs will be written to, can be set to `$CONTAINERNAME` to write to topics named after each container, or `$CONTAINERID` to write to topics named after each containers id. If not set, will default to global LOG_TOPIC variable |
| LOG_TAG | Tag to be added to the log messages |


## Output Format
Each log message will be written to a single Kafka message. The message within Kafka is a JSON record containing the log message and details about the source of the message. An example log message pretty-printed is below. Mesages are not stored pretty-printed in Kafka.
```
{
	"Line": "This message shows that your installation appears to be working correctly.",
	"Source": "stdout",
	"Timestamp": "2017-04-24T23:03:52.065164047Z",
	"Partial": false,
	"ContainerName": "/HelloOutThere",
	"ContainerId": "be2af19df661fb08561a8a99c734f637f9dc1397c43d88379479fceb5fc0666d",
	"ContainerImageName": "hello-world",
	"ContainerImageId": "sha256:48b5124b2768d2b917edcb640435044a97967015485e812545546cbed5cf0233",
	"Hostname": "docker-master",
	"Tag": "common",
	"Err": null
}
```

**Fields**

| Field | Description |
| ----- | ----------- |
| Line  | The log message itself|
 | Source | Source of the log message as reported by docker |
 | Timestamp | Timestamp that the log was collected by the log driver |
 | Partial | Whether docker reported that the log message was only partially collected |
 |ContainerName | Name of the container that generated the log message |
 | ContainerId | Id of the container that generated the log message |
 | ContainerImageName | Name of the container's image |
 | ContainerImageId | ID of the container's image |
 | Hostname | Hostname of the server running the container |
 | Tag | The log tag |
 | Err | Usually null, otherwise will be a string containing and error from the logdriver |



package main

import (
	"github.com/docker/docker/daemon/logger"
	"strings"
	"github.com/Sirupsen/logrus"
	"errors"
	"os"
)


/**
  ENVIRONMENT VARIABLES
 */
const
(
	// Minimum level for the plugin ITSELF to log at. Does not effect log level from the containers.
	ENV_LOG_LEVEL string = "LOG_LEVEL"
	DEFAULT_LOG_LEVEL string = "info"

	// Name of the environment variable the user can use to override the topic name to allow per-container topics
	ENV_TOPIC string = "LOG_TOPIC"
	DEFAULT_TOPIC string = "dockerlogs"

	// Value the logs should be tagged with
	ENV_LOG_TAG string = "LOG_TAG"
	DEFAULT_LOG_TAG string = "common"

	// Partition strategy
	ENV_PARTITION_STRATEGY string = "PARTITION_STRATEGY"

	// Key strategy
	ENV_KEY_STRATEGY string = "KEY_STRATEGY"

	// Kafka brokers in the usual host:port,host:port format. Must *always* be set
	ENV_KAFKA_BROKER_ADDR string = "KAFKA_BROKER_ADDR"
)

/**
  Special values for the ENV_TOPIC
 */
const (
	// Set the topic name to be the container name
	TOPIC__CONTAINERNAME = "$CONTAINERNAME"

	// Set the topic name to tbe the container id
 	TOPIC__CONTAINERID = "$CONTAINERID"
)

/**
Available values for partition strategy
 */
const (
	PARTITION_STRATEGY__ROUND_ROBIN string = "round_robin"
	PARTITION_STRATEGY__KEY_HASH string = "key_hash"
)
const DEFAULT_PARTITION_STRATEGY string = PARTITION_STRATEGY__ROUND_ROBIN

/**
Available values for key strategy
 */
const (
	KEY_STRATEGY__CONTAINER_ID string = "key_by_container_id"
	KEY_STRATEGY__TIMESTAMP string = "key_by_timestamp"
)
const DEFAULT_KEY_STRATEGY string = KEY_STRATEGY__TIMESTAMP

/**
Environment variable value mappings
 */

type KeyStrategy int
const (
	KEY_BY_CONTAINER_ID KeyStrategy = iota
	KEY_BY_TIMESTAMP    KeyStrategy = iota
	TAG                 string = "common"
)

type PartitionStrategy int
const (
	PARTITION_ROUND_ROBIN PartitionStrategy = iota
	PARTITION_KEY_HASH PartitionStrategy = iota
)


func getPartitionStrategyEnv() PartitionStrategy {
	partitionStrategyStr := os.Getenv(ENV_PARTITION_STRATEGY)
	if partitionStrategyStr == "" {
		partitionStrategyStr = DEFAULT_PARTITION_STRATEGY
	}
	partitionStrat, err := getPartitionStrategyFromString(partitionStrategyStr)
	if err != nil {
		logrus.Error("unknown partition strategy", err)
		os.Exit(1)
	}
	return partitionStrat
}

func getKafkaBrokersEnv() []string {
	addrList := os.Getenv(ENV_KAFKA_BROKER_ADDR)
	if addrList == "" {
		logrus.Error("Missing environment var " + ENV_KAFKA_BROKER_ADDR)
		os.Exit(1)
	}
	addrs := strings.Split(addrList, ",")
	return addrs
}

func getKafkaTopicEnv() string {
	outputTopic := os.Getenv(ENV_TOPIC)
	if outputTopic == "" {
		outputTopic = DEFAULT_TOPIC
	}
	return outputTopic
}

func getKeyStrategyEnv() KeyStrategy {
	keyStrategyVar := os.Getenv(ENV_KEY_STRATEGY)
	if keyStrategyVar == "" {
		keyStrategyVar = DEFAULT_KEY_STRATEGY
	}

	keyStrat, err := getKeyStrategyFromString(keyStrategyVar)
	if err != nil {
		logrus.Error("unknown key strategy", err)
		os.Exit(1)
	}
	return keyStrat
}

func getKeyTagEnv() string {
	tag := os.Getenv(ENV_LOG_TAG)
	if tag == "" {
		tag = DEFAULT_LOG_TAG
	}
	return tag
}


func getKeyStrategyFromString(keyStrategyString string) (KeyStrategy, error) {
	// Trim and whitespace and lowercase the string so it matches
	// no matter what someone has put in
	switch strings.TrimSpace(strings.ToLower(keyStrategyString)) {
	case KEY_STRATEGY__CONTAINER_ID:
		return KEY_BY_CONTAINER_ID, nil
	case KEY_STRATEGY__TIMESTAMP:
		return KEY_BY_TIMESTAMP, nil
	default:
		return 0, errors.New("Unknown keying strategy " + keyStrategyString +". Expected: key_by_container_id,key_by_timestamp" )
	}
}

func getPartitionStrategyFromString(partitionStrategy string) (PartitionStrategy, error) {
	// Trim and whitespace and lowercase the string so it matches
	// no matter what someone has put in
	switch strings.TrimSpace(strings.ToLower(partitionStrategy)) {
	case PARTITION_STRATEGY__ROUND_ROBIN:
		return PARTITION_ROUND_ROBIN, nil
	case PARTITION_STRATEGY__KEY_HASH:
		return PARTITION_KEY_HASH, nil
	default:
		return 0, errors.New("Unknown partition strategy" + partitionStrategy +". Expected: round_robin,key_hash")
	}
}


func getEnvVarOrDefault(logCtx logger.Info, envVarName string, defaultValue string) string {
	value := defaultValue

	for _, env := range logCtx.ContainerEnv {
		// Only split on the first '='. An equals might be present in the topic name, we don't want to split on that
		envArg := strings.SplitN(env, "=", 2)
		// Check that there was a key=value and not just a random key.
		if len(envArg) == 2 {
			envName := envArg[0]
			envValue := envArg[1]
			if strings.ToUpper(envName) == envVarName {
				logrus.WithField(envVarName, envValue).Info("environment property overriden for container")
				value = envValue
			}
		}
	}
	return value
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		//In the event that an error occured, set the hostname to an empty string as we're unable to retrieve it
		hostname = ""

		//Only log at debug level, we don't want to flood the system if this should happen for a particular reason (permissions perhaps?)
		logrus.WithField("err", err).Debug("Unable to retrieve hostname")
	}

	return hostname
}

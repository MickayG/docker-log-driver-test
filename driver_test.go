package main

import (
	"testing"
	"bytes"
	"github.com/docker/docker/api/types/plugins/logdriver"
	"time"
	"github.com/docker/docker/pkg/ioutils"
	protoio "github.com/gogo/protobuf/io"
	"encoding/binary"
	"io"
	"github.com/Shopify/sarama/mocks"
	"github.com/docker/docker/daemon/logger"
	"github.com/stretchr/testify/assert"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"os"
)


func TestConsumesSingleLogMessagesFromDocker(t *testing.T) {
	producer := NewProducer(t)
	defer producer.Close()

	logMsg := newLogEntry("alpha")

	stream := createBufferForLogMessages([]logdriver.LogEntry{logMsg})

	lf := createLogPair(producer, stream)

	producer.ExpectInputAndSucceed()
	writeLogsToKafka(&lf, "topic", KEY_BY_TIMESTAMP, TAG)

	recvMsg := <-producer.Successes()
	assertLineMatch(t, "alpha", recvMsg)
}


func TestConsumesMultipleLogMessagesFromDocker(t *testing.T) {
	producer := NewProducer(t)
	defer producer.Close()

	stream := createBufferForLogMessages([]logdriver.LogEntry{
		newLogEntry("alpha"),
		newLogEntry("beta"),
		newLogEntry("charlie"),
		newLogEntry("delta"),
	})

	lf := createLogPair(producer, stream)

	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()
	writeLogsToKafka(&lf, "topic", KEY_BY_TIMESTAMP, TAG)

	assertLineMatch(t, "alpha", <-producer.Successes())
	assertLineMatch(t, "beta", <-producer.Successes())
	assertLineMatch(t, "charlie", <-producer.Successes())
	assertLineMatch(t, "delta", <-producer.Successes())
}

func TestAggregatesPartialLogMessagesFromDocker(t *testing.T) {
	producer := NewProducer(t)
	defer producer.Close()

	stream := createBufferForLogMessages([]logdriver.LogEntry{
		newPartialLogEntry("alpha"),
		newLogEntry("beta"),
		newLogEntry("charlie\n"),
	})

	lf := createLogPair(producer, stream)

	producer.ExpectInputAndSucceed()
	writeLogsToKafka(&lf, "topic", KEY_BY_TIMESTAMP, TAG)

	assertLineMatch(t, "alphabetacharlie\n", <-producer.Successes())
}

func TestJsonIncludesContainerInformation(t *testing.T) {
	expectedContainerId := "containerid1"
	expectedContainerName := "containername1"
	expectedContainerImageName := "my/image"
	expectedContainerImageID := "23293480238"


	producer := NewProducer(t)
	defer producer.Close()

	logMsg := newLogEntry("alpha")

	stream := createBufferForLogMessages([]logdriver.LogEntry{logMsg})

	lf := createLogPair(producer, stream)
	lf.info.ContainerID = expectedContainerId
	lf.info.ContainerName = expectedContainerName
	lf.info.ContainerImageName = expectedContainerImageName
	lf.info.ContainerImageID = expectedContainerImageID


	producer.ExpectInputAndSucceed()
	writeLogsToKafka(&lf, "topic", KEY_BY_TIMESTAMP, TAG)

	recvMsg := <-producer.Successes()
	outMsg := unmarshallMessage(recvMsg, t)
	assert.Equal(t, expectedContainerId, outMsg.ContainerId)
	assert.Equal(t, expectedContainerName, outMsg.ContainerName)
	assert.Equal(t, expectedContainerImageID, outMsg.ContainerImageId)
	assert.Equal(t, expectedContainerImageName, outMsg.ContainerImageName)
}

func TestJsonIncludesHostname(t *testing.T) {
	expectedHostname,_ := os.Hostname()

	producer := NewProducer(t)
	defer producer.Close()

	logMsg := newLogEntry("alpha")

	stream := createBufferForLogMessages([]logdriver.LogEntry{logMsg})

	lf := createLogPair(producer, stream)

	producer.ExpectInputAndSucceed()
	writeLogsToKafka(&lf, "topic", KEY_BY_TIMESTAMP, TAG)

	recvMsg := <-producer.Successes()
	outMsg := unmarshallMessage(recvMsg, t)
	assert.Equal(t, expectedHostname, outMsg.Hostname)
}

func TestTagCanBeOverridenWithEnvironmentVariable(t *testing.T) {
	overrideTag := "overide"
	defaultTag := "default"

	var driver KafkaDriver
	driver.tag = defaultTag

	envVars := []string{ENV_LOG_TAG + "=" + overrideTag}
	info := logger.Info{ContainerEnv: envVars}

	tag := getTagForContainer(&driver, info)
	assert.Equal(t, overrideTag, tag)
}

func TestTopicCanBeOverridenWithEnvironmentVariable(t *testing.T) {
	overrideTopic := "override"
	defaultTopic := "default"

	var driver KafkaDriver
	driver.outputTopic = defaultTopic

	envVars := []string{ENV_TOPIC + "=" + overrideTopic}
	info := logger.Info{ContainerEnv: envVars}

	chosenTopic := getOutputTopicForContainer(&driver, info)
	assert.Equal(t, overrideTopic, chosenTopic)
}

func TestTopicDefaultsToGlobalVariableWhenNotOverriden(t *testing.T) {
	defaultTopic := "default"

	var driver KafkaDriver
	driver.outputTopic = defaultTopic

	envVars := []string{}
	info := logger.Info{ContainerEnv: envVars}

	chosenTopic := getOutputTopicForContainer(&driver, info)
	assert.Equal(t, defaultTopic, chosenTopic)
}

func TestTopicIsContainerNameWhenWanted(t *testing.T) {
	overrideTopic := "$CONTAINERNAME"
	defaultTopic := "default"

	var driver KafkaDriver
	driver.outputTopic = defaultTopic

	envVars := []string{ENV_TOPIC + "=" + overrideTopic}
	info := logger.Info{ContainerEnv: envVars, ContainerName: "containera"}

	chosenTopic := getOutputTopicForContainer(&driver, info)
	assert.Equal(t, "containera", chosenTopic)
}


func TestTopicIsContainerIdWhenWanted(t *testing.T) {
	overrideTopic := "$CONTAINERID"
	defaultTopic := "default"

	var driver KafkaDriver
	driver.outputTopic = defaultTopic

	envVars := []string{ENV_TOPIC + "=" + overrideTopic}
	info := logger.Info{ContainerEnv: envVars, ContainerName: "containera", ContainerID: "abcdef"}

	chosenTopic := getOutputTopicForContainer(&driver, info)
	assert.Equal(t, "abcdef", chosenTopic)
}

func TestReadingSingleLineFromOnePartition(t *testing.T) {
	config := sarama.NewConfig()
	consumer := mocks.NewConsumer(t, config)

	expectedLine := "alpha"
	expectedSource := "stdout"
	expectedPartial := false
	expectedTime := time.Now()
	expectedContainerId := "3423423"

	inputBytes := createLogMessage(expectedLine, expectedSource, expectedPartial, expectedTime, expectedContainerId)

	var logInfo logger.Info
	logInfo.ContainerID = expectedContainerId

	topics := make(map[string][]int32)
	topics["logtopic"] = []int32{0}
	consumer.SetTopicMetadata(topics)
	partition := consumer.ExpectConsumePartition("logtopic", 0, sarama.OffsetOldest)

	expectMessage(inputBytes, partition)

	partition.ExpectMessagesDrainedOnClose()
	r, err := readLogsFromKafka(consumer, "logtopic", logInfo, logger.ReadConfig{})

	dec := protoio.NewUint32DelimitedReader(r, binary.BigEndian, 1e6)
	var outputLogMessage logdriver.LogEntry
	err = dec.ReadMsg(&outputLogMessage)
	if err != nil {
		t.Error(err)
	}

	dec.Close()

	// New line is added on output
	assert.Equal(t, expectedLine + "\n", string(outputLogMessage.Line))
	assert.Equal(t, expectedSource, outputLogMessage.Source)
	assert.Equal(t, expectedTime.UnixNano(), outputLogMessage.TimeNano)
	assert.Equal(t, expectedPartial, outputLogMessage.Partial)
}


func TestReadingMultipleLogMessages(t *testing.T) {
	config := sarama.NewConfig()
	consumer := mocks.NewConsumer(t, config)

	expectedSource := "stdout"
	expectedPartial := false
	expectedTime := time.Now()
	expectedContainerId := "3423423"

	topics := make(map[string][]int32)
	topics["logtopic"] = []int32{0}
	consumer.SetTopicMetadata(topics)
	partition := consumer.ExpectConsumePartition("logtopic", 0, sarama.OffsetOldest)

	numberOfMessages := 100
	for i := 0; i < numberOfMessages; i++ {
		inputBytes := createLogMessage(strconv.Itoa(i), expectedSource, expectedPartial, expectedTime, expectedContainerId)
		expectMessage(inputBytes, partition)
	}

	var logInfo logger.Info
	logInfo.ContainerID = expectedContainerId

	partition.ExpectMessagesDrainedOnClose()
	r, err := readLogsFromKafka(consumer, "logtopic", logInfo, logger.ReadConfig{})

	dec := protoio.NewUint32DelimitedReader(r, binary.BigEndian, 1e6)
	count := 0
	for i := 0; i < numberOfMessages; i++ {
		var outputLogMessage logdriver.LogEntry
		err = dec.ReadMsg(&outputLogMessage)
		if err != nil {
			t.Error(err)
		}

		count++
	}

	dec.Close()

	assert.Equal(t, numberOfMessages, count)
}

func TestTailSettingOfOne(t *testing.T) {
	setLogLevel("debug")
	config := sarama.NewConfig()
	consumer := mocks.NewConsumer(t, config)

	expectedSource := "stdout"
	expectedPartial := false
	expectedTime := time.Now()
	expectedContainerId := "3423423"

	topics := make(map[string][]int32)
	topics["logtopic"] = []int32{0}
	consumer.SetTopicMetadata(topics)

	// This is the real assert here, that the offset is '100'
	partition := consumer.ExpectConsumePartition("logtopic", 0, 100)

	numberOfMessages := 100
	for i := 0; i < numberOfMessages; i++ {
		inputBytes := createLogMessage(strconv.Itoa(i), expectedSource, expectedPartial, expectedTime, expectedContainerId)
		expectMessage(inputBytes, partition)
	}
	partition.ExpectMessagesDrainedOnClose()

	var logInfo logger.Info
	logInfo.ContainerID = expectedContainerId

	readLogsFromKafka(consumer, "logtopic", logInfo, logger.ReadConfig{Tail: 1})
}

func TestReadingSingleLineFromMultiplePartitions(t *testing.T) {
	config := sarama.NewConfig()
	consumer := mocks.NewConsumer(t, config)


	expectedSource := "stdout"
	expectedPartial := false
	expectedTime := time.Now()
	expectedContainerId := "3423423"

	var logInfo logger.Info
	logInfo.ContainerID = expectedContainerId
	topics := make(map[string][]int32)
	topics["logtopic"] = []int32{1,2,3,4,5}
	consumer.SetTopicMetadata(topics)

	for _,r := range topics["logtopic"] {
		msg := createLogMessage(strconv.Itoa(int(r)), expectedSource, expectedPartial, expectedTime, expectedContainerId)
		partition := consumer.ExpectConsumePartition("logtopic", r, sarama.OffsetOldest)
		expectMessage(msg, partition)

	}

	r, err := readLogsFromKafka(consumer, "logtopic", logInfo, logger.ReadConfig{})

	expectedMessageCount := len(topics["logtopic"])

	//byteWrapper := bytes.NewReader(outputBytes)
	dec := protoio.NewUint32DelimitedReader(r, binary.BigEndian, 1e6)

	count := 0
	for i := 0 ; i < len(topics["logtopic"]); i++ {
		var outputLogMessage logdriver.LogEntry
		err = dec.ReadMsg(&outputLogMessage)
		if err != nil {
			t.Error(err)
		}
		count++
	}

	dec.Close()

	assert.Equal(t, expectedMessageCount, count)
}

func TestReadingDoesNotOutputLogsForOtherContainer(t *testing.T) {
	config := sarama.NewConfig()
	consumer := mocks.NewConsumer(t, config)

	differentContainerId := "not_the_container_we_want"


	expectedLine := "alpha"
	expectedSource := "stdout"
	expectedPartial := false
	expectedTime := time.Now()
	expectedContainerId := "3423423"

	inputBytes := createLogMessage(expectedLine, expectedSource, expectedPartial, expectedTime, expectedContainerId)

	var logInfo logger.Info
	logInfo.ContainerID = differentContainerId

	topics := make(map[string][]int32)
	topics["logtopic"] = []int32{0}
	consumer.SetTopicMetadata(topics)
	partition := consumer.ExpectConsumePartition("logtopic", 0, sarama.OffsetOldest)

	expectMessage(inputBytes, partition)

	partition.ExpectMessagesDrainedOnClose()
	r, err := readLogsFromKafka(consumer, "logtopic", logInfo, logger.ReadConfig{})

	// Wait a few seconds for the go threads to run
	time.Sleep(3 * time.Second)

	outputBytes, err := ioutil.ReadAll(r)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, 0, len(outputBytes))
}

func expectMessage(inputBytes []byte, partition *mocks.PartitionConsumer) {
	var outputMsg sarama.ConsumerMessage
	outputMsg.Value = inputBytes
	partition.YieldMessage(&outputMsg)
}


func createLogMessage(expectedLine string, expectedSource string, expectedPartial bool, expectedTime time.Time, expectedContainerId string) []byte {
	var inputMessage LogMessage
	inputMessage.Line = expectedLine
	inputMessage.Source = expectedSource
	inputMessage.Partial = expectedPartial
	inputMessage.Timestamp = expectedTime
	inputMessage.ContainerId = expectedContainerId
	inputBytes, _ := json.Marshal(inputMessage)
	return inputBytes
}


func createLogPair(producer *mocks.AsyncProducer, stream io.ReadCloser) logPair {
	var lf logPair
	lf.producer = producer
	lf.stream = stream
	lf.info = logger.Info{ContainerName: "mycontainer", ContainerID: "abcdefg"}
	return lf
}


func createBufferForLogMessages(logs []logdriver.LogEntry) io.ReadCloser {
	var buf bytes.Buffer

	protoWriter := protoio.NewUint32DelimitedWriter(&buf, binary.BigEndian)

	for _,log := range logs {
		protoWriter.WriteMsg(&log)
	}

	protoWriter.Close()

	closeFunc := func () error {
		return nil
	}

	readCloser := ioutils.NewReadCloserWrapper(&buf, closeFunc)
	return readCloser
}


func newLogEntry(line string) logdriver.LogEntry {
	var le logdriver.LogEntry
	le.Line = []byte(line)
	le.Source = "container"
	le.Partial = false
	le.TimeNano = time.Now().UnixNano()
	return le
}

func newPartialLogEntry(line string) logdriver.LogEntry {
	le := newLogEntry(line)
	le.Partial = true
	return le
}


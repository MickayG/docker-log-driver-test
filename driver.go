package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/api/types/plugins/logdriver"
	"github.com/docker/docker/daemon/logger"
	protoio "github.com/gogo/protobuf/io"
	"github.com/pkg/errors"
	"github.com/tonistiigi/fifo"
	"github.com/Shopify/sarama"
	"encoding/json"
	"strconv"
	"math"
	"strings"
)

// An mapped version of logger.Message where Line is a String, not a byte array
type LogMessage struct {
	Line               string
	Source             string
	Timestamp          time.Time
	Partial            bool
	ContainerName      string
	ContainerId        string
	ContainerImageName string
	ContainerImageId   string
	Hostname           string
	Tag                string

	// Err is an error associated with a message. Completeness of a message
	// with Err is not expected, tho it may be partially complete (fields may
	// be missing, gibberish, or nil)
	Err error

}

type LogDriver interface {
	StartLogging(file string, logCtx logger.Info) error
	StopLogging(file string) error
	ReadLogs(info logger.Info, config logger.ReadConfig) (io.ReadCloser, error)
	GetCapability() logger.Capability
}

type KafkaDriver struct {
	mu          sync.Mutex
	logs        map[string]*logPair
	idx         map[string]*logPair
	logger      logger.Logger
	client      *sarama.Client
	outputTopic string
	keyStrategy KeyStrategy
	partitionStrategy PartitionStrategy
	tag         string
}

type logPair struct {
	stream io.ReadCloser
	info   logger.Info
	producer sarama.AsyncProducer
}


// How many seconds to keep trying to consume from kafka until the connection stops
const READ_LOGS_TIMEOUT  = 10 * time.Second

func newDriver(client *sarama.Client, outputTopic string, keyStrategy KeyStrategy, tag string) *KafkaDriver {
	return &KafkaDriver{
		logs: make(map[string]*logPair),
		idx:  make(map[string]*logPair),
		client: client,
		outputTopic: outputTopic,
		keyStrategy: keyStrategy,
		tag: tag,
	}
}

func (d *KafkaDriver) StartLogging(file string, logCtx logger.Info) error {
	d.mu.Lock()
	if _, exists := d.logs[file]; exists {
		d.mu.Unlock()
		return fmt.Errorf("logger for %q already exists", file)
	}
	d.mu.Unlock()

	if logCtx.LogPath == "" {
		logCtx.LogPath = filepath.Join("/var/log/docker", logCtx.ContainerID)
	}
	if err := os.MkdirAll(filepath.Dir(logCtx.LogPath), 0755); err != nil {
		return errors.Wrap(err, "error setting up logger dir")
	}

	logrus.WithField("id", logCtx.ContainerID).WithField("file", file).WithField("logpath", logCtx.LogPath).Debugf("Start logging")
	f, err := fifo.OpenFifo(context.Background(), file, syscall.O_RDONLY, 0700)
	if err != nil {
		return errors.Wrapf(err, "error opening logger fifo: %q", file)
	}

	d.mu.Lock()

	producer, err := CreateProducer(d.client)
	if err != nil {
		return errors.Wrapf(err,"unable to create kafka consumer")
	}

	// The user can specify a custom topic with the below argument. If its present, use that as the
	//   topic instead of the global configuration option
	outputTopic := getOutputTopicForContainer(d, logCtx)
	tag := getTagForContainer(d, logCtx)

	lf := &logPair{f, logCtx, producer}
	d.logs[file] = lf
	d.idx[logCtx.ContainerID] = lf

	d.mu.Unlock()

	go writeLogsToKafka(lf, outputTopic, d.keyStrategy, tag)

	return nil
}

func (d *KafkaDriver) StopLogging(file string) error {
	logrus.WithField("file", file).Debugf("Stop logging")
	d.mu.Lock()
	lf, ok := d.logs[file]
	if ok {
		lf.stream.Close()
		delete(d.logs, file)

		lf.producer.Close()
	}
	d.mu.Unlock()
	return nil
}

func (d* KafkaDriver) GetCapability() logger.Capability {
	return logger.Capability{ReadLogs: true}
}

func (d *KafkaDriver) ReadLogs(info logger.Info, config logger.ReadConfig) (io.ReadCloser, error) {
	logrus.WithField("tail", config.Tail).WithField("follow", config.Follow).WithField("since", config.Since).WithField("container", info.ContainerName).Info("Recieved read request for container")

	logTopic := getOutputTopicForContainer(d, info)
	consumer,err := sarama.NewConsumerFromClient(*d.client)
	if err != nil {
		return nil, errors.WithMessage(err, "Unable to create consumer for logs")
	}

	return readLogsFromKafka(consumer, logTopic, info, config)
}


func writeLogsToKafka(lf *logPair, topic string, keyStrategy KeyStrategy, tag string) {
	dec := protoio.NewUint32DelimitedReader(lf.stream, binary.BigEndian, 1e6)
	defer dec.Close()
	var buf logdriver.LogEntry

	var partialBuffer string
	for {
		// Check if there are any Kafka errors thus far
		select {
			case kafkaErr := <- lf.producer.Errors():
				// In the event of an error, continue to attempt to write messages
				logrus.Error("error received from Kafka", kafkaErr)
			default:
				//No errors, continue
		}

		if err := dec.ReadMsg(&buf); err != nil {
			if err == io.EOF {
				logrus.WithField("id", lf.info.ContainerID).WithError(err).Debug("shutting down log logger")
				lf.stream.Close()
				return
			}
			logrus.WithField("id", lf.info.ContainerID).WithError(err).Error("error whilst reading from log")
			dec = protoio.NewUint32DelimitedReader(lf.stream, binary.BigEndian, 1e6)
		}

		lineAsString := string(buf.Line)

		// In the event that the message is partial, we attempt to aggregate if possible
		switch {
			case buf.Partial:
				logrus.WithField("line", lineAsString).WithField("buffer", partialBuffer).Debug("Received partial message, start reconstruction")
				fallthrough
			case len(partialBuffer) > 0 && !strings.HasSuffix(lineAsString, "\n"):
				logrus.WithField("line", lineAsString).WithField("buffer", partialBuffer).Debug("Received partial message, continue reconstruction")
				partialBuffer += lineAsString
				continue
			case len(partialBuffer) > 0 && strings.HasSuffix(lineAsString, "\n"):
				logrus.WithField("line", lineAsString).WithField("buffer", partialBuffer).Debug("Received partial message, finish reconstruction")
				// Add the remaining line to the buffer and reset the line to be the total buffer size
				partialBuffer += lineAsString
				lineAsString = partialBuffer
				partialBuffer = ""
		}

		var msg LogMessage
		msg.Line = lineAsString
		msg.Source = buf.Source
		msg.Partial = buf.Partial
		msg.Timestamp = time.Unix(0, buf.TimeNano)
		msg.ContainerId = lf.info.ContainerID
		msg.ContainerName = lf.info.ContainerName
		msg.ContainerImageName = lf.info.ContainerImageName
		msg.ContainerImageId = lf.info.ContainerImageID
		msg.Hostname = getHostname()
		msg.Tag = tag

		err := WriteMessage(topic, msg, lf.info.ContainerID, keyStrategy, lf.producer)
		if err != nil {
			logrus.WithField("id", lf.info.ContainerID).WithField("msg", msg).Error("Unable to write message to kafka", err)
		}

		buf.Reset()
	}
}

func readLogsFromKafka(consumer sarama.Consumer, logTopic string, info logger.Info, config logger.ReadConfig) (io.ReadCloser, error) {
	partitions, err := consumer.Partitions(logTopic)
	if err != nil {
		return nil, errors.WithMessage(err, "Unable to list partitions for topic " + logTopic)
	}

	highWaterMarks := consumer.HighWaterMarks()

	r, w := io.Pipe()

	// This channel will be used for the consumers to push data into, and the writes to write data from
	logMessages := make(chan logdriver.LogEntry)
	halt := make(chan bool)

	var wg sync.WaitGroup

	// We need to create a consumer for each partition as the Sarama library only reads from one partition at at a time
	for _,partition := range partitions {
		logrus.WithField("topic", logTopic).Debug("Reading partition: " + strconv.Itoa(int(partition)))

		//Default offset to oldest
		offset := sarama.OffsetOldest
		if config.Tail != -1 && config.Tail != 0{
			hwm := highWaterMarks[logTopic][partition]
			offset = hwm - int64(config.Tail)
			// The offset cannot be less than 0, unless it's a magic number
			offset = int64(math.Max(0, float64(offset)))
			logrus.Debug("Reading ", logTopic, " partition ", strconv.Itoa(int(partition)), " with offset ", strconv.Itoa(int(offset)), " with high water mark of ", hwm)
		}

		wg.Add(1)
		go consumeFromTopic(consumer, logTopic, int32(partition), offset, info.ContainerID, config.Follow, logMessages, halt, &wg)
	}

	// This method will read from the logMessages channel and write the messages to protobuf
	go writeLogsToWriter(w, logMessages, halt)

	// If all consumers stop, then inform the writeLogsToOutput routine to stop
	go func() {
		wg.Wait()
		safelyClose(halt)
	}()

	return r, nil
}

func writeLogsToWriter(w *io.PipeWriter, entries chan logdriver.LogEntry, halt chan bool) {
	enc := protoio.NewUint32DelimitedWriter(w, binary.BigEndian)
	defer enc.Close()
	// If this method returns and stop writing logs, we also want to send a message to the kafka consumers
	// to tell them to stop consuming too
	//defer close(halt)

	for {
		select {
		case logEntry := <-entries:
			err := enc.WriteMsg(&logEntry)
			if err != nil {
				logrus.Error("Unable to write out log message. This may be due to the user closing the stream ", err)
				w.CloseWithError(err)
				safelyClose(halt)
				return
			}
		case _, ok :=<-halt:
			if !ok {
				w.Close()
				return
			}

		default:
		}
	}


}

func consumeFromTopic(consumer sarama.Consumer, topic string, partition int32, offset int64, containerId string, noTimeout bool, logMessages chan logdriver.LogEntry, halt chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	logrus.WithField("topic", topic).WithField("partition", partition).WithField("offset", offset).WithField("containerId", containerId).Info("Beginning consuming of messages")

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		// Log an error and close the channel, effectively stopping all the consumers
		logrus.WithField("topic", topic).WithField("partition", partition).Error("Unable to consume from partition", err)
		close(logMessages)
		return
	}

	defer partitionConsumer.Close()

	lastMessage := time.Now()

	for {
		select {
		case msg, ok := <-partitionConsumer.Messages():
			if !ok {
				logrus.WithField("topic", topic).WithField("partition", partition).Error("Consuming from partition stopped", err)
				return
			}

			var logMessage LogMessage
			json.Unmarshal(msg.Value, &logMessage)
			// If the container ids are the same, then output the logs
			if logMessage.ContainerId == containerId {
				//Now recreate the input protobuf logentry
				var logEntry logdriver.LogEntry
				logEntry.TimeNano = logMessage.Timestamp.UnixNano()

				// Note that we also add a newline to the end. It was striped off when it was intially
				// written to Kafka.
				logEntry.Line = []byte(logMessage.Line + "\n")
				logEntry.Partial= logMessage.Partial
				logEntry.Source = logMessage.Source

				// Add it to the channel for the next routine to write it out
				logMessages <- logEntry
			}

		case <-halt:
			// It is likely that the consumer of the logs has closed the stream. We should therefore stop consuming
			logrus.Debug("Halting log reading")
			return

		default:
			if !noTimeout && time.Now().Sub(lastMessage) > READ_LOGS_TIMEOUT {
				logrus.Debug("Closing consumer, waited 10 seconds for additional logs")
				return
			}
		}

	}
}

func getOutputTopicForContainer(d *KafkaDriver, logCtx logger.Info) string {
	topicName := getEnvVarOrDefault(logCtx, ENV_TOPIC, d.outputTopic)

	if topicName == TOPIC__CONTAINERNAME {
		topicName = logCtx.ContainerName
	} else if topicName == TOPIC__CONTAINERID {
		topicName = logCtx.ContainerID
	}

	return topicName
}

func getTagForContainer(d *KafkaDriver, logCtx logger.Info) string {
	return getEnvVarOrDefault(logCtx, ENV_LOG_TAG, d.tag)
}



func safelyClose(ch chan bool) {
	defer func () {
		if e := recover(); e!= nil {
			logrus.Error("Closing channel errored")
		}
	}()

	close(ch)
}
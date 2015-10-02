package main

import (
	"fmt"

	"github.com/blinsay/ktk/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws"
	"github.com/blinsay/ktk/Godeps/_workspace/src/github.com/aws/aws-sdk-go/service/kinesis"
)

const MaxSendSize = 500

// An interface for any type that can be encoded as a Kinesis message. Types
// must provide a non-nil PartitionKey and Value.
type KinesisMessage interface {
	PartitionKey() (*string, error)
	Value() ([]byte, error)
}

// A type that lets a string be used as a producer message. The first 256
// characters of the string (the maximum size of a Kinesis partition key!) are
// used as the partition key. The whole string is used as the value.
type StringMessage string

func intMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (s StringMessage) PartitionKey() (*string, error) {
	return aws.String(string(s[:intMin(len(s), 256)])), nil
}

func (s StringMessage) Value() ([]byte, error) {
	return []byte(s), nil
}

// A message that wasn't put to Kinesis due to some error.
//
// If the record failed to be put due to a service error, the error code and
// message returned by kinesis are included.
type FailedPut struct {
	Message      KinesisMessage
	ErrorCode    *string
	ErrorMessage *string
}

// A Producer sends the given partition-key/value pair to Kinesis using some
// underlying client. A Producer may buffer data or send it to kinesis
// asynchronously.
type Producer interface {
	// Send the given message to Kinesis at some point in the future. Put may
	// return an error even if a send to Kinesis was not triggered if the message
	// fails validation for any reason.
	//
	// If this put triggers sending messages to Kinesis, any number of messages
	// previously passed to this client may fail and be returned.
	Put(KinesisMessage) ([]FailedPut, error)
}

// A producer that buffers data internally and sends one a batch of messages
// reaches a specific size.
//
// This producer isn't safe to call from multiple Goroutines. Callers must
// synchronize their own access to Put and Close.
type BufferedProducer struct {
	// The Stream data is being sent to. This shouldn't change during use.
	StreamName string
	// The number of messages that are buffered internally before sending data
	// to Kinesis.
	SendSize int

	client kinesis.Kinesis

	// The next open slot to store data. This is also the current number of
	// messages buffered in the client. Any message, partition key, or value
	// at or after this index should be considered invalid.
	current int
	// The arrays of messages to be sent and their pre-computed partition keys
	// and values. The keys and values are pre-computed so that Put can fail fast
	// on any record where a KinesisMessage method returns an error.
	messages      []KinesisMessage
	partitionKeys []*string
	values        [][]byte
}

func NewBufferedProducer(stream string, client kinesis.Kinesis, sendSize int) (*BufferedProducer, error) {
	if sendSize > MaxSendSize {
		return nil, fmt.Errorf("sendSize too large")
	}

	p := &BufferedProducer{
		StreamName: stream,
		SendSize:   sendSize,
		client:     client,

		// Ensure that the slices are all initialized and ready to go. The Put and
		// send code rely on these not being nil slices.
		messages:      make([]KinesisMessage, sendSize),
		partitionKeys: make([]*string, sendSize),
		values:        make([][]byte, sendSize),
	}
	return p, nil
}

func validValue(msg KinesisMessage) ([]byte, error) {
	val, err := msg.Value()
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, fmt.Errorf("msg.Value must be at least 1 byte long")
	}
	return val, nil
}

func validPartitionKey(msg KinesisMessage) (*string, error) {
	key, err := msg.PartitionKey()
	if err != nil {
		return nil, err
	}
	if len(*key) == 0 {
		return nil, fmt.Errorf("msg.PartitionKey must be a non-empty string")
	}
	return key, nil
}

func (b *BufferedProducer) Put(msg KinesisMessage) ([]FailedPut, error) {
	// Fail fast on messages that can't be encoded. It's worth the extra storage
	// to memoize results to know that encoding is busted sooner rather than later.
	pkey, err := validPartitionKey(msg)
	if err != nil {
		return nil, err
	}
	val, err := validValue(msg)
	if err != nil {
		return nil, err
	}

	// Buffer the message.
	b.messages[b.current] = msg
	b.partitionKeys[b.current] = pkey
	b.values[b.current] = val
	b.current++

	// Maybe send the data
	if b.current == b.SendSize {
		return b.send()
	}
	return nil, nil
}

func (b *BufferedProducer) Flush() ([]FailedPut, error) {
	return b.send()
}

func (b *BufferedProducer) reset() {
	b.messages = make([]KinesisMessage, b.SendSize)
	b.partitionKeys = make([]*string, b.SendSize)
	b.values = make([][]byte, b.SendSize)
	b.current = 0
}

func (b *BufferedProducer) send() ([]FailedPut, error) {
	// Don't send with no data.
	if b.current == 0 {
		return nil, nil
	}

	// Build the request
	entries := make([]*kinesis.PutRecordsRequestEntry, b.current)
	for i := 0; i < b.current; i++ {
		entries[i] = &kinesis.PutRecordsRequestEntry{
			PartitionKey: b.partitionKeys[i],
			Data:         b.values[i],
		}
	}
	input := &kinesis.PutRecordsInput{
		StreamName: aws.String(b.StreamName),
		Records:    entries,
	}

	defer b.reset()
	result, err := b.client.PutRecords(input)
	// There was a client error. Every message failed to get put. Return them
	// all with no ErrorCode or ErrorMessage set. The client error is returned
	// alongside.
	if err != nil {
		failures := make([]FailedPut, len(entries))
		for i := 0; i < b.current; i++ {
			failures[i] = FailedPut{Message: b.messages[i]}
		}
		return failures, err
	}

	// Some (but maybe not all!) records failed. All records are returned in the
	// order that they were sent. Effectively zip them with the sent records
	// and grab the failed ones.
	failedRecordCount := *result.FailedRecordCount
	if failedRecordCount > 0 {
		failureCount := 0
		failures := make([]FailedPut, failedRecordCount)
		for i, record := range result.Records {
			if record.ErrorCode != nil {
				failures[failureCount] = FailedPut{
					Message:      b.messages[i],
					ErrorCode:    record.ErrorCode,
					ErrorMessage: record.ErrorMessage,
				}
				failureCount++
			}
		}
		return failures, nil
	}

	// Everything is cool.
	return nil, nil
}

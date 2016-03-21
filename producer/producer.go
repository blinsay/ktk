package producer

import (
	"errors"
	"log"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/hashicorp/go-multierror"
)

const MaxSendSize = 500

var (
	EmptyPartitionKey   = errors.New("Partition keys may not be empty")
	InvalidUnicode      = errors.New("Partition key must be valid unicode")
	PartitionKeyTooLong = errors.New("Partition key must be at most 256 characters")
	EmptyValue          = errors.New("Value must not be empty")
)

// An interface that covers the way the producer uses kinesis.Kinesis so that
// the client can be stubbed out for tests.
type kinesisClient interface {
	PutRecords(*kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

// A pair type for buffering input.
type message struct {
	PartitionKey *string
	Value        []byte
}

// A Producer that buffers requests internally and sends a PutRecords request to
// Kinesis once enough data has been buffered internally.
//
// Individual record failures will be automatically retried with an exponential
// backoff until they succeed. This is useful in `ktk cat` where it's sane to
// just log a message to the user using the default logger and keep trying, but
// may not be ideal for other use cases.
//
// Producers cannot be safely used by multiple goroutines. Callers should
// synchronize access.
type Producer struct {
	StreamName string
	SendSize   int

	Throttle func() Throttle
	Debug    bool

	client   kinesisClient
	current  int
	messages []message
}

// Create a new Producer with the max Kinesis send size and the default AWS
// Kinesis client. Any Kinesis InternalFailures or
// ProvisionedThroughputExceededExceptions will be retried automatically until
// they succeed, using an exponential backoff.
//
// To configure a client more fully, set SendSize and Client before usage. They
// *must* be set before the first call to Put, otherwise behavior is undefined.
// SendSize must be >= 0 and <= MaxSendSize.
func New(stream string) *Producer {
	return &Producer{
		StreamName: stream,
		SendSize:   MaxSendSize,
		client:     kinesis.New(nil),
		messages:   make([]message, MaxSendSize),
		Throttle: func() Throttle {
			return &exponentialThrottle{
				unit:    time.Millisecond,
				waitFor: 500,
				maxWait: 10000,
			}
		},
	}
}

// Send the given string to Kinesis. The first 256 bytes of the string will be
// used as the partition key. message must not be a valid Unicode string, and
// must be non-empty.
func (p *Producer) PutString(message string) error {
	return p.Put(aws.String(message[:intMin(len(message), 256)]), []byte(message))
}

func intMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Send the given key-value pair to Kinesis. Partition keys must be non-empty
// unicode strings of up to 256 characters. Values may be up to 1MB in size.
//
// TODO: what happens if we just let Kinesis error on bad records?
func (p *Producer) Put(key *string, value []byte) error {
	if err := validate(key, value); err != nil {
		return err
	}

	p.messages[p.current] = message{key, value}
	p.current++

	if p.current == p.SendSize {
		return p.send()
	}

	return nil
}

func validate(key *string, value []byte) error {
	var err *multierror.Error

	if len(*key) == 0 {
		err = multierror.Append(err, EmptyPartitionKey)
	}
	if len(*key) > 256 {
		err = multierror.Append(err, PartitionKeyTooLong)
	}
	if !utf8.ValidString(*key) {
		err = multierror.Append(err, InvalidUnicode)
	}
	if len(value) == 0 {
		err = multierror.Append(err, EmptyValue)
	}

	return err.ErrorOrNil()
}

// Flush any buffered data to Kinesis.
func (p *Producer) Flush() error {
	return p.send()
}

func (p *Producer) reset() {
	p.current = 0
	p.messages = make([]message, p.SendSize)
}

func (p *Producer) send() error {
	if len(p.messages) == 0 {
		return nil
	}

	defer p.reset()

	stream, messages := aws.String(p.StreamName), p.messages[0:p.current]
	for {
		res, err := p.client.PutRecords(putRecordsInput(stream, messages))

		if err != nil {
			return err
		}

		if *res.FailedRecordCount == 0 {
			if p.Debug {
				log.Printf("Put %d message(s).", len(res.Records))
			}
			return nil
		}

		messages = failedMessages(messages, res.Records)
		if p.Debug {
			log.Printf("Put failed for %d message(s). Backing off and trying again.", *res.FailedRecordCount)
		}
		p.Throttle().Await()
	}
	return nil
}

func putRecordsInput(stream *string, messages []message) *kinesis.PutRecordsInput {
	entries := make([]*kinesis.PutRecordsRequestEntry, len(messages))
	for i, m := range messages {
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         m.Value,
			PartitionKey: m.PartitionKey,
		}
	}

	return &kinesis.PutRecordsInput{
		StreamName: stream,
		Records:    entries,
	}
}

func failedMessages(messages []message, records []*kinesis.PutRecordsResultEntry) []message {
	var resend []message
	for i, e := range records {
		if e.ErrorCode != nil {
			resend = append(resend, messages[i])
		}
	}
	return resend
}

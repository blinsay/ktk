package consumer

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// A func passed to a Consumer and called on all of the incoming records
// for a stream. This func will be called concurrently from multiple goroutines.
type Processor func([]*kinesis.Record)

type kinesisClient interface {
	DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error)
	GetShardIterator(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error)
}

// A consumer consumes a Kinesis stream from LATEST on every shard. Consumers
// should be created with Tail - the zero value is non-functional.
//
// Consumers are designed to be used in `ktk tail` where streams are consumed
// until the process ends. This means they totally ignore checkpoints, can't
// start anywhere but LATEST on every shard, and can't be shutdown cleanly.
type Consumer struct {
	stream    *string
	client    kinesisClient
	processor Processor

	debug bool

	complete   chan string
	waiterFunc func() waiter
}

// Start a consumer at the given Stream's LATEST and process each shard with
// processor.
//
// Each shard will be processed in an individual goroutine.
func Tail(stream string, debug bool, processor Processor) error {
	c := &Consumer{
		stream:    aws.String(stream),
		client:    kinesis.New(nil),
		processor: processor,

		debug: debug,

		complete:   make(chan string),
		waiterFunc: func() waiter { return &realWaiter{} },
	}

	return c.tail()
}

// Start consuming the stream from LATEST and pass every consumed record to
// processor. Resharding will be handled automatically.
//
// Consumption and processing happens in multiple goroutines in the background.
func (c *Consumer) tail() error {
	shards, err := c.listShards()
	if err != nil {
		return err
	}

	go c.monitor()

	for _, id := range withNoChildren(shards) {
		c.startShardConsumer(id, LATEST, c.processor)
	}

	return nil
}

var LATEST = aws.String(kinesis.ShardIteratorTypeLatest)
var TRIM_HORIZON = aws.String(kinesis.ShardIteratorTypeTrimHorizon)

func (c *Consumer) startShardConsumer(shard string, iterType *string, processor Processor) {
	s := &shardConsumer{
		client:    c.client,
		stream:    c.stream,
		shard:     aws.String(shard),
		debug:     c.debug,
		processor: processor,

		waiter:   c.waiterFunc(),
		complete: c.complete,
	}

	go func() {
		s.init(iterType)
		s.consume()
	}()
}

// shard monitor

func (c *Consumer) monitor() {
	for {
		completeShard := <-c.complete

		shards, err := c.listShards()
		maybePanic(err)

		for _, s := range c.nextShards(completeShard, shards) {
			c.startShardConsumer(*s.ShardId, TRIM_HORIZON, c.processor)
		}
	}
}

// block for a given duration
type waiter interface {
	wait(duration time.Duration) <-chan time.Time
}

type realWaiter struct{}

func (r *realWaiter) wait(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

type stubWaiter struct{}

func (s *stubWaiter) wait(duration time.Duration) <-chan time.Time {
	c := make(chan time.Time)
	close(c)
	return c
}

// shard consumer

type shardConsumer struct {
	client    kinesisClient
	stream    *string
	shard     *string
	processor Processor
	debug     bool

	iterator *string

	waiter   waiter
	complete chan string
}

func maybePanic(err error) {
	if err != nil {
		panic(err)
	}
}

func (s *shardConsumer) log(fmt string, args ...interface{}) {
	if s.debug {
		log.Printf(fmt, args...)
	}
}

func (s *shardConsumer) init(iterType *string) {
	s.log("%s: starting consumer at %s", *s.shard, *iterType)

	resp, err := s.client.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        s.stream,
		ShardId:           s.shard,
		ShardIteratorType: iterType,
	})

	maybePanic(err)
	s.iterator = resp.ShardIterator
}

func (s *shardConsumer) consume() {
	waitTime := 250 * time.Millisecond
	maxWaitTime := 10 * time.Second

	for {
		resp, err := s.client.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: s.iterator,
		})

		if err != nil {
			if throughputExceeded(err) {
				s.log("%s: throughput exceeded. backing off for %dms\n", *s.shard, int64(waitTime/time.Millisecond))
				<-s.waiter.wait(waitTime)

				waitTime = maybeDouble(waitTime, maxWaitTime)
				continue
			} else {
				panic(err)
			}
		} else {
			waitTime = 250 * time.Millisecond
		}

		s.iterator = resp.NextShardIterator
		s.log("%s: processing %d records\n", *s.shard, len(resp.Records))
		s.processor(resp.Records)

		if s.iterator == nil {
			break
		}
	}

	s.complete <- *s.shard
}

func maybeDouble(current, max time.Duration) time.Duration {
	next := 2 * current
	if next > max {
		return max
	}
	return next
}

func throughputExceeded(err error) bool {
	if err == nil {
		return false
	}

	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == "ProvisionedThroughputExceededException" {
			return true
		}
	}
	return false
}

// getting and filtering shards

func (c *Consumer) listShards() ([]*kinesis.Shard, error) {
	var shards []*kinesis.Shard
	for {
		resp, err := c.client.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: c.stream,
		})

		if err != nil {
			return nil, err
		}

		for _, shard := range resp.StreamDescription.Shards {
			shards = append(shards, shard)
		}

		if !*resp.StreamDescription.HasMoreShards {
			break
		}
	}
	return shards, nil
}

func withNoChildren(shards []*kinesis.Shard) []string {
	hasChildren := make(map[string]bool)

	for _, s := range shards {
		if s.AdjacentParentShardId != nil {
			hasChildren[*s.AdjacentParentShardId] = true
		}
		if s.ParentShardId != nil {
			hasChildren[*s.ParentShardId] = true
		}
	}

	var shardIds []string
	for _, s := range shards {
		if !hasChildren[*s.ShardId] {
			shardIds = append(shardIds, *s.ShardId)
		}
	}

	return shardIds
}

func (c *Consumer) nextShards(finished string, shards []*kinesis.Shard) []*kinesis.Shard {
	var next []*kinesis.Shard
	for _, s := range shards {
		if s.ParentShardId != nil && finished == *s.ParentShardId {
			next = append(next, s)
		}
	}
	return next
}

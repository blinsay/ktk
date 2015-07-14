package main

// FIXME: support resharding
// FIXME: suppot consuming from a checkpoint

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

// A record processor. Passed to a Consumer and used to process all of the
// incoming records on a stream. The same processor will be called multiple
// times from multiple goroutines, so the Process function must be thread-safe.
type Processor interface {
	// Process is called on every batch of records returned from a shard. Records
	// are passed in order of increasing sequence number both within and between
	// batches.
	//
	// Calls to panic in this function are assumed to be a sign of something
	// seriously wrong and will cause a crash.
	Process([]*kinesis.Record)
}

// An adapter that allows ordinary functions to be used as Processors.
type ProcessorFunc func([]*kinesis.Record)

func (p ProcessorFunc) Process(records []*kinesis.Record) {
	p(records)
}

// A high level Kinesis client that acts as a Processor multiplexer. It tracks
// the shard state of the streams it processes and handles splits and merges
// while ensuring that all records are processed in order.
type Consumer struct {
	client *kinesis.Kinesis
}

// Return a new Consumer.
func NewConsumer(client *kinesis.Kinesis) *Consumer {
	return &Consumer{client}
}

func (c *Consumer) TailFunc(stream string, processor ProcessorFunc) {
	c.Tail(stream, processor)
}

// Process the given stream starting from the LATEST record. Returns an error if
// there's an issue getting shard information.
func (c *Consumer) Tail(stream string, processor Processor) error {
	shards, err := c.activeShards(stream)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		s := &shardConsumer{
			client:    c.client,
			stream:    stream,
			shard:     shard,
			processor: processor,
		}

		go func(s *shardConsumer) {
			s.getLatestIterator()
			s.consume()
		}(s)
	}

	return nil
}

func (c *Consumer) activeShards(stream string) ([]string, error) {
	resp, err := c.client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		return nil, err
	}
	desc := resp.StreamDescription

	// TODO: handle the case where a merge/split has happened in the last 24h
	shards := make([]string, len(desc.Shards))
	for i, shard := range desc.Shards {
		shards[i] = *shard.ShardID
	}
	return shards, nil
}

// A shardConsumer reads records from a single shard in a Kinesis stream.
type shardConsumer struct {
	client        *kinesis.Kinesis
	stream        string
	shard         string
	shardIterator *string
	processor     Processor
}

func (s *shardConsumer) getLatestIterator() {
	resp, err := s.client.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        aws.String(s.stream),
		ShardID:           aws.String(s.shard),
		ShardIteratorType: aws.String("LATEST"),
	})
	if err != nil {
		// FIXME: figure out how to return errors here
		panic(err)
	}
	s.shardIterator = resp.ShardIterator
}

func (s *shardConsumer) consume() {
	for {
		// Kinesis has a hard limit of 5 transactions per second per shard. Try
		// not to violate that by reading too fast.
		throttle := time.After(200 * time.Millisecond)

		resp, err := s.client.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: s.shardIterator,
		})
		if err != nil {
			// FIXME: figure out how to return errors here
			panic(err)
		}

		s.shardIterator = resp.NextShardIterator
		s.processor.Process(resp.Records)

		<-throttle
	}
}

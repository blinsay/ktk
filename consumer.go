package main

// FIXME: support resharding
// FIXME: suppot consuming from a checkpoint

import "github.com/awslabs/aws-sdk-go/service/kinesis"

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
	client kinesis.Kinesis
}

// Return a new Consumer.
func NewConsumer(client kinesis.Kinesis) *Consumer {
	return &Consumer{client}
}

// Process the given stream starting from the LATEST record. Returns an error if
// there's an issue getting shard information.
func (c *Consumer) Tail(stream string, processor Processor) error {
	shards, err := c.activeShards(stream)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		consumer := &shardConsumer{
			stream:    stream,
			shard:     shard,
			processor: processor,
		}
		go consumer.consume()
	}
	return nil

}

func (c *Consumer) activeShards(stream string) ([]string, error) {
	desc, err := describeStream(c.client, stream)
	if err != nil {
		return nil, err
	}

	// TODO: handle the case where a merge/split has happened in the last 24h
	shards := make([]string, len(desc.Shards))
	for i, shard := range desc.Shards {
		shards[i] = *shard.ShardID
	}
	return shards, nil
}

func describeStream(client kinesis.Kinesis, stream string) (*kinesis.StreamDescription, error) {
	return nil, nil
}

// A shardConsumer reads records from a single shard in a Kinesis stream.
type shardConsumer struct {
	stream        string
	shard         string
	processor     Processor
	shardIterator string
}

// Consume the entirety of a Kinesis shard, passing all records to
func (c *shardConsumer) consume() {
	for {
		c.getNextIterator()
		c.processor.Process(c.getRecords())
	}
}

func (c *shardConsumer) getNextIterator() {
	// FIXME
}

func (c *shardConsumer) getRecords() []*kinesis.Record {
	return nil
}

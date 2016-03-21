package consumer

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	defaultStream         = "test_stream"
	defaultPartitionKey   = "partition_key"
	defaultSequenceNumber = "seq_num"
)

// test that once a consumer tails every shard to completion, it sends a message
// back on the completed chan and exits.
func TestConsume(t *testing.T) {
	testCases := []struct {
		name         string
		descriptions [][]shard
		data         map[string][]string
	}{
		{
			name: "one shard",
			descriptions: [][]shard{
				{{id: "shard-01"}},
			},
			data: map[string][]string{
				"shard-01": {"twinkle", "twinkle"},
			},
		},
		{
			name: "three shards",
			descriptions: [][]shard{
				{{id: "shard-01"}, {id: "shard-02"}, {id: "shard-03"}},
			},
			data: map[string][]string{
				"shard-01": {"twinkle", "twinkle"},
				"shard-02": {"hey", "there", "lil", "fella"},
				"shard-03": {"nah"},
			},
		},
		{
			name: "two shards, one closed",
			descriptions: [][]shard{
				{{id: "shard-01", closed: true}, {id: "shard-02"}, {id: "shard-03"}},
			},
			data: map[string][]string{
				"shard-02": {"hey", "there", "lil", "fella"},
				"shard-03": {"nah"},
			},
		},
		{
			name: "one shard that splits",
			descriptions: [][]shard{
				{
					{id: "shard-01"},
				},
				{
					{id: "shard-01", closed: true},
					{id: "shard-02", parentOne: "shard-01"},
					{id: "shard-03", parentOne: "shard-01"},
				},
			},
			data: map[string][]string{
				"shard-01": {"twinkle", "twinkle"},
				"shard-02": {"hey", "there", "lil", "fella"},
				"shard-03": {"nah"},
			},
		},
		{
			name: "two shards that merge",
			descriptions: [][]shard{
				{
					{id: "shard-01"},
					{id: "shard-02"},
				},
				{
					{id: "shard-01", closed: true},
					{id: "shard-02", closed: true},
					{id: "shard-03", parentOne: "shard-01", parentTwo: "shard-02"},
				},
			},
			data: map[string][]string{
				"shard-01": {"twinkle", "twinkle"},
				"shard-02": {"hey", "there", "lil", "fella"},
				"shard-03": {"nah"},
			},
		},
	}

	for _, testCase := range testCases {
		consumed := make(chan string)
		c := consumerWith(testCase.descriptions, testCase.data, func(records []*kinesis.Record) {
			for _, record := range records {
				consumed <- string(record.Data)
			}
		})

		c.tail()
		expectedRecords := getRecords(testCase.data)
		actualRecords := takeTimes(len(expectedRecords), consumed)

		sort.Sort(sort.StringSlice(expectedRecords))
		sort.Sort(sort.StringSlice(actualRecords))
		if !reflect.DeepEqual(actualRecords, expectedRecords) {
			t.Errorf(`%s: expected records not equal to consumed records

			actual:   %+v
			expected: %+v`, testCase.name, actualRecords, expectedRecords)
		}
	}
}

// helpers

func getRecords(m map[string][]string) []string {
	var records []string

	for _, v := range m {
		records = append(records, v...)
	}

	return records
}

func takeTimes(n int, ch chan string) []string {
	var got []string
	for i := 0; i < n; i++ {
		got = append(got, <-ch)
	}
	return got
}

func consumerWith(descriptions [][]shard, data map[string][]string, processor Processor) *Consumer {
	return &Consumer{
		stream:     aws.String(defaultStream),
		client:     &StubClient{describe: descriptions, records: data},
		complete:   make(chan string),
		processor:  processor,
		waiterFunc: func() waiter { return &stubWaiter{} },
	}
}

// a struct for describing shards
type shard struct {
	id                   string
	parentOne, parentTwo string
	closed               bool
}

// A stub client for setting up tests/data.
type StubClient struct {
	sync.Mutex

	describe [][]shard
	records  map[string][]string
}

// NOTE: calls are totally synchronized for sanity
func (s *StubClient) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	s.Lock()
	defer s.Unlock()

	// always leave the last description in the stream. should never run out.
	if len(s.describe) == 0 {
		return nil, fmt.Errorf("ran out of shards. test over")
	}

	var shards []shard
	if len(s.describe) == 1 {
		shards = s.describe[0]
	} else {
		shards, s.describe = s.describe[0], s.describe[1:]
	}

	output := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			HasMoreShards: aws.Bool(false),
			Shards:        shardsToAws(shards...),
		},
	}
	return output, nil
}

// always return the shard id as the iterator.
func (s *StubClient) GetShardIterator(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	output := &kinesis.GetShardIteratorOutput{
		ShardIterator: input.ShardId,
	}
	return output, nil
}

// NOTE: not synchronized. caller's responsibility
func (s *StubClient) getNextRecord(shardId string) string {
	records := s.records[shardId]
	if len(records) == 0 {
		return ""
	}

	var record string
	record, s.records[shardId] = records[0], records[1:]
	return record
}

// NOTE: calls are totally synchronized for sanity
func (s *StubClient) GetRecords(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	s.Lock()
	defer s.Unlock()

	var nextIterator *string
	nextRecord := s.getNextRecord(*input.ShardIterator)
	if nextRecord != "" {
		nextIterator = input.ShardIterator
	}

	output := &kinesis.GetRecordsOutput{
		MillisBehindLatest: aws.Int64(123),
		NextShardIterator:  nextIterator,
		Records:            makeRecords(nextRecord),
	}

	return output, nil
}

func shardsToAws(shards ...shard) []*kinesis.Shard {
	var awsShards []*kinesis.Shard
	for _, s := range shards {
		awsShard := &kinesis.Shard{ShardId: aws.String(s.id)}
		if s.parentOne != "" {
			awsShard.ParentShardId = aws.String(s.parentOne)
		}
		if s.parentTwo != "" {
			awsShard.AdjacentParentShardId = aws.String(s.parentTwo)
		}
		awsShards = append(awsShards, awsShard)
	}
	return awsShards
}

func makeRecords(s string) []*kinesis.Record {
	if s == "" {
		return nil
	}

	return []*kinesis.Record{&kinesis.Record{
		Data:           []byte(s),
		PartitionKey:   &defaultPartitionKey,
		SequenceNumber: &defaultSequenceNumber,
	}}
}

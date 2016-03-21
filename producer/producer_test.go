package producer

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/hashicorp/go-multierror"
)

const TestStream = "test"

func TestPutInvalidString(t *testing.T) {
	invalid := []struct {
		name     string
		input    string
		expected error
	}{
		{"empty string", "", EmptyPartitionKey},
		{"invalid unicode", string([]byte{0xc1, 0xbf}), InvalidUnicode},
	}

	producer := producerWithStubClient(123)
	for _, tc := range invalid {
		if err := producer.PutString(tc.input); !errContains(err, tc.expected) {
			t.Errorf("expected %s to error with '%s'. got '%+v'", tc.name, tc.expected, err)
		}
	}
}

func TestInvalidPut(t *testing.T) {
	invalid := []struct {
		name     string
		key      *string
		value    []byte
		expected error
	}{
		{"key too long", aws.String(randomString(257)), []byte{0x6c, 0x6f, 0x6c}, PartitionKeyTooLong},
		{"invalid unicode", aws.String(string([]byte{0xc1, 0xbf})), []byte{0x6c, 0x6f, 0x6c}, InvalidUnicode},
		{"empty value", aws.String(randomString(123)), []byte{}, EmptyValue},
	}

	producer := producerWithStubClient(123)
	for _, tc := range invalid {
		if err := producer.Put(tc.key, tc.value); !errContains(err, tc.expected) {
			t.Errorf("expected %s to error with '%s'. got '%+v'", tc.name, tc.expected, err)
		}
	}
}

func TestPutBuffers(t *testing.T) {
	testCases := [][]message{
		{{aws.String("twinkle"), []byte("twinkle")}},
		{{aws.String("hey"), []byte("there")}, {aws.String("big"), []byte("fella")}},
	}

	for _, testCase := range testCases {
		producer := producerWithStubClient(len(testCase) + 1)
		client := producer.client.(*StubClient)

		for _, input := range testCase {
			err := producer.Put(input.PartitionKey, input.Value)
			if err != nil {
				t.Fatalf("expected no Put errors. got '%s'", err)
			}
		}

		if client.puts != 0 {
			t.Errorf("expected 0 sent messages, got %d", client.puts)
		}
		if producer.current != len(testCase) {
			t.Errorf("expected %d buffered messages, found %d", len(testCase), len(producer.messages))
		}
	}
}

func TestPutSends(t *testing.T) {
	testCases := [][]message{
		{{aws.String("twinkle"), []byte("twinkle")}},
		{{aws.String("hey"), []byte("there")}, {aws.String("big"), []byte("fella")}},
	}

	for _, testCase := range testCases {
		producer := producerWithStubClient(len(testCase))
		client := producer.client.(*StubClient)

		for _, input := range testCase {
			err := producer.Put(input.PartitionKey, input.Value)
			if err != nil {
				t.Fatalf("expected no Put errors. got '%s'", err)
			}
		}

		if client.puts != 1 {
			t.Errorf("expected a single send to Kinesis")
			return
		}
		assertSentMessages(t, fmt.Sprintf("%d successful records", len(testCase)), testCase, client.sent)
	}
}

func TestPutRetriesFailedRecords(t *testing.T) {
	testCases := []struct {
		name      string
		messages  []message
		responses []clientResponse
	}{
		{
			"one message retrying once",
			[]message{{aws.String("twinkle"), []byte("twinkle")}},
			[]clientResponse{
				{outputWithErrors("ProvisionedThroughputExceededException"), nil},
			},
		},
		{
			"one message retrying twice",
			[]message{{aws.String("twinkle"), []byte("twinkle")}},
			[]clientResponse{
				{outputWithErrors("ProvisionedThroughputExceededException"), nil},
				{outputWithErrors("ProvisionedThroughputExceededException"), nil},
			},
		},
		{
			"two messages with one retrying once",
			[]message{{aws.String("hey"), []byte("there")}, {aws.String("big"), []byte("fella")}},
			[]clientResponse{
				{outputWithErrors("", "ProvisionedThroughputExceededException"), nil},
			},
		},
	}

	for _, testCase := range testCases {
		actualRetries := 0

		producer := producerRespondingWith(MaxSendSize, testCase.responses...)
		producer.Throttle = func() Throttle {
			actualRetries++
			return &noOpThrottle{}
		}

		client := producer.client.(*StubClient)

		for _, m := range testCase.messages {
			if err := producer.Put(m.PartitionKey, m.Value); err != nil {
				t.Fatalf("unexpected producer error! %s", err)
			}
		}
		producer.Flush()

		expectedRetries := len(testCase.responses)
		if actualRetries != expectedRetries {
			t.Errorf("expected %d retries, got %d", expectedRetries, actualRetries)
		}
		assertSentMessages(t, testCase.name, testCase.messages, client.sent)
	}
}

func assertSentMessages(t *testing.T, testName string, expected []message, actual []*kinesis.PutRecordsRequestEntry) {
	var sent []message
	for _, record := range actual {
		sent = append(sent, message{record.PartitionKey, record.Data})
	}

	if !reflect.DeepEqual(sent, expected) {
		t.Errorf(`%s: expected messages are not equal to sent records:

		actual:   %+v
		expected: %+v
		`, testName, sent, expected)
	}
}

// Testing helper for multierror
func errContains(e error, checkForErrors ...error) bool {
	multi, ok := e.(*multierror.Error)
	if !ok {
		return false
	}

	haveErrors := make(map[error]bool)
	for _, e := range multi.Errors {
		haveErrors[e] = true
	}

	for _, expected := range checkForErrors {
		if !haveErrors[expected] {
			return false
		}
	}
	return true
}

// The return values from kinesisClient.PutRecords
type clientResponse struct {
	output *kinesis.PutRecordsOutput
	err    error
}

// A client where Puts always succeed.
type StubClient struct {
	nextResponse int
	responses    []clientResponse

	puts int
	sent []*kinesis.PutRecordsRequestEntry
}

// Put records to the stub and save any that were put successfully for comparing
// later.
func (s *StubClient) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	s.puts++
	var response *kinesis.PutRecordsOutput
	var err error

	if s.nextResponse >= len(s.responses) {
		response, err = successfulResponse(input)
	} else {
		next := s.responses[s.nextResponse]
		response, err = next.output, next.err
		s.nextResponse++
	}

	for i, record := range input.Records {
		if response.Records[i].ErrorCode == nil {
			s.sent = append(s.sent, record)
		}
	}

	return response, err
}

// A response where every record in the given input is a success.
func successfulResponse(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	records := make([]*kinesis.PutRecordsResultEntry, len(input.Records))
	for i := range input.Records {
		records[i] = &kinesis.PutRecordsResultEntry{
			SequenceNumber: aws.String("sequence_number"),
			ShardId:        aws.String("an_shard"),
		}
	}
	output := &kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int64(0),
		Records:           records,
	}
	return output, nil
}

// Return an output response with the given error codes. Blank strings imply
// that a message was not an error.
func outputWithErrors(codes ...string) *kinesis.PutRecordsOutput {
	resultEntries := make([]*kinesis.PutRecordsResultEntry, len(codes))

	errorCount := 0
	for i, code := range codes {
		resultEntries[i] = &kinesis.PutRecordsResultEntry{
			ErrorCode: aws.String(code),
		}

		if code != "" {
			errorCount++
		}
	}

	return &kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int64(int64(errorCount)),
		Records:           resultEntries,
	}
}

// Create a producer that uses the default TestStream and a StubClient.
func producerWithStubClient(sendSize int) *Producer {
	return &Producer{
		StreamName: TestStream,
		SendSize:   sendSize,
		client:     &StubClient{},
		messages:   make([]message, sendSize),
		Throttle:   func() Throttle { return &noOpThrottle{} },
	}
}

// A producer with TestStream and a StubClient that responds with the given
// responses before always succeeding.
func producerRespondingWith(sendSize int, responses ...clientResponse) *Producer {
	return &Producer{
		StreamName: TestStream,
		SendSize:   sendSize,
		client:     &StubClient{responses: responses},
		messages:   make([]message, sendSize),
		Throttle:   func() Throttle { return &noOpThrottle{} },
	}
}

// Random strings, courtesy of StackOverflow: http://stackoverflow.com/a/31832326

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

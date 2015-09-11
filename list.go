package main

// TODO(benl): Describe the streams instead of just listing names.

import (
	"log"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

var listCommand = &Command{
	Name:  "list",
	Usage: "list",
	Short: "list Kinesis streams",
	Description: `

	List the Kinesis streams associated with your account.
	`,
	Run: runList,
}

// List the names of all of the Kinesis streams.
func runList(args []string) {
	// Ignore args

	streams, err := listStreams(kinesis.New(nil))
	if err != nil {
		log.Fatalln("error:", err)
	}

	for _, stream := range streams {
		log.Println(stream)
	}
}

func listStreams(k *kinesis.Kinesis) ([]string, error) {
	streams := make([]string, 0)
	request := &kinesis.ListStreamsInput{}
	hasMoreStreams := true

	for hasMoreStreams {
		resp, err := k.ListStreams(request)
		if err != nil {
			return nil, err
		}

		for _, streamName := range resp.StreamNames {
			streams = append(streams, *streamName)
		}

		if !*resp.HasMoreStreams {
			break
		}
		request.ExclusiveStartStreamName = resp.StreamNames[len(resp.StreamNames)-1]
	}

	return streams, nil
}

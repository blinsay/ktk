package main

import (
	"fmt"
	"os"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

func ListStreams(k *kinesis.Kinesis) []string {
	streams := make([]string, 0)
	request := &kinesis.ListStreamsInput{Limit: aws.Long(1)}
	hasMoreStreams := true

	for hasMoreStreams {
		resp, err := k.ListStreams(request)
		if awsError := aws.Error(err); awsError != nil {
			fmt.Println("error:", awsError.Code, awsError.Message)
			os.Exit(1)
		} else if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}

		for _, streamName := range resp.StreamNames {
			streams = append(streams, *streamName)
		}

		if !*resp.HasMoreStreams {
			break
		}
		request.ExclusiveStartStreamName = resp.StreamNames[len(resp.StreamNames)-1]
	}

	return streams
}

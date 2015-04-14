package main

import (
	"fmt"
	"log"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/aws/awsutil"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

var tailCommand = &Command{
	Name:  "tail",
	Usage: "tail stream-name",
	Short: "print data from the given stream",
	Description: `
	Tail the given Kinesis stream and print data to stdout. Functions like a
	tail -f for Kinesis.
	`,
	Run: doTail,
}

func doTail(args []string) {
	if len(args) < 1 {
		log.Fatalln("ktk tail: no stream name given")
	}

	stream := args[0]
	client := kinesis.New(nil)

	resp, err := client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		log.Fatalf("error: %#v\n", err)
	}

	fmt.Println(awsutil.StringValue(resp))
}

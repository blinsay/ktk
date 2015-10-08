package main

import (
	"fmt"
	"log"

	"github.com/blinsay/ktk/Godeps/_workspace/src/github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/blinsay/ktk/consumer"
)

var tailCommand = &Command{
	Name:  "tail",
	Usage: "tail stream-name",
	Short: "Print data from the given stream",
	Description: `
	Tail the given Kinesis stream and print data to stdout. Functions like a
	tail -f for Kinesis. Only the data from each Kinesis Record is printed.

	Tail follows a stream from the LATEST record. It handles reading through a
	stream split or merge.
	`,
	Run: doTail,
}

func doTail(args []string) {
	if len(args) < 1 {
		log.Fatalln("ktk tail: no stream name given")
	}

	stream := args[0]
	lines := make(chan string)

	err := consumer.Tail(stream, envBool(VERBOSE), func(records []*kinesis.Record) {
		for _, record := range records {
			lines <- string(record.Data)
		}
	})
	fatalOnErr(err)

	for {
		fmt.Println(<-lines)
	}
}

package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/blinsay/ktk/consumer"
)

var tailCommand = &Command{
	Name:  "tail",
	Usage: "tail stream-name",
	Short: "print data from the given stream",
	Description: `
	Tail the given Kinesis stream and print data to stdout. Functions like a
	tail -f for Kinesis. Each message is printed on a new line.
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

package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/service/kinesis"
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
	consumer := NewConsumer(kinesis.New(nil))

	lines := make(chan string)
	consumer.TailFunc(stream, func(records []*kinesis.Record) {
		for _, r := range records {
			lines <- string(r.Data)
		}
	})

	for {
		fmt.Println(<-lines)
	}
}

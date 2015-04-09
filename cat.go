package main

import (
	"fmt"
	"log"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

var catCommand = &Command{
	Name:  "cat",
	Usage: "cat stream [file...]",
	Short: "send data to a Kinesis stream",
	Description: `
	Sends data to the specified Kinesis stream.
	`,
	Run: runCat,
}

// Handle any errors from producer.Put and maybe log them and exit. Is a no-op
// if there were no failures.
func handleErrs(failures []FailedPut, err error) {
	if awserr := aws.Error(err); awserr != nil {
		if len(awserr.Message) > 0 {
			log.Fatalln("aws error:", awserr.Message)
		}
		log.Fatalf("aws error:", awserr.Code)
	}
	if err != nil {
		log.Fatalln("error:", err)
	}
	if failures != nil {
		errCounts := make(map[string]int)
		for _, e := range failures {
			errCounts[*e.ErrorCode]++
		}
		fmt.Println("error map: ", errCounts)
		log.Fatalf("%d puts failed: %+v\n", len(failures), errCounts)
	}

	// Do nothing
}

// Run the cat command with the given arguments.
//
// The name of the stream to send data to is required. Any other arguments are
// filenames that should be sent line-by-line into Kinesis. If no files are
// passed, data is sent from Stdin.
func runCat(args []string) {
	if len(args) < 1 {
		log.Fatalln("error: no stream name given")
	}

	stream := args[0]
	inputFiles := args[1:]

	var supplier ReaderSupplier
	if len(inputFiles) == 0 {
		supplier = &StdinSupplier{}
	} else {
		supplier = &FileSupplier{filenames: inputFiles}
	}

	producer, err := NewBufferedProducer(stream, *kinesis.New(nil), MaxSendSize)
	if err != nil {
		log.Fatalln("error:", err)
	}

	scanner := NewMultiScanner(supplier)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			handleErrs(producer.Put(StringMessage(line)))
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalln("error:", err)
	}

	handleErrs(producer.Flush())
}

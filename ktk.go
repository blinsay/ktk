package main

import (
	"fmt"
	"os"

	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

const usage = "usage: ktk [list|cat]"

func list() error {
	for _, stream := range ListStreams(kinesis.New(nil)) {
		fmt.Println(stream)
	}

	return nil
}

func handleCatFailures(failures []FailedPut, err error) error {
	if err != nil {
		return err
	}
	if failures != nil {
		errCounts := make(map[string]int)
		for _, e := range failures {
			errCounts[*e.ErrorCode]++
		}
		return fmt.Errorf("error: %d puts failed: %x", len(failures), errCounts)
	}
	return nil
}

func cat(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("cat requires a stream name")
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
		return err
	}

	scanner := NewMultiScanner(supplier)
	for scanner.Scan() {
		err := handleCatFailures(producer.Put(StringMessage(scanner.Text())))
		if err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if err := handleCatFailures(producer.Flush()); err != nil {
		return err
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(-1)
	}

	var err error
	cmd := os.Args[1]
	switch cmd {
	case "cat":
		err = cat(os.Args[2:])
	case "list":
		err = list()
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(-1)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

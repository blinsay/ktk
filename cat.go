package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

var catCommand = &Command{
	Name:  "cat",
	Usage: "cat stream [file...]",
	Short: "send data to a Kinesis stream",
	Description: `
	Sends data to the specified Kinesis stream one line at a time. If the names of
	files are given as arguments, they're opened and read in order.
	`,
	Run: runCat,
}

// Handle any errors from producer.Put and maybe log them and exit. Is a no-op
// if there were no failures.
func handleErrs(failures []FailedPut, err error) {
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
		log.Fatalln("ktk cat: no stream name given")
	}

	stream := args[0]
	inputFiles := args[1:]

	reader := io.Reader(os.Stdin)
	if len(inputFiles) > 0 {
		reader = openFiles(inputFiles)
	}
	scanner := bufio.NewScanner(reader)

	producer, err := NewBufferedProducer(stream, *kinesis.New(nil), MaxSendSize)
	if err != nil {
		log.Fatalln("error:", err)
	}

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

// NOTE: If this returns err the files aren't closed. That's kewl, the program
// is about to exit anyway.
func openFiles(filenames []string) io.Reader {
	files := make([]io.Reader, len(filenames))
	for i, name := range filenames {
		f, err := os.Open(name)
		if err != nil {
			log.Fatalln("ktk cat: error:", err)
			return nil // unreachable
		}
		files[i] = f
	}
	return io.MultiReader(files...)
}

package main

import (
	"bufio"
	"io"
	"log"
	"os"

	"github.com/blinsay/ktk/producer"
)

var catCommand = &Command{
	Name:  "cat",
	Usage: "cat stream [file...]",
	Short: "send data to a Kinesis stream",
	Description: `
	Sends data to the specified Kinesis stream one line at a time. If the names of
	files are given as arguments, they're opened and read in order.

	Cat sends data as fast as possible, using the first 256 characters of the
	string as the partition key. Any throughput errors are automatically retried
	until data is sent successfully.
	`,
	Run: runCat,
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

	reader := io.Reader(os.Stdin)
	if len(inputFiles) > 0 {
		reader = openFiles(inputFiles)
	}
	scanner := bufio.NewScanner(reader)

	p := producer.New(stream)
	p.Debug = envBool(VERBOSE)

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			fatalOnErr(p.PutString(line))
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalln("error:", err)
	}

	fatalOnErr(p.Flush())
}

// NOTE: If this returns err the files aren't closed. That's kewl, the program
// is about to exit anyway.
func openFiles(filenames []string) io.Reader {
	files := make([]io.Reader, len(filenames))
	for i, name := range filenames {
		f, err := os.Open(name)
		if err != nil {
			log.Fatalln("error:", err)
			return nil // unreachable
		}
		files[i] = f
	}
	return io.MultiReader(files...)
}

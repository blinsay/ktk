package main

import (
	"flag"
	"log"

	"github.com/blinsay/ktk/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/awserr"
)

func logFatalAwsErr(err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		log.Fatalf("aws error: %s", awsErr.Message())
	}
	log.Fatalln("error:", err)
}

// A ktk sub-command to run. (e.g. cat)
type Command struct {
	// The name of the command.
	Name string
	// The command's usage string.
	Usage string
	// A short description of the command.
	Short string
	// A long help-style description of the command.
	Description string
	// The function that should actually run when the command is called. Called
	// with every command-line arg after the name of the command.
	Run func([]string)
}

// Available commands
var commands = []*Command{
	catCommand,
	listCommand,
	tailCommand,
}

func usage() {
	log.Println("usage: ktk command [arguments...]")
	log.Println()
	log.Println("commands:")

	for _, cmd := range commands {
		log.Printf("\t%s\t%s\n", cmd.Name, cmd.Short)
	}
}

func init() {
	log.SetFlags(0)
}

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		usage()
		return
	}

	if args[0] == "help" {
		if len(args) > 1 {
			for _, cmd := range commands {
				if cmd.Name == args[1] {
					log.Printf("ktk %s\n%s", cmd.Usage, cmd.Description)
					return
				}
			}
		}

		usage()
		return
	}

	for _, cmd := range commands {
		if cmd.Name == args[0] {
			cmd.Run(args[1:])
			return
		}
	}

	usage()
}

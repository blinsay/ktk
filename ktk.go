package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	"github.com/blinsay/ktk/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/awserr"
)

const VERBOSE = "KTK_VERBOSE"

// Return true if the given env variable is set to a truthy value. See
// strconv.ParseBool for truthy values.
func envBool(name string) bool {
	b, e := strconv.ParseBool(os.Getenv(name))
	if e != nil {
		return false
	}
	return b
}

// log.Fatalln on any non-nil error. Pretty print any AWS errors.
func fatalOnErr(err error) {
	if err == nil {
		return
	}

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

const usageHeader = `usage: ktk command [arguments...]

	help	Show help for an individual command`

func usage() {
	log.Println(usageHeader)

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

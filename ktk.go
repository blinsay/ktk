package main

import (
	"flag"
	"log"
)

type Command struct {
	Name        string
	Usage       string
	Short       string
	Description string
	Run         func([]string)
}

var commands = []*Command{
	catCommand,
	listCommand,
}

func usage() {
	log.Println("usage: ktk command [arguments...]")
	log.Println()
	log.Println("Valid commands:")

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

	for _, cmd := range commands {
		if cmd.Name == args[0] {
			cmd.Run(args[1:])
			return
		}
	}

	usage()
}

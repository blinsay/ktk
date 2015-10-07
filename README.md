`ktk` is a command line tool for interacting with Kinesis like it's a file. See
`ktk help` for a list of individual commands, and `ktk help command` for
information on a particular command.

```
$ ktk help
usage: ktk command [arguments...]

	help	show help on an individual command
	cat	send data to a Kinesis stream
	list	list Kinesis streams
	tail	print data from the given stream
```

#### AWS Credentials

`ktk` gets credentials from the standard AWS environment variables or
credentials file. If you've already configured the `aws` command line tools,
you're good to go.

#### Install

Download a binary from the Release tab on Github!

#### Install from Source

Install `ktk` from source with the Go toolchain:

`go install github.com/blinsay/ktk`


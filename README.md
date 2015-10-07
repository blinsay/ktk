# ktk - An AWS Kinesis stream interface

`ktk` is a command line tool for interacting with Kinesis like it is a file. See
`ktk help` for a list of individual subcommands, and `ktk help command` for
information on a particular subcommand.

```
$ ktk help
usage: ktk command [arguments...]

	help	Show help on an individual command
	cat		Send data to a Kinesis stream
	list	List Kinesis streams
	tail	Print data from the given stream
```

## AWS Credentials

The `ktk` command gets credentials from the standard AWS environment variables or
credentials file. If you've already configured the `aws` command line tools then you're good to go.

## Install

Download a binary from the [`Release`](https://github.com/blinsay/ktk/releases) tab on Github!

## Install from Source

Install `ktk` from source with the Golang toolchain:

`go install github.com/blinsay/ktk`

## License

&copy; Copyright Ben Linsay 2015.

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.00)

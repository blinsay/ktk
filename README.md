# ktk - Treat a Kinesis stream like a file

*NOTE: macOS Sierra users should upgrade to v1.0.1*

`ktk` is a command line tool for interacting with Kinesis like it's a file. See
`ktk help` for a list of subcommands, and `ktk help command` for information on
a particular subcommand.

```
$ ktk help
usage: ktk command [arguments...]

	help    Show help for an individual command
	cat     Send data to a Kinesis stream
	list    List Kinesis streams
	tail    Print data from the given stream
```

#### AWS Credentials

`ktk` gets credentials from the standard AWS environment variables or
credentials file. If you've already configured the `aws` command line tools,
you're good to go.

#### Install

Download a binary from the [`Release`](https://github.com/blinsay/ktk/releases)
tab on Github!

#### Install from Source

Install `ktk` from source with the Go toolchain:

`go install github.com/blinsay/ktk`

## License

&copy; Copyright Ben Linsay 2015.

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.00)

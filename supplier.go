package main

import (
	"io"
	"os"
)

// Returns a new io.Reader on every call to Reader().
//
// If an error occurs the supplier may return a nil Reader and a non-nil error.
// Once the reader has been exhausted, both the Reader and the error will always
// be nil.
type ReaderSupplier interface {
	Reader() (io.Reader, error)
}

// Return a ReaderSupplier of all of the files listed in os.Args[1:]. If there
// are no files passsed on the command line, returns a one-shot supplier that
// returns os.Stdin.
//
// Mimics Python's fileinput.input() or Ruby's ARGF.
func GetInput() ReaderSupplier {
	if len(os.Args) > 1 {
		return &FileSupplier{filenames: os.Args[1:]}
	}

	return &StdinSupplier{}
}

// A ReaderSupplier that returns os.Stdin exactly once.
type StdinSupplier struct {
	returned bool
}

func (ss *StdinSupplier) Reader() (io.Reader, error) {
	if ss.returned {
		return nil, nil
	}

	ss.returned = true
	return os.Stdin, nil
}

// A ReaderSupplier that returns a sequence of files.
type FileSupplier struct {
	filenames []string
	current   int
	err       error
}

func (fs *FileSupplier) Reader() (io.Reader, error) {
	if fs.current < 0 {
		return nil, nil
	}

	f, err := os.Open(fs.filenames[fs.current])
	if fs.current < len(fs.filenames)-1 {
		fs.current++
	} else {
		fs.current = -1
	}
	return f, err
}

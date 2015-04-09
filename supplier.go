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

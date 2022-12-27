//go:build !windows && !plan9
// +build !windows,!plan9

// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package badger creates and manipulates files whose contents should only
// change atomically.
package badger

import (
	"fmt"
	"io/fs"
	"os"
	"runtime"
)

// A File is a locked *os.File.
//
// Closing the file releases the lock.
//
// If the program exits while a file is locked, the operating system releases
// the lock but may not do so promptly: callers must ensure that all locked
// files are closed before exiting.
type File struct {
	osFile
	closed bool
}

// osFile embeds a *os.File while keeping the pointer itself unexported.
// (When we close a File, it must be the same file descriptor that we opened!)
type osFile struct {
	*os.File
}

// OpenFile is like os.OpenFile, but returns a locked file.
// If flag includes os.O_WRONLY or os.O_RDWR, the file is write-locked;
// otherwise, it is read-locked.
func OpenFile(name string, flag int, perm fs.FileMode) (*File, error) {
	var (
		f   = new(File)
		err error
	)
	f.osFile.File, err = openFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	// Although the operating system will drop locks for open files when the go
	// command exits, we want to hold locks for as little time as possible, and we
	// especially don't want to leave a file locked after we're done with it. Our
	// Close method is what releases the locks, so use a finalizer to report
	// missing Close calls on a best-effort basis.
	runtime.SetFinalizer(f, func(f *File) {
		panic(fmt.Sprintf("lockedfile.File %s became unreachable without a call to Close", f.Name()))
	})

	return f, nil
}

// Close unlocks and closes the underlying file.
//
// Close may be called multiple times; all calls after the first will return a
// non-nil error.
func (f *File) Close() error {
	if f.closed {
		return &fs.PathError{
			Op:   "close",
			Path: f.Name(),
			Err:  fs.ErrClosed,
		}
	}
	f.closed = true

	err := closeFile(f.osFile.File)
	runtime.SetFinalizer(f, nil)
	return err
}

// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build linux freebsd netbsd openbsd darwin dragonfly solaris

// Common parts for all implementations (epoll(7), select(2), etc.)

package poller

import (
	"syscall"
	"time"
)

// Flags to Open
const (
	O_RW = (syscall.O_NOCTTY |
		syscall.O_CLOEXEC |
		syscall.O_RDWR |
		syscall.O_NONBLOCK) // Open file for read and write
	O_RO = (syscall.O_NOCTTY |
		syscall.O_CLOEXEC |
		syscall.O_RDONLY |
		syscall.O_NONBLOCK) // Open file for read
	O_WO = (syscall.O_NOCTTY |
		syscall.O_CLOEXEC |
		syscall.O_WRONLY |
		syscall.O_NONBLOCK) // Open file for write
	o_MODE = 0666
)

// Open the named path for reading, writing or both, depnding on the
// flags argument.
func Open(name string, flags int) (*FD, error) {
	sysfd, err := syscall.Open(name, flags, o_MODE)
	if err != nil {
		return nil, err
	}
	return NewFD(sysfd)
}

// NewFD initializes and returns a new poller file-descriptor from the
// given system (unix) file-descriptor. After calling NewFD the system
// file-descriptor must be used only through the poller.FD methods,
// not directly. NewFD sets the system file-descriptor to non-blocking
// mode.
func NewFD(sysfd int) (*FD, error) {
	return newFD(sysfd)
}

// TODO(npat): Add FromFile function?

// Read up to len(p) bytes into p. Returns the number of bytes read (0
// <= n <= len(p)) and any error encountered. If some data is
// available but not len(p) bytes, Read returns what is available
// instead of waiting for more. Read is compatible with the Read
// method of the io.Reader interface. In addition Read honors the
// timeout set by (*FD).SetDeadline and (*FD).SetReadDeadline. If no
// data are read before the timeout expires Read returns with err ==
// ErrTimeout (and n == 0). If the read(2) system-call returns 0, Read
// returns with err = io.EOF (and n == 0).
func (fd *FD) Read(p []byte) (n int, err error) {
	return fdIO(fd, false, p)
}

// Writes len(p) bytes from p to the file-descriptor.  Returns the
// number of bytes written (0 <= n <= len(p)) and any error
// encountered that caused the write to stop early.  Write returns a
// non-nil error if it returns n < len(p). Write is compatible with
// the Write method of the io.Writer interface. In addition Write
// honors the timeout set by (*FD).SetDeadline and
// (*FD).SetWriteDeadline. If less than len(p) data are writen before
// the timeout expires Write returns with err == ErrTimeout (and n <
// len(p)). If the write(2) system-call returns 0, Write returns with
// err == io.ErrUnexpectedEOF.
//
// Write attempts to write the full amount requested by issuing
// multiple system calls if required. This sequence of system calls is
// NOT atomic. If multiple goroutines write to the same FD at the same
// time, their Writes may interleave. If you wish to protect against
// this, you can embed FD and use a mutex to guard Write method calls:
//
//     struct myFD {
//         *poller.FD
//         mu sync.Mutex
//     }
//
//     func (fd *myFD) Write(p []byte) (n int, err error) {
//         fd.mu.Lock()
//         n, err := fd.FD.Write(p)
//         fd.mu.Unlock()
//         return n, err
//     }
//
func (fd *FD) Write(p []byte) (n int, err error) {
	for n != len(p) {
		var nn int
		nn, err = fdIO(fd, true, p[n:])
		if err != nil {
			break
		}
		n += nn
	}
	return n, err
}

// Close the file descriptor. It is ok to call Close concurently with
// other operations (Read, Write, SetDeadline, etc) on the same
// file-descriptor. In this case ongoing operations will return with
// ErrClosed. Any subsequent operations (after Close) on the
// file-descriptor will also fail with ErrClosed.
func (fd *FD) Close() error {
	// Take the C lock, to exclude misc operations from accessing
	// a closed sysfd.
	if err := fd.Lock(); err != nil {
		debugf("FD %03d: CL: Closed", fd.id)
		return err
	}
	defer fd.Unlock()
	return fd.CloseUnlocked()
}

// CloseUnlocked is equivalent to (*FD).Close, with the difference
// that with CloseUnlocked the caller is responsible for locking the
// FD before calling it, and unlocking it after. It can be useful for
// performing clean-up operations (e.g. reset tty settings) atomically
// with Close.
func (fd *FD) CloseUnlocked() error {
	return fd.closeUnlocked()
}

// SetDeadline sets the deadline for both Read and Write operations on
// the file-descriptor. Deadlines are expressed as ABSOLUTE instances
// in time. For example, to set a deadline 5 seconds to the future do:
//
//   fd.SetDeadline(time.Now().Add(5 * time.Second))
//
// This is equivalent to:
//
//   fd.SetReadDeadline(time.Now().Add(5 * time.Second))
//   fd.SetWriteDeadline(time.Now().Add(5 * time.Second))
//
// A zero value for t, cancels (removes) the existing deadline.
//
func (fd *FD) SetDeadline(t time.Time) error {
	if err := fd.SetReadDeadline(t); err != nil {
		return err
	}
	return fd.SetWriteDeadline(t)
}

// SetReadDeadline sets the deadline for Read operations on the
// file-descriptor.
func (fd *FD) SetReadDeadline(t time.Time) error {
	return setDeadline(fd, false, t)
}

// SetWriteDeadline sets the deadline for Write operations on the
// file-descriptor.
func (fd *FD) SetWriteDeadline(t time.Time) error {
	return setDeadline(fd, true, t)
}

// Lock the file-descriptor. It must be called before perfoming
// miscellaneous operations (e.g. ioctls) on the underlying system
// file descriptor. It protects the operations against concurent
// Close's. Typical usage is:
//
//     if err := fd.Lock(); err != nil {
//         return err
//     }
//     deffer fd.Unlock()
//     ... do ioctl on fd.Sysfd() ...
//
// Notice that Read's and Write's *can* happen concurrently with misc
// operations on a locked FD---as this is not, necessarily, an
// error. Lock protects *only* against concurent Close's.
//
func (fd *FD) Lock() error {
	fd.c.Lock()
	if fd.closed {
		fd.c.Unlock()
		return ErrClosed
	}
	return nil
}

// Unlock unlocks the file-descriptor.
func (fd *FD) Unlock() {
	fd.c.Unlock()
}

// Sysfd returns the system file-descriptor associated with the given
// poller file-descriptor. See also (*FD).Lock.
func (fd *FD) Sysfd() int {
	return fd.sysfd
}

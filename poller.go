// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build linux freebsd netbsd openbsd darwin dragonfly solaris

// Common parts for all implementations (epoll(7), select(2), etc.)

package poller

import (
	"fmt"
	"log"
	"sync"
	"syscall"
	"time"
)

// TODO(npat): Perhaps lock fdMap with a RWMutex??

//  fdMap maps IDs (arbitrary integers) to FD structs. IDs are *not*
//  reused, so if we get stale file-descriptor events (which are keyed
//  by ID), no harm is done.
//
//  NOTE: Some implementations (e.g. the select(2)-based one) use
//  sysfds (Unix file descriptors) instead of arbitrary IDs to map FD
//  structs. In this case, since sysfds *are* reused, the implications
//  of the reuse must be taken into considereation by the
//  implementation.
type fdMap struct {
	mu  sync.Mutex
	seq int
	fd  map[int]*FD
}

func (fdm *fdMap) Init(n int) {
	// For easier debugging, start with a large ID so that we can
	// easily tell apart sysfd's from IDs
	fdm.seq = 100
	fdm.fd = make(map[int]*FD, n)
}

// GetFD returns the FD with key id. Returns nil if no such FD is in
// the map.
func (fdm *fdMap) GetFD(id int) *FD {
	fdm.mu.Lock()
	fd := fdm.fd[id]
	fdm.mu.Unlock()
	return fd
}

// GetID reserves and returns the next available ID
func (fdm *fdMap) GetID() int {
	fdm.mu.Lock()
	id := fdm.seq
	fdm.seq++
	fdm.mu.Unlock()
	return id
}

// AddFD creates an entry for the FD with key "fd.id".
func (fdm *fdMap) AddFD(fd *FD) {
	fdm.mu.Lock()
	if _, ok := fdm.fd[fd.id]; ok {
		fdm.mu.Unlock()
		log.Panicf("poller: Add existing fd: %d", fd.id)
	}
	fdm.fd[fd.id] = fd
	fdm.mu.Unlock()
}

func (fdm *fdMap) DelFD(id int) {
	fdm.mu.Lock()
	if _, ok := fdm.fd[id]; !ok {
		fdm.mu.Unlock()
		log.Panicf("poller: Del non-existing fd: %d", id)
	}
	delete(fdm.fd, id)
	fdm.mu.Unlock()
}

// fdCtl keeps control fields (locks, timers, etc) for a single
// direction. For every FD there is one fdCtl for Read operations and
// another for Write operations.
type fdCtl struct {
	mu       sync.Mutex
	cond     *sync.Cond
	deadline time.Time
	timer    *time.Timer
	timeout  bool
}

// FD is a poller file-descriptor. Typically a file-descriptor
// connected to a terminal, a pseudo terminal, a character device, a
// FIFO (named pipe), or any unix stream that can be used by the
// system's file-descriptor multiplexing mechanism (epoll(7),
// select(2), etc).
type FD struct {
	id int // Key in fdMap[] (immutable)

	// Lock C protects misc operations against Close
	c sync.Mutex
	// Must hold all C, R and W locks and to set. Must hold one
	// (any) of these locks to read
	sysfd  int  // Unix file descriptor
	closed bool // Set by Close(), never cleared

	// Must hold respective lock to access
	r fdCtl // Control fields for Read operations
	w fdCtl // Control fields for Write operations
}

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

// TODO(npat): If deadline is not After(time.Now()) wake-up goroutines
// imediatelly; don't go through the timer callback

func setDeadline(fd *FD, write bool, t time.Time) error {
	var fdc *fdCtl
	var dpre string

	if !write {
		// Setting read deadline
		fdc = &fd.r
		if debug_enable {
			dpre = fmt.Sprintf("FD %03d: DR:", fd.id)
		}
	} else {
		// Setting write deadline
		fdc = &fd.w
		if debug_enable {
			dpre = fmt.Sprintf("FD %03d: DW:", fd.id)
		}
	}
	// R & W deadlines are handled identically
	fdc.cond.L.Lock()
	if fd.closed {
		fdc.cond.L.Unlock()
		return ErrClosed
	}
	fdc.deadline = t
	fdc.timeout = false
	if t.IsZero() {
		if fdc.timer != nil {
			fdc.timer.Stop()
		}
		debugf("%s Removed dl", dpre)
	} else {
		d := t.Sub(time.Now())
		if fdc.timer == nil {
			id := fd.id
			fdc.timer = time.AfterFunc(d,
				func() { timerEvent(id, write) })
		} else {
			fdc.timer.Stop()
			fdc.timer.Reset(d)
		}
		debugf("%s Set dl: %v", dpre, d)
	}
	fdc.cond.L.Unlock()
	return nil
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

func debugf(format string, v ...interface{}) {
	if debug_enable {
		log.Printf("poller: "+format, v...)
	}
}

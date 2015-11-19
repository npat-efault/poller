// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build linux freebsd netbsd openbsd darwin dragonfly solaris

package poller

type errFlags int

const (
	efClosed errFlags = 1 << iota
	efTimeout
	efTemporary
)

type errT struct {
	flags errFlags
	msg   string
}

func (e *errT) Error() string {
	return e.msg
}

func (e *errT) Timeout() bool {
	return e.flags&efTimeout != 0
}

func (e *errT) Temporary() bool {
	return e.flags&(efTimeout|efTemporary) != 0
}

func (e *errT) Closed() bool {
	return e.flags&efClosed != 0
}

func mkErr(flags errFlags, msg string) error {
	return &errT{flags: flags, msg: msg}
}

func newErr(msg string) error {
	return &errT{msg: msg}
}

// Errors returned by poller functions and methods. In addition to
// these, poller functions and methods may return the errors reported
// by the underlying system calls (open(2), read(2), write(2), etc.),
// as well as io.EOF and io.ErrUnexpectedEOF.
var (
	// Use of closed poller file-descriptor. ErrClosed has
	// Closed() == true.
	ErrClosed error = mkErr(efClosed, "use of closed descriptor")
	// Operation timed-out. ErrTimeout has Timeout() == true and
	// Temporary() == true.
	ErrTimeout error = mkErr(efTimeout, "timeout/deadline expired")
)

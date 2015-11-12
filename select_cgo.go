// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build linux,noepoll freebsd netbsd openbsd darwin dragonfly solaris

package poller

/*
#include <sys/select.h>

void fdclr(int fd, fd_set *set) {
	FD_CLR(fd, set);
}

int fdisset(int fd, fd_set *set) {
	return FD_ISSET(fd, set);
}

void fdset(int fd, fd_set *set) {
	FD_SET(fd, set);
}

void fdzero(fd_set *set) {
	FD_ZERO(set);
}
*/
import "C"
import (
	"syscall"
	"unsafe"
)

type fdSet syscall.FdSet

func (fs *fdSet) Clr(fds ...int) {
	for _, fd := range fds {
		C.fdclr(C.int(fd), (*C.fd_set)(unsafe.Pointer(fs)))
	}
}

func (fs *fdSet) Set(fds ...int) {
	for _, fd := range fds {
		C.fdset(C.int(fd), (*C.fd_set)(unsafe.Pointer(fs)))
	}
}

func (fs *fdSet) Zero() {
	C.fdzero((*C.fd_set)(unsafe.Pointer(fs)))
}

func (fs *fdSet) IsSet(fd int) {
	return C.fdisset(C.int(fd), (*C.fd_set)(unsafe.Pointer(fs))) != 0
}

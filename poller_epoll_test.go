// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build linux,!noepoll

package poller

import (
	"syscall"
	"testing"
)

func TestNewFDFail(t *testing.T) {
	sysfd, err := syscall.Open("/dev/null", O_RW, 0666)
	if err != nil {
		t.Fatal("Open /dev/null:", err)
	}
	fd, err := NewFD(sysfd)
	if err != syscall.EPERM {
		t.Fatal("NewFD:", err)
	}
	if fd != nil {
		t.Fatal("fd != nil!")
	}
	err = syscall.Close(sysfd)
	if err != nil {
		t.Fatal("Close /dev/null:", err)
	}
}

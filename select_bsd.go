// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build freebsd netbsd openbsd darwin dragonfly solaris

package poller

import "syscall"

func uxSelect(nfd int, r, w, e *fdSet, tv *syscall.Timeval) (n int, err error) {
	ne := syscall.Select(nfd,
		(*syscall.FdSet)(r),
		(*syscall.FdSet)(w),
		(*syscall.FdSet)(e),
		tv)

	var n int
	if ne != nil {
		n = int(ne.(syscall.Errno))
	}
	if n >= 0 {
		return n, nil
	}
	return 0, ne
}

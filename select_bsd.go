// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build freebsd netbsd openbsd darwin dragonfly solaris

package poller

import "syscall"

func uxSelect(nfd int, r, w, e *fdSet, tv *syscall.Timeval) (n int, err error) {
	// The Go syscall.Select for the BSD unixes is buggy. It
	// returns only the error and not the number of active file
	// descriptors. To cope with this, we return "nfd" as the
	// number of active file-descriptors. This can cause
	// significant performance degradation but there's nothing
	// else we can do.
	err = syscall.Select(nfd,
		(*syscall.FdSet)(r),
		(*syscall.FdSet)(w),
		(*syscall.FdSet)(e),
		tv)
	if err != nil {
		return 0, err
	}
	return nfd, nil
}

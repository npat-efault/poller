// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

/*

Package poller is an epoll(7)-based file-descriptor multiplexer. It
allows concurent Read and Write operations from and to multiple
file-descriptors without allocating one OS thread for every blocked
operation. It behaves similarly to Go's netpoller (which multiplexes
network connections) without requiring special support from the Go
runtime. It can be used with tty devices, character devices, pipes,
FIFOs, and any Unix file-descriptor that is epoll(7)-able. In addition
it allows the user to set timeouts (deadlines) for read and write
operations.

Typical usage:

    fd, err: = poller.Open("/dev/ttyXX", poller.O_RW)
    if err != nil {
        log.Fatal("Failed to open device:", err)
    }

    ...

    err = fd.SetReadDeadline(time.Now().Add(5 * time.Second))
    if err != nil {
        log.Fatal("Failed to set R deadline:", err)
    }

    ...

    b := make([]byte, 10)
    n, err := fd.Read(b)
    if err != nil {
        log.Fatal("Write failed:", err)
    }

    ...

    n, err = fd.Write([]byte("Test"))
    if err != nil {
        log.Fatal("Write failed:", err)
    }


All operations on poller FDs are thread-safe; you can use the same FD
from multiple go-routines. It is, for example, safe to close a file
descriptor blocked on a Read or Write call from another go-routine.

*/
package poller

#poller [![GoDoc](https://godoc.org/github.com/npat-efault/poller?status.png)](https://godoc.org/github.com/npat-efault/poller)
Package poller is a file-descriptor multiplexer.

Download:
```shell
go get github.com/npat-efault/poller
```

* * *

Package poller is a file-descriptor multiplexer. It allows concurent
Read and Write operations from and to multiple file-descriptors
without allocating one OS thread for every blocked operation. It
operates similarly to Go's netpoller (which multiplexes network
connections) without requiring special support from the Go runtime. It
can be used with tty devices, character devices, pipes, FIFOs, and any
file-descriptor that is poll-able (can be used with select(2),
epoll(7), etc.) In addition, package poller allows the user to set
timeouts (deadlines) for read and write operations, and also allows
for safe cancelation of blocked read and write operations; a Close
from another go-routine safely cancels ongoing (blocked) read and
write operations.

Typical usage

```
fd, err: = poller.Open("/dev/ttyXX", poller.O_RW)
if err != nil {
    log.Fatal("Failed to open device:", err)
}

...

err = fd.SetReadDeadline(time.Now().Add(5 * time.Second))
if err != nil {
    log.Fatal("Failed to set R deadline:", err)
}

b := make([]byte, 10)
n, err := fd.Read(b)
if err != nil {
    log.Fatal("Read failed:", err)
}

...

n, err = fd.Write([]byte("Test"))
if err != nil {
    log.Fatal("Write failed:", err)
}
```

All operations on poller FDs are thread-safe; you can use the same FD
from multiple go-routines. It is, for example, safe to close a file
descriptor blocked on a Read or Write call from another go-routine.

##Supported systems
Linux systems are supported using an implementations based on
epoll(7).

For other POSIX systems (that is, most Unix-like systems), a more
portable fallback implementation based on select(2) is provided. It
has the same semantics as the Linux epoll(7)-based implementation, but
is expected to be of lower performance. The select(2)-based
implementation uses CGo for some ancillary select-related operations.

Ideally, system-specific io-multiplexing or async-io facilities
(e.g. kqueue(2), or /dev/poll(7)) should be used to provide higher
performance implementations for other systems. Patches for this will
be greatly appreciated.

If you wish, you can build package poller on Linux to use the
select(2)-based implementation instead of the epoll(7) one. To do this
define the build-tag "noepoll". Normally, there is no reason to do
this.


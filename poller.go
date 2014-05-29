// Package poller is an epoll(7)-based file-descriptor multiplexer. It
// allows concurent Read and Write operations from and to multiple
// file-descriptors without allocating one OS thread for every blocked
// operation. It behaves similarly to Go's netpoller (which
// multiplexes network connections) without requiring special support
// from the Go runtime. It can be used with tty devices, character
// devices, pipes, FIFOs, and any Unix file-descriptor that is
// poll(7)-able. In addition it allows the user to set timeouts
// (deadlines) to read and write operations.
//
package poller

import (
	"io"
	"log"
	"sync"
	"syscall"
	"time"
)

//  fdMap maps IDs (arbitrary integers) to FD structs. IDs are *not*
//  reused, so if we get stale events from EpollWait (which are keyed
//  by ID), no harm is done.
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
	fdm.fd[fd.id] = fd
	fdm.mu.Unlock()
}

func (fdm *fdMap) DelFD(id int) {
	fdm.mu.Lock()
	delete(fdm.fd, id)
	fdm.mu.Unlock()
}

type FD struct {
	id    int // Key in fdMap[]
	sysfd int // Unix file descriptor

	closed bool       // Set by Close(), never cleared
	rmu    sync.Mutex // Mutex and condition for Read ops
	rco    *sync.Cond
	rtm    *time.Timer // Read timer
	rdl    time.Time   // Read deadline
	rto    bool        // Read timeout occured
	wmu    sync.Mutex  // Mutex and condition for Write ops
	wco    *sync.Cond
	wtm    *time.Timer // Write timer
	wdl    time.Time   // Write deadline
	wto    bool        // Write timeout occured
}

// TODO(npat): Add finalizer

func NewFD(sysfd int) (*FD, error) {
	// Set sysfd to non-blocking mode
	err := syscall.SetNonblock(sysfd, true)
	if err != nil {
		debug("FD xxx: NF: sysfd=%d, err=%v", sysfd, err)
		return nil, err
	}
	// Initialize FD
	fd := &FD{sysfd: sysfd}
	fd.id = fdM.GetID()
	fd.rco = sync.NewCond(&fd.rmu)
	fd.wco = sync.NewCond(&fd.wmu)
	// Add to Epoll set. We may imediatelly start receiving events
	// after this. They will be dropped since the FD is not yet in
	// fdMap. It's ok. Nobody is waiting on this FD yet, anyway.
	ev := syscall.EpollEvent{
		Events: syscall.EPOLLIN |
			syscall.EPOLLOUT |
			syscall.EPOLLRDHUP |
			(syscall.EPOLLET & 0xffffffff),
		Fd: int32(fd.id)}
	err = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, fd.sysfd, &ev)
	if err != nil {
		debug("FD %03d: NF: sysfd=%d, err=%v", fd.id, fd.sysfd, err)
		return nil, err
	}
	// Add to fdMap
	fdM.AddFD(fd)
	debug("FD %03d: NF: sysfd=%d", fd.id, fd.sysfd)
	return fd, nil
}

/*

- We use Edge Triggered notifications so we only sleep (cond.Wait)
  when the syscall (Read/Write) returns with EAGAIN

- While sleeping we don't have the lock. We re-acquire it immediatelly
  after waking-up (cond.Wait does). After waking up we must re-check
  the "closed" and "tmo" conditions. It is also possible that after
  waking up the syscall (Read/Write) returns EAGAIN again (because
  another goroutine might have stepped in front of us, before
  we re-acquire the lock, and read the data or written to the buffer
  before we do).

- The following can wake us: A read or write event from EpollWait. A
  call to Close (sets fd.closed = 1). The expiration of a timer /
  deadline (sets fd.r/wto = 1). Read and write events wake-up a single
  goroutine waiting on the FD (cond.Signal). Closes and timeouts
  wake-up all threads waiting on the FD (cond.Broadcast).

- The Read/Write methods must wake-up the next goroutine (possibly)
  waiting on the FD, in the following cases: A Read/Write syscall
  error (incl. zero-bytes return) and when they read/write *all* the
  requested data.

*/

func (fd *FD) Read(p []byte) (n int, err error) {
	fd.rco.L.Lock()
	for {
		if fd.closed {
			debug("FD %03d: RD: Closed", fd.id)
			fd.rco.L.Unlock()
			return 0, ErrClosed
		}
		if fd.rto {
			debug("FD %03d: RD: Timeout", fd.id)
			fd.rco.L.Unlock()
			return 0, ErrTimeout
		}
		n, err = syscall.Read(fd.sysfd, p)
		debug("FD %03d: RD: sysfd=%d n=%d, err=%v",
			fd.id, fd.sysfd, n, err)
		if err != nil {
			n = 0
			if err != syscall.EAGAIN {
				// I/O error. Wake-up next.
				fd.rco.Signal()
				break
			}
			// EAGAIN
			debug("FD %03d: RD: Wait", fd.id)
			fd.rco.Wait()
			debug("FD %03d: RD: Wakeup", fd.id)
			continue
		}
		if n == 0 && len(p) != 0 {
			// Remote end closed. Wake-up next.
			fd.rco.Signal()
			err = io.EOF
			break
		}
		// Successful read
		if n == len(p) {
			// Read all we asked. Wake-up next.
			fd.rco.Signal()
		}
		break
	}
	fd.rco.L.Unlock()
	return n, err
}

func (fd *FD) Write(p []byte) (n int, err error) {
	fd.wco.L.Lock()
	for {
		if fd.closed {
			debug("FD %03d: WR: Closed", fd.id)
			fd.wco.L.Unlock()
			return 0, ErrClosed
		}
		if fd.wto {
			debug("FD %03d: WR: Timeout", fd.id)
			fd.wco.L.Unlock()
			return 0, ErrTimeout
		}
		n, err = syscall.Write(fd.sysfd, p)
		debug("FD %03d: WR: sysfd=%d n=%d, err=%v",
			fd.id, fd.sysfd, n, err)
		if err != nil {
			n = 0
			if err != syscall.EAGAIN {
				// I/O error. Wake-up next.
				fd.wco.Signal()
				break
			}
			// EAGAIN
			debug("FD %03d: WR: Wait", fd.id)
			fd.wco.Wait()
			debug("FD %03d: WR: Wakeup", fd.id)
			continue
		}
		if n == 0 && len(p) != 0 {
			// Unexpected EOF error. Wake-up next.
			fd.wco.Signal()
			err = io.ErrUnexpectedEOF
			break
		}
		// Successful write
		if n == len(p) {
			// Wrote all we asked. Wake-up next.
			fd.wco.Signal()
		}
		break
	}
	fd.wco.L.Unlock()
	return n, err
}

func (fd *FD) Close() error {
	// Take both locks, to exclude read and write operations from
	// accessing a closed sysfd.
	fd.rco.L.Lock()
	fd.wco.L.Lock()
	if fd.closed {
		debug("FD %03d: CL: Closed", fd.id)
		fd.wco.L.Unlock()
		fd.rco.L.Unlock()
		return ErrClosed
	}
	fd.closed = true

	// ev is not used by EpollCtl/DEL. Just don't pass a nil
	// pointer.
	var ev syscall.EpollEvent
	err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_DEL, fd.sysfd, &ev)
	if err != nil {
		log.Printf("poller: EpollCtl/DEL (fd=%d, sysfd=%d): %s",
			fd.id, fd.sysfd, err.Error())
	}
	fdM.DelFD(fd.id)
	err = syscall.Close(fd.sysfd)

	// Wake up everybody waiting on the FD.
	fd.rco.Broadcast()
	fd.wco.Broadcast()

	debug("FD %03d: CL: close(sysfd=%d)", fd.id, fd.sysfd)
	fd.wco.L.Unlock()
	fd.rco.L.Unlock()

	return err
}

func readEvent(ev *syscall.EpollEvent) {
	fd := fdM.GetFD(int(ev.Fd))
	if fd == nil {
		// Drop event. Probably stale.
		debug("FD %03d: ER: Dropped: 0x%x", ev.Fd, ev.Events)
		return
	}
	fd.rco.L.Lock()
	if fd.closed {
		debug("FD %03d: ER: Closed: 0x%x", fd.id, ev.Events)
		fd.rco.L.Unlock()
		return
	}
	if fd.rto {
		debug("FD %03d: ER: Timeout: 0x%x", fd.id, ev.Events)
		fd.rco.L.Unlock()
		return
	}
	// Wake up one of the goroutines waiting on the FD.
	fd.rco.Signal()
	debug("FD %03d: ER: Delivered: 0x%x", fd.id, ev.Events)
	fd.rco.L.Unlock()
}

func writeEvent(ev *syscall.EpollEvent) {
	fd := fdM.GetFD(int(ev.Fd))
	if fd == nil {
		// Drop event. Probably stale.
		debug("FD %03d: EW: Dropped: 0x%x", ev.Fd, ev.Events)
		return
	}
	fd.wco.L.Lock()
	if fd.closed {
		debug("FD %03d: EW: Closed: 0x%x", fd.id, ev.Events)
		fd.wco.L.Unlock()
		return
	}
	if fd.wto {
		debug("FD %03d: EW: Timeout: 0x%x", fd.id, ev.Events)
		fd.wco.L.Unlock()
		return
	}
	// Wake up one of the goroutines waiting on the FD.
	fd.wco.Signal()
	debug("FD %03d: EW: Delivered: 0x%x", fd.id, ev.Events)
	fd.wco.L.Unlock()
}

func isReadEvent(ev *syscall.EpollEvent) bool {
	return ev.Events&(syscall.EPOLLIN|
		syscall.EPOLLRDHUP|
		syscall.EPOLLHUP|
		syscall.EPOLLERR) != 0
}

func isWriteEvent(ev *syscall.EpollEvent) bool {
	return ev.Events&(syscall.EPOLLOUT|
		syscall.EPOLLHUP|
		syscall.EPOLLERR) != 0
}

func poller() {
	debug("Started.")
	events := make([]syscall.EpollEvent, 128)
	for {
		n, err := syscall.EpollWait(epfd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			log.Panicf("poller: EpollWait: %s", err.Error())
		}
		for i := 0; i < n; i++ {
			ev := &events[i]
			if isReadEvent(ev) {
				readEvent(ev)
			}
			if isWriteEvent(ev) {
				writeEvent(ev)
			}
		}
	}
}

func debug(format string, v ...interface{}) {
	if debug_enable {
		log.Printf("poller: "+format, v...)
	}
}

const debug_enable = true

var fdM fdMap
var epfd int = -1

func init() {
	fdM.Init(128)
	fd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		log.Panicf("poller: EpollCreate1: %s", err.Error())
	}
	epfd = fd
	go poller()
}

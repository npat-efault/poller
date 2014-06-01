package poller

import (
	"io"
	"log"
	"sync"
	"syscall"
	"time"
)

// TODO(npat): Perhaps lock fdMap with a RWMutex??

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

// FD is a poller file-descriptor. Typically a file-descriptor
// connected to a terminal, a pseudo terminal, a character device, a
// FIFO (named pipe), of any unix stream that supports the epoll(7)
// interface.
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

// NewFD initializes and returns a new poller file-descriptor from the
// given system (unix) file-descriptor. After calling NewFD the system
// file-descriptor must be used only through the poller.FD methods,
// not directly. NewFD sets the system file-descriptor to non-blocking
// mode.
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

/*

- We use Edge Triggered notifications so we can only sleep (cond.Wait)
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
  wake-up all goroutines waiting on the FD (cond.Broadcast).

- The Read/write1 methods must wake-up the next goroutine (possibly)
  waiting on the FD, in the following cases: (a) When a Read/Write
  syscall error (incl. zero-bytes return) is detected, and (b) When
  they successfully read/write *all* the requested data.

- The Read and Write methods implement the io.Reader and io.Writer
  interfaces respectivelly. There is an asymetry between these
  interfaces: io.Readers are allowed to read less than requested,
  without signaling an error. io.Writers are *not*. The write1 method
  is exactly symmetrical with Read (issues only one system call and
  can write less than requested). Write wraps write1.

*/

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
	fd.rco.L.Lock()
	defer fd.rco.L.Unlock()
	for {
		if fd.closed {
			debug("FD %03d: RD: Closed", fd.id)
			return 0, ErrClosed
		}
		if fd.rto {
			debug("FD %03d: RD: Timeout", fd.id)
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
	return n, err
}

func (fd *FD) write1(p []byte) (n int, err error) {
	fd.wco.L.Lock()
	defer fd.wco.L.Unlock()
	for {
		if fd.closed {
			debug("FD %03d: WR: Closed", fd.id)
			return 0, ErrClosed
		}
		if fd.wto {
			debug("FD %03d: WR: Timeout", fd.id)
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
	return n, err
}

// Writes len(p) bytes from p to the file-descriptor.  Returns the
// number of bytes written (0 <= nn <= len(p)) and any error
// encountered that caused the write to stop early.  Write returns a
// non-nil error if it returns nn < len(p). Write is compatible with
// the Write method of the io.Writer interface. In addition Write
// honors the timeout set by (*FD).SetDeadline and
// (*FD).SetWriteDeadline. If less than len(p) data are writen before
// the timeout expires Write returns with err == ErrTimeout (and nn <
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
//         poller.FD
//         mu sync.Mutex
//     }
//
//     func (fd *myFD) Write(p []byte) (n int, err error) {
//         fd.mu.Lock()
//         n, err = fd.FD.Write(p)
//         fd.mu.Unlock()
//         return n, err
//     }
//
func (fd *FD) Write(p []byte) (nn int, err error) {
	for nn != len(p) {
		var n int
		n, err = fd.write1(p[nn:])
		if err != nil {
			break
		}
		nn += n
	}
	return nn, err
}

/*

  - Close must acquire *both* locks (read and write) before
    proceeding. Otherwise there is the danger that a concurent read or
    write operation will access a closed (and possibly re-opened)
    sysfd.

  - Misc operations (e.g. ioctls) also acquire bock locks, though it
    is not strictly necessary.

  - Deadline operations (Set*Deadline) acquire the respective
    locks. SetReadDeadline acquires the read lock, SetWriteDeadline
    acquires the write lock.

  - In order to signal (wake-up) a goroutine waiting on the FD we must
    hold the respective lock.

*/

// Close the file descriptor. It is ok to call Close concurently with
// other operations (Read, Write, SetDeadline, etc) on the same
// file-descriptor. In this case ongoing operations will return with
// ErrClosed. Any subsequent operations (after Close) on the
// file-descriptor will also fail with ErrClosed.
func (fd *FD) Close() error {
	// Take both locks, to exclude read and write operations from
	// accessing a closed sysfd.
	if err := fd.Lock(); err != nil {
		debug("FD %03d: CL: Closed", fd.id)
		return err
	}
	defer fd.Unlock()
	fd.closed = true

	// ev is not used by EpollCtl/DEL. Just don't pass a nil
	// pointer.
	var ev syscall.EpollEvent
	err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_DEL, fd.sysfd, &ev)
	if err != nil {
		log.Printf("poller: EpollCtl/DEL (fd=%d, sysfd=%d): %s",
			fd.id, fd.sysfd, err.Error())
	}
	if fd.rtm != nil {
		fd.rtm.Stop()
	}
	if fd.wtm != nil {
		fd.wtm.Stop()
	}
	fdM.DelFD(fd.id)
	err = syscall.Close(fd.sysfd)

	// Wake up everybody waiting on the FD.
	fd.rco.Broadcast()
	fd.wco.Broadcast()

	debug("FD %03d: CL: close(sysfd=%d)", fd.id, fd.sysfd)
	return err
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
	fd.rco.L.Lock()
	if fd.closed {
		fd.rco.L.Unlock()
		return ErrClosed
	}
	fd.rdl = t
	fd.rto = false
	if t.IsZero() {
		if fd.rtm != nil {
			fd.rtm.Stop()
		}
		debug("FD %03d: DR: Removed dl", fd.id)
	} else {
		d := t.Sub(time.Now())
		if fd.rtm == nil {
			id := fd.id
			fd.rtm = time.AfterFunc(d, func() { readTmo(id) })
		} else {
			fd.rtm.Stop()
			fd.rtm.Reset(d)
		}
		debug("FD %03d: DR: Set dl: %v", fd.id, d)
	}
	fd.rco.L.Unlock()
	return nil
}

// SetWriteDeadline sets the deadline for Write operations on the
// file-descriptor.
func (fd *FD) SetWriteDeadline(t time.Time) error {
	fd.wco.L.Lock()
	if fd.closed {
		fd.wco.L.Unlock()
		return ErrClosed
	}
	fd.wdl = t
	fd.wto = false
	if t.IsZero() {
		if fd.wtm != nil {
			fd.wtm.Stop()
		}
		debug("FD %03d: DW: Removed dl", fd.id)
	} else {
		d := t.Sub(time.Now())
		if fd.wtm == nil {
			id := fd.id
			fd.wtm = time.AfterFunc(d, func() { writeTmo(id) })
		} else {
			fd.wtm.Stop()
			fd.wtm.Reset(d)
		}
		debug("FD %03d: DW: Set dl: %v", fd.id, d)
	}
	fd.wco.L.Unlock()
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
func (fd *FD) Lock() error {
	fd.rco.L.Lock()
	fd.wco.L.Lock()
	if fd.closed {
		fd.wco.L.Unlock()
		fd.rco.L.Unlock()
		return ErrClosed
	}
	return nil
}

// Unlock unlocks the file-descriptor.
func (fd *FD) Unlock() {
	fd.wco.L.Unlock()
	fd.rco.L.Unlock()
}

// Sysfd returns the system file-descriptor associated with the given
// poller file-descriptor. See also (*FD).Lock.
func (fd *FD) Sysfd() int {
	return fd.sysfd
}

func readTmo(id int) {
	fd := fdM.GetFD(int(id))
	if fd == nil {
		// Drop event. Probably stale FD.
		debug("FD %03d: TR: Dropped", id)
		return
	}
	fd.rco.L.Lock()
	if !fd.rto && !fd.rdl.IsZero() && !fd.rdl.After(time.Now()) {
		fd.rto = true
		fd.rco.Broadcast()
		debug("FD %03d: TR: Broadcast", fd.id)
	} else {
		debug("FD %03d: TR: Ignored", fd.id)
	}
	fd.rco.L.Unlock()
}

func writeTmo(id int) {
	fd := fdM.GetFD(int(id))
	if fd == nil {
		// Drop event. Probably stale FD.
		debug("FD %03d: TW: Dropped", id)
		return
	}
	fd.wco.L.Lock()
	if !fd.wto && !fd.wdl.IsZero() && !fd.wdl.After(time.Now()) {
		fd.wto = true
		fd.wco.Broadcast()
		debug("FD %03d: TW: Broadcast", fd.id)
	} else {
		debug("FD %03d: TW: Ignored", fd.id)
	}
	fd.wco.L.Unlock()
}

func readEvent(ev *syscall.EpollEvent) {
	fd := fdM.GetFD(int(ev.Fd))
	if fd == nil {
		// Drop event. Probably stale FD.
		debug("FD %03d: ER: Dropped: 0x%x", ev.Fd, ev.Events)
		return
	}
	fd.rco.L.Lock()
	if !fd.closed && !fd.rto {
		// Wake up one of the goroutines waiting on the FD.
		fd.rco.Signal()
		debug("FD %03d: ER: Signal: 0x%x", fd.id, ev.Events)
	} else {
		debug("FD %03d: ER: Closed | Tmo: 0x%x", fd.id, ev.Events)
	}
	fd.rco.L.Unlock()
}

func writeEvent(ev *syscall.EpollEvent) {
	fd := fdM.GetFD(int(ev.Fd))
	if fd == nil {
		// Drop event. Probably stale FD.
		debug("FD %03d: EW: Dropped: 0x%x", ev.Fd, ev.Events)
		return
	}
	fd.wco.L.Lock()
	if !fd.closed && !fd.wto {
		// Wake up one of the goroutines waiting on the FD.
		fd.wco.Signal()
		debug("FD %03d: EW: Signal: 0x%x", fd.id, ev.Events)
	} else {
		debug("FD %03d: EW: Closed | Tmo: 0x%x", fd.id, ev.Events)
	}
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

const debug_enable = false

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

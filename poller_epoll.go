// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build linux,!noepoll

// Implementation based on epoll(7), for linux.

package poller

import (
	"fmt"
	"io"
	"log"
	"sync"
	"syscall"
	"time"
)

// TODO(npat): Add finalizer

func newFD(sysfd int) (*FD, error) {
	// Set sysfd to non-blocking mode
	err := syscall.SetNonblock(sysfd, true)
	if err != nil {
		debugf("FD xxx: NF: sysfd=%d, err=%v", sysfd, err)
		return nil, err
	}
	// Initialize FD
	fd := &FD{sysfd: sysfd}
	fd.id = fdM.GetID()
	fd.r.cond = sync.NewCond(&fd.r.mu)
	fd.w.cond = sync.NewCond(&fd.w.mu)
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
		debugf("FD %03d: NF: sysfd=%d, err=%v", fd.id, fd.sysfd, err)
		return nil, err
	}
	// Add to fdMap
	fdM.AddFD(fd)
	debugf("FD %03d: NF: sysfd=%d", fd.id, fd.sysfd)
	return fd, nil
}

/*

- We use Edge Triggered notifications so we can only sleep (cond.Wait)
  when the syscall (Read/Write) returns with EAGAIN

- While sleeping we don't have the lock. We re-acquire it immediatelly
  after waking-up (cond.Wait does). After waking up we must re-check
  the "closed" and "timeout" conditions. It is also possible that
  after waking up the syscall (Read/Write) returns EAGAIN again
  (because another goroutine might have stepped in front of us, before
  we re-acquire the lock, and read the data or written to the buffer
  before we do).

- The following can wake us: A read or write event from EpollWait. A
  call to Close (sets fd.closed = 1). The expiration of a timer /
  deadline (sets fd.r/w.timeout = 1). Read and write events wake-up a
  single goroutine waiting on the FD (cond.Signal). Closes and
  timeouts wake-up all goroutines waiting on the FD (cond.Broadcast).

- We must wake-up the next goroutine (possibly) waiting on the FD, in
  the following cases: (a) When a Read/Write syscall error
  (incl. zero-bytes return) is detected, and (b) When we successfully
  read/write *all* the requested data.

*/

func fdIO(fd *FD, write bool, p []byte) (n int, err error) {
	var fdc *fdCtl
	var sysc func(int, []byte) (int, error)
	var errEOF error
	var dpre string

	if !write {
		// Prepare things for Read.
		fdc = &fd.r
		sysc = syscall.Read
		errEOF = io.EOF
		if debug_enable {
			dpre = fmt.Sprintf("FD %03d: RD:", fd.id)
		}
	} else {
		// Prepare things for Write.
		fdc = &fd.w
		sysc = syscall.Write
		errEOF = io.ErrUnexpectedEOF
		if debug_enable {
			dpre = fmt.Sprintf("FD %03d: WR:", fd.id)
		}
	}
	// Read & Write are identical
	fdc.cond.L.Lock()
	defer fdc.cond.L.Unlock()
	for {
		if fd.closed {
			debugf("%s Closed", dpre)
			return 0, ErrClosed
		}
		if fdc.timeout {
			debugf("%s Timeout", dpre)
			return 0, ErrTimeout
		}
		n, err = sysc(fd.sysfd, p)
		debugf("%s sysfd=%d n=%d, err=%v", dpre, fd.sysfd, n, err)
		if err != nil {
			n = 0
			if err != syscall.EAGAIN {
				// I/O error. Wake-up next.
				fdc.cond.Signal()
				break
			}
			// EAGAIN
			debugf("%s Wait", dpre)
			fdc.cond.Wait()
			debugf("%s Wakeup", dpre)
			continue
		}
		if n == 0 && len(p) != 0 {
			// Remote end closed. Wake-up next.
			fdc.cond.Signal()
			err = errEOF
			break
		}
		// Successful syscall
		if n == len(p) {
			// R/W all we asked. Wake-up next.
			fdc.cond.Signal()
		}
		break
	}
	return n, err
}

/*

  - Close / CloseUnlocked must acquire *all* locks (read, write and C)
    before proceeding. Otherwise there is the danger that a concurent
    read, write, or misc operation will access a closed (and possibly
    re-opened) sysfd.

  - Misc operations (e.g. ioctls) acquire only the C lock. Misc
    operations *can* happen concurrently with reads and writes on the
    same (or other) fds. They are only protected against concurrent
    close's.

  - Deadline operations (Set*Deadline) acquire the respective
    locks. SetReadDeadline acquires the read lock, SetWriteDeadline
    acquires the write lock.

  - In order to signal (wake-up) a goroutine waiting on the FD we must
    hold the respective lock.

*/

func (fd *FD) closeUnlocked() error {
	// Caller MUST already hold the C lock. Take the both R and W
	// locks, to exclude Read and Write operations from accessing
	// a closed sysfd.
	fd.r.cond.L.Lock()
	fd.w.cond.L.Lock()

	fd.closed = true
	// ev is not used by EpollCtl/DEL. Just don't pass a nil
	// pointer.
	var ev syscall.EpollEvent
	err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_DEL, fd.sysfd, &ev)
	if err != nil {
		log.Printf("poller: EpollCtl/DEL (fd=%d, sysfd=%d): %s",
			fd.id, fd.sysfd, err.Error())
	}
	if fd.r.timer != nil {
		fd.r.timer.Stop()
	}
	if fd.w.timer != nil {
		fd.w.timer.Stop()
	}
	fdM.DelFD(fd.id)
	err = syscall.Close(fd.sysfd)

	// Wake up everybody waiting on the FD.
	fd.r.cond.Broadcast()
	fd.w.cond.Broadcast()

	debugf("FD %03d: CL: close(sysfd=%d)", fd.id, fd.sysfd)

	fd.w.cond.L.Unlock()
	fd.r.cond.L.Unlock()
	return err
}

func timerEvent(id int, write bool) {
	var fdc *fdCtl
	var dpre string

	if debug_enable {
		if !write {
			dpre = fmt.Sprintf("FD %03d: TR:", id)
		} else {
			dpre = fmt.Sprintf("FD %03d: TW:", id)
		}
	}
	fd := fdM.GetFD(id)
	if fd == nil {
		// Drop event. Probably stale FD.
		debugf("%s Dropped", dpre)
		return
	}
	if !write {
		// A Read timeout
		fdc = &fd.r
	} else {
		// A Write timeout
		fdc = &fd.w
	}
	fdc.cond.L.Lock()
	if !fd.closed && !fdc.timeout &&
		!fdc.deadline.IsZero() && !fdc.deadline.After(time.Now()) {
		fdc.timeout = true
		fdc.cond.Broadcast()
		debugf("%s Broadcast", dpre)
	} else {
		debugf("%s Ignored", dpre)
	}
	fdc.cond.L.Unlock()
}

func epollEvent(ev *syscall.EpollEvent, write bool) {
	var fdc *fdCtl
	var dpre string

	if debug_enable {
		if !write {
			dpre = fmt.Sprintf("FD %03d: ER:", ev.Fd)
		} else {
			dpre = fmt.Sprintf("FD %03d: EW:", ev.Fd)
		}
	}
	fd := fdM.GetFD(int(ev.Fd))
	if fd == nil {
		// Drop event. Probably stale FD.
		debugf("%s Dropped: 0x%x", dpre, ev.Events)
		return
	}
	if !write {
		// A read event
		fdc = &fd.r
	} else {
		// A write event
		fdc = &fd.w
	}
	fdc.cond.L.Lock()
	if !fd.closed && !fdc.timeout {
		// Wake up one of the goroutines waiting on the FD.
		fdc.cond.Signal()
		debugf("%s Signal: 0x%x", dpre, ev.Events)
	} else {
		debugf("%s Ignored: 0x%x", dpre, ev.Events)
	}
	fdc.cond.L.Unlock()
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
	debugf("Started.")
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
				epollEvent(ev, false)
			}
			if isWriteEvent(ev) {
				epollEvent(ev, true)
			}
		}
	}
}

var fdM fdMap
var epfd int = -1

// TODO(npat): Support systems that don't have EpollCreate1

func init() {
	fdM.Init(128)
	fd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		log.Panicf("poller: EpollCreate1: %s", err.Error())
	}
	epfd = fd
	go poller()
}

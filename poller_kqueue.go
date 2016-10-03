// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build freebsd,!nokqueue

// Implementation based on kqueue(7), for BSD.

package poller

import (
	"fmt"
	"io"
	"log"
	"sync"
	"syscall"
	"time"
)

func newFD(sysfd int) (*FD, error) {
	// Set sysfd to non-blocking mode
	err := syscall.SetNonblock(sysfd, true)
	if err != nil {
		debugf("FD xxx: NF: sysfd=%d, err=%v", sysfd, err)
		return nil, err
	}
	// Initialize FD. We don't generate arbitrary ids, instead we
	// use sysfd as the fdMap id. In effect, fd.id is alays ==
	// fd.sysfd. Remember that sysfd's can be reused; we must be
	// carefull not to allow this to mess things up.
	fd := &FD{id: sysfd, sysfd: sysfd}
	fd.r.cond = sync.NewCond(&fd.r.mu)
	fd.w.cond = sync.NewCond(&fd.w.mu)
	// Add to Kqueue. We add 2 event-filters for each FD; one for
	// read-activity and one for write-activity. Both filters are
	// edge-triggered (EV_CLEAR flag set). We may imediatelly
	// start receiving events after this. They will be dropped
	// since the FD is not yet in fdMap. It's ok. Nobody is
	// waiting on this FD yet, anyway.
	evs := make([]syscall.Kevent_t, 2)
	syscall.SetKevent(&evs[0], sysfd,
		syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_CLEAR)
	syscall.SetKevent(&evs[1], sysfd,
		syscall.EVFILT_WRITE, syscall.EV_ADD|syscall.EV_CLEAR)
	_, err = syscall.Kevent(kqfd, evs, nil, nil)
	if err != nil {
		debugf("FD %03d: NF: err=%v", fd.id, err)
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

- We must wake-up the next goroutine (possibly) waiting on the FD
  whenever the current operation (read or write) succeeds or fails
  with something other than EAGAIN.

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
		// Successful syscall. Wake-up next.
		fdc.cond.Signal()
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
	if fd.r.timer != nil {
		fd.r.timer.Stop()
	}
	if fd.w.timer != nil {
		fd.w.timer.Stop()
	}
	fdM.DelFD(fd.id)
	// Event-filters will be removed from kqueue when sysfd is closed.
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
	// The fd returned by fdM.GetFD may be a new fd (the fd may
	// have been closed, and a new one opened with the same
	// sysfd). It won't harm because checking fd.{r,w}.deadline
	// will determine if this is a valid timeout event.
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

func kqueueEvent(ev *syscall.Kevent_t, write bool) {
	var fdc *fdCtl
	var dpre string

	if debug_enable {
		if !write {
			dpre = fmt.Sprintf("FD %03d: ER:", ev.Ident)
		} else {
			dpre = fmt.Sprintf("FD %03d: EW:", ev.Ident)
		}
	}
	// The fd returned by fdM.GetFD may be a new fd (the fd may
	// have been closed, and a new one opened with the same
	// sysfd). It's ok. Worts-case you get a spurious wake-up
	// (cond.Signal).
	fd := fdM.GetFD(int(ev.Ident))
	if fd == nil {
		// Drop event. Probably stale FD.
		debugf("%s Dropped", dpre)
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
		// Wake up a goroutine waiting on the FD.
		fdc.cond.Signal()
		debugf("%s Signal", dpre)
	} else {
		debugf("%s Ignored", dpre)
	}
	fdc.cond.L.Unlock()
}

func poller() {
	debugf("Started.")
	events := make([]syscall.Kevent_t, 128)
	for {
		n, err := syscall.Kevent(kqfd, events, nil, nil)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			log.Panicf("poller: Kevent: %s", err.Error())
		}
		for i := 0; i < n; i++ {
			ev := &events[i]
			if ev.filter == syscall.EVFILT_READ {
				kqueueEvent(ev, false)
			}
			if ev.filter == syscall.EVFILT_WRITE {
				kqueueEvent(ev, true)
			}
		}
	}
}

var fdM fdMap
var kqfd int = -1

// TODO(npat): Support systems that don't have EpollCreate1

func init() {
	fdM.Init(128)
	fd, err := syscall.Kqueue()
	if err != nil {
		log.Panicf("poller: Kqueue: %s", err.Error())
	}
	syscall.CloseOnExec(fd)
	kqfd = fd
	go poller()
}

// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build linux,noepoll freebsd netbsd openbsd darwin dragonfly solaris

// Implementation based on select(2), for most Unix-like systems

package poller

import (
	"fmt"
	"io"
	"log"
	"sync"
	"syscall"
	"time"
)

// selectCtx contains variables used to interact with the select
// goroutine.
type selectCtx struct {
	// Must hold to access rset, wset and fdmax.
	sync.Mutex
	// Filedes sets to select on. Fds are set by Read and Write
	// operations and cleared either by the select goroutine, or
	// by Close and timeout-events.
	rset, wset fdSet
	fdmax      int
	// Pipe ends (read/write) used to "interrupt" select. Read and
	// Write operations write on pfdw when altering the wset /
	// rset filedes-sets to notify the select goroutine (unblock
	// the select(2) call).
	pfdr, pfdw int
	// Dummy buffer to use for writing to pfdw.
	b []byte
	// Dummy buffer used for draining pfdr
	b1 []byte
}

func (sc *selectCtx) Init() error {
	sc.rset.Zero()
	sc.wset.Zero()
	sc.fdmax = -1
	// Create and configure the pipe-ends.
	b := make([]int, 2)
	err := syscall.Pipe(b)
	if err != nil {
		return err
	}
	sc.pfdr, sc.pfdw = b[0], b[1]
	syscall.CloseOnExec(sc.pfdr)
	syscall.CloseOnExec(sc.pfdw)
	err = syscall.SetNonblock(sc.pfdr, true)
	if err != nil {
		syscall.Close(sc.pfdr)
		syscall.Close(sc.pfdw)
		return err
	}
	err = syscall.SetNonblock(sc.pfdw, true)
	if err != nil {
		syscall.Close(sc.pfdr)
		syscall.Close(sc.pfdw)
		return err
	}
	// pfdr (read-end of pipe) is set (and remains set forever) on
	// rset.
	sc.rset.Set(sc.pfdr)
	sc.fdmax = sc.pfdr
	// allocate dummy selectCtx.{b,b1} buffers.
	sc.b = make([]byte, 1)
	sc.b1 = make([]byte, 128)
	return nil
}

type dirFlag int

const (
	dirRead = 1 << iota
	dirWrite
)

func (sc *selectCtx) Set(sysfd int, rw dirFlag) {
	if rw == 0 {
		return
	}
	sc.Lock()
	if rw&dirRead != 0 {
		sc.rset.Set(sysfd)
	}
	if rw&dirWrite != 0 {
		sc.wset.Set(sysfd)
	}
	if sysfd > sc.fdmax {
		sc.fdmax = sysfd
	}
	sc.Unlock()
}

func (sc *selectCtx) Clear(sysfd int, rw dirFlag) {
	if rw == 0 {
		return
	}
	sc.Lock()
	if rw&dirRead != 0 {
		sc.rset.Clr(sysfd)
	}
	if rw&dirWrite != 0 {
		sc.wset.Clr(sysfd)
	}
	// NOTE(npat): It would be nice if, at some point, we could
	// reset sc.fdmax
	sc.Unlock()
}

func (sc *selectCtx) Notify() {
	// Write 1 byte, don't care what
	_, err := syscall.Write(sc.pfdw, sc.b)
	// Don't care if it fails with EAGAIN
	if err != nil && err != syscall.EAGAIN {
		log.Panicf("poller: Cannot notify select: %s", err.Error())
	}
}

func (sc *selectCtx) getSets() (nfds int, rs, ws fdSet) {
	sc.Lock()
	nfds, rs, ws = sc.fdmax+1, sc.rset, sc.wset
	sc.Unlock()
	return nfds, rs, ws
}

func (sc *selectCtx) drainPfdr() {
	for {
		_, err := syscall.Read(sc.pfdr, sc.b1)
		if err != nil {
			if err == syscall.EAGAIN {
				break
			}
			log.Panicf("poller: Cannot drain pipe: %s", err.Error())
		}
	}
}

func setStr(s *fdSet, nfd int) string {
	var str string
	n := nfd
	if n > 16 {
		n = 16
	}
	for i := 0; i < n; i++ {
		if s.IsSet(i) {
			str += "1"
		} else {
			str += "0"
		}
		if (i+1)%4 == 0 {
			str += " "
		}
	}
	if n < nfd {
		str += "..."
	}
	return str
}

func debugSets(pref string, rs, ws *fdSet, nfds int) {
	if debug_enable {
		debugf("%srs: %s, ws: %s", pref,
			setStr(rs, nfds), setStr(ws, nfds))
	}
}

// run runs as the select-goroutine. It never returns.
func (sc *selectCtx) run() {
	for {
		nfds, rs, ws := sc.getSets()
		debugSets("SL  IN: ", &rs, &ws, nfds)
		n, err := uxSelect(nfds, &rs, &ws, nil, nil)
		if err != nil {
			if err == syscall.EBADF || err == syscall.EINTR {
				// A filedes may have been closed. In
				// this case the respective bits will
				// have been cleared in sc.rset and
				// sc.wset, so just retrying the
				// select will get the job done.
				debugf("SL OUT: %s", err.Error())
				continue
			}
			log.Panicf("poller: select(2) failed: %s", err.Error())
		}
		// Bellow, when we lookup the fd using sysfd
		// (fdM.GetFD), the following corner-cases may happen:
		//
		// - The fd may have been closed and removed from
		// fdM. In this case fdM.Get will fail (fd = nil) and
		// we have to do nothing. The respective sc.rset and
		// sc.wset bits will have been cleared by close.
		//
		// - The fd may have been closed and removed from fdM,
		// and another one with the same sysfd may have taken
		// its place, before we call fdM.Get. In this case we
		// may try to wake-up (cond.Broadcast) a read or write
		// that is not waiting. That's ok.
		//
		// - We get an fd, and it gets closed *after* we get
		// it. The check for fd.closed (which we do while
		// holding the read or write lock) will catch this.
		//
		debugSets("SL OUT: ", &rs, &ws, nfds)
		for sysfd := 0; sysfd < nfds && n > 0; sysfd++ {
			isr, isw := rs.IsSet(sysfd), ws.IsSet(sysfd)
			if !isr && !isw {
				continue
			}
			if sysfd == sc.pfdr {
				n--
				sc.drainPfdr()
				continue
			}
			fd := fdM.GetFD(sysfd)
			if isr {
				n--
				if fd != nil {
					fd.r.cond.L.Lock()
					if !fd.closed && !fd.r.timeout {
						fd.r.cond.Broadcast()
						sc.Clear(sysfd, dirRead)
						debugf("FD %03d: ER: Signal", sysfd)
					} else {
						debugf("FD %03d: ER: Ignored", sysfd)
					}
					fd.r.cond.L.Unlock()
				} else {
					debugf("FD %03d: ER: Dropped", sysfd)
				}
			}
			if isw {
				n--
				if fd != nil {
					fd.w.cond.L.Lock()
					if !fd.closed && !fd.w.timeout {
						fd.w.cond.Broadcast()
						sc.Clear(sysfd, dirWrite)
						debugf("FD %03d: EW: Signal", sysfd)
					} else {
						debugf("FD %03d: EW: Ignored", sysfd)
					}
					fd.w.cond.L.Unlock()
				} else {
					debugf("FD %03d: EW: Dropped", sysfd)
				}
			}
		}
	}
}

func newFD(sysfd int) (*FD, error) {
	// Set sysfd to non-blocking mode
	err := syscall.SetNonblock(sysfd, true)
	if err != nil {
		debugf("FD %03d: NF: SetNonBlock: %s", sysfd, err.Error())
		return nil, err
	}
	// Check if sysfd is select-able
	// NOTE(npat): Is this useful? Can it ever fail?
	var rs fdSet
	var tv syscall.Timeval
	rs.Zero()
	rs.Set(sysfd)
	_, err = uxSelect(sysfd+1, &rs, nil, nil, &tv)
	if err != nil {
		debugf("FD %03d: NF: select(2): %s", sysfd, err.Error())
		return nil, err
	}
	// Initialize FD. We don't generate arbitrary ids, instead we
	// use sysfd as the fdMap id. In effect, fd.id is alays ==
	// fd.sysfd. Remember that sysfd's can be reused; we must be
	// carefull not to allow this to mess things up.
	fd := &FD{id: sysfd, sysfd: sysfd}
	fd.r.cond = sync.NewCond(&fd.r.mu)
	fd.w.cond = sync.NewCond(&fd.w.mu)
	// Add to fdMap
	fdM.AddFD(fd)
	debugf("FD %03d: NF: Ok", fd.id)
	return fd, nil
}

/*

- We use Level-Triggered notifications. We first attempt to read or
  write, and if we get EAGAIN, we add sysfd to the set of monitored
  filedescriptors, and sleep (cond.Wait) waiting for the
  select-goroutine to wake us. Once the select goroutine wakes us, it
  removes sysfd from the set of monitored file-descriptors.

- While sleeping we don't have the lock. We re-acquire it immediatelly
  after waking-up (cond.Wait does). After waking up we must re-check
  the "closed" and "timeout" conditions. It is also possible that
  after waking up the syscall (Read/Write) returns EAGAIN again
  (because another goroutine might have stepped in front of us, before
  we re-acquire the lock, and read the data or written to the buffer
  before we do).

- The following can wake us: A read or write event from the select
  goroutine. A call to Close (sets fd.closed = 1). The expiration of a
  timer / deadline (sets fd.r/w.timeout = 1). All events wake-up all
  goroutines waiting on the fd (cond.Broadcast).

*/

func fdIO(fd *FD, write bool, p []byte) (n int, err error) {
	var fdc *fdCtl
	var dir dirFlag
	var sysc func(int, []byte) (int, error)
	var errEOF error
	var dpre string

	if !write {
		// Prepare things for Read.
		fdc = &fd.r
		dir = dirRead
		sysc = syscall.Read
		errEOF = io.EOF
		if debug_enable {
			dpre = fmt.Sprintf("FD %03d: RD:", fd.id)
		}
	} else {
		// Prepare things for Write.
		fdc = &fd.w
		dir = dirWrite
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
		debugf("%s n=%d, err=%v", dpre, n, err)
		if err != nil {
			n = 0
			if err != syscall.EAGAIN {
				break
			}
			// EAGAIN
			debugf("%s Wait", dpre)
			// NOTE(npat): We may avoid setting/notifying
			// if sysfd is already set (by another
			// groutine doing the same access on the fd).
			sc.Set(fd.sysfd, dir)
			sc.Notify()
			fdc.cond.Wait()
			debugf("%s Wakeup", dpre)
			continue
		}
		if n == 0 && len(p) != 0 {
			// Remote end closed.
			err = errEOF
			break
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
	if fd.r.timer != nil {
		fd.r.timer.Stop()
	}
	if fd.w.timer != nil {
		fd.w.timer.Stop()
	}
	sc.Clear(fd.sysfd, dirRead|dirWrite)
	fdM.DelFD(fd.id)
	err := syscall.Close(fd.sysfd)

	// Wake up everybody waiting on the FD.
	fd.r.cond.Broadcast()
	fd.w.cond.Broadcast()

	debugf("FD %03d: CL: close", fd.id)

	fd.w.cond.L.Unlock()
	fd.r.cond.L.Unlock()
	return err
}

func timerEvent(sysfd int, write bool) {
	var fdc *fdCtl
	var dir dirFlag
	var dpre string

	if debug_enable {
		if !write {
			dpre = fmt.Sprintf("FD %03d: TR:", sysfd)
		} else {
			dpre = fmt.Sprintf("FD %03d: TW:", sysfd)
		}
	}
	// The fd returned by fdM.GetFD may be a new fd (the fd may
	// have been closed, and a new one opened with the same
	// sysfd). It won't harm because checking fd.{r,w}.deadline
	// will determine if this is a valid timeout event.
	fd := fdM.GetFD(sysfd)
	if fd == nil {
		// Drop event. Fd closed and removed from fdM.
		debugf("%s Dropped", dpre)
		return
	}
	if !write {
		// A Read timeout
		fdc = &fd.r
		dir = dirRead
	} else {
		// A Write timeout
		fdc = &fd.w
		dir = dirWrite
	}
	fdc.cond.L.Lock()
	if !fd.closed && !fdc.timeout &&
		!fdc.deadline.IsZero() && !fdc.deadline.After(time.Now()) {
		fdc.timeout = true
		sc.Clear(sysfd, dir)
		// NOTE(npat): Notify select here ??
		fdc.cond.Broadcast()
		debugf("%s Broadcast", dpre)
	} else {
		debugf("%s Ignored", dpre)
	}
	fdc.cond.L.Unlock()
}

var fdM fdMap
var sc selectCtx

func init() {
	fdM.Init(128)
	err := sc.Init()
	if err != nil {
		log.Panicf("poller: Init: %s", err.Error())
	}
	go sc.run()
}

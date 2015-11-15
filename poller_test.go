// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

// +build linux freebsd netbsd openbsd darwin dragonfly solaris

package poller

import (
	"fmt"
	"io"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// !! ATTENTION !! We unconditionally delete files named like this !!
const fifo = "/tmp/poller-test-fifo" // /tmp/poller-test-fifoXXX

func fifoName(i int) string {
	return fifo + fmt.Sprintf("%03d", i)
}

func mkFifo(t *testing.T, i int) {
	name := fifoName(i)
	syscall.Unlink(name)
	err := syscall.Mkfifo(name, 0666)
	if err != nil {
		t.Fatalf("mkfifo %s: %v", name, err)
	}
}

func openFifo(t *testing.T, i int, read bool) *FD {
	name := fifoName(i)
	flags := O_RO
	if !read {
		flags = O_WO
	}
	fd, err := Open(name, flags)
	if err != nil {
		t.Fatalf("Open %s: %v", name, err)
	}
	return fd
}

func waitNTmo_(t *testing.T, ch <-chan error, n int, d time.Duration) {
	tmo := time.After(d)
	for i := 0; i < n; i++ {
		select {
		case err := <-ch:
			if err != nil {
				_, _, line, _ := runtime.Caller(2)
				t.Fatalf("%d: %v", line, err)
			}
		case <-tmo:
			_, _, line, _ := runtime.Caller(2)
			t.Fatalf("%d: Timeout!", line)
		}
	}
}

func waitN(t *testing.T, ch <-chan error, n int) {
	waitNTmo_(t, ch, n, 15*time.Second)
}

func waitNTmo(t *testing.T, ch <-chan error, n int, d time.Duration) {
	waitNTmo_(t, ch, n, d)
}

func readStr(fd *FD, s string) error {
	b := make([]byte, len(s))
	n, err := fd.Read(b)
	if err != nil {
		return fmt.Errorf("Read: %v", err)
	}
	if n != len(s) {
		return fmt.Errorf("Read %d != %d", n, len(s))
	}
	if string(b) != s {
		return fmt.Errorf("Read \"%s\" != \"%s\"", string(b), s)
	}
	return nil
}

func writeStr(fd *FD, s string) error {
	n, err := fd.Write([]byte(s))
	if err != nil {
		return fmt.Errorf("Write: %v", err)
	}
	if n != len(s) {
		return fmt.Errorf("Write %d != %d", n, len(s))
	}
	return nil
}

func readBlock(fd *FD, n, bs int, dly time.Duration) error {
	b := make([]byte, bs)
	for i := 0; i < n; i++ {
		nn := 0
		for {
			n, err := fd.Read(b[nn:])
			if err != nil {
				return fmt.Errorf("readBlock: %v", err)
			}
			nn += n
			if nn == bs {
				break
			}
		}
		if dly != 0 {
			time.Sleep(dly)
		}
	}
	return nil
}

func writeBlock(fd *FD, n, bs int, dly time.Duration) error {
	b := make([]byte, bs)
	for i := 0; i < n; i++ {
		nn, err := fd.Write(b)
		if err != nil {
			return fmt.Errorf("writeBlock: %v", err)
		}
		if nn != bs {
			return fmt.Errorf("writeBlock %d != %d", nn, bs)
		}
		if dly != 0 {
			time.Sleep(dly)
		}
	}
	return nil
}

func TestOpen(t *testing.T) {
	mkFifo(t, 0)
	fdr := openFifo(t, 0, true)
	fdw := openFifo(t, 0, false)
	if fdr == nil {
		t.Fatal("fdr is nil!")
	}
	if fdw == nil {
		t.Fatal("fdw is nil!")
	}
	if fdM.GetFD(fdr.id) != fdr {
		t.Fatal("fdr not in fdMap!")
	}
	if fdM.GetFD(fdw.id) != fdw {
		t.Fatal("fdw not in fdMap!")
	}
	err := fdr.Close()
	if err != nil {
		t.Fatal("Close fdr:", err)
	}
	err = fdw.Close()
	if err != nil {
		t.Fatal("Close fdw:", err)
	}
	if fdM.GetFD(fdr.id) != nil {
		t.Fatal("fdr still in fdMap!")
	}
	if fdM.GetFD(fdw.id) != nil {
		t.Fatal("fdw still in fdMap!")
	}
}

func TestClose(t *testing.T) {
	mkFifo(t, 0)
	fdr := openFifo(t, 0, true)
	fdw := openFifo(t, 0, false)

	clread := func() error {
		b := make([]byte, 4)
		n, err := fdr.Read(b)
		if err != ErrClosed {
			return fmt.Errorf("Read: %v", err)
		}
		if n != 0 {
			return fmt.Errorf("Read n != 0: %d", n)
		}
		return nil
	}

	clwrite := func() error {
		// must fill write buffer
		b := make([]byte, 1024*1024)
		n, err := fdw.Write(b)
		if err != ErrClosed {
			return fmt.Errorf("Write: %v", err)
		}
		if n >= len(b) {
			return fmt.Errorf("Write n >= %d: %d", len(b), n)
		}
		return nil
	}

	end := make(chan error)
	b := make([]byte, 1)

	go func() { end <- clread() }()
	go func() { end <- clread() }()
	go func() { end <- clread() }()
	time.Sleep(100 * time.Millisecond)
	err := fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}
	waitN(t, end, 3)

	err = fdr.Close()
	if err != ErrClosed {
		t.Fatal("Close R:", err)
	}
	_, err = fdr.Read(b)
	if err != ErrClosed {
		t.Fatal("Read R:", err)
	}

	debugf("--------")

	fdr = openFifo(t, 0, true)

	go func() { end <- clwrite() }()
	go func() { end <- clwrite() }()
	go func() { end <- clwrite() }()
	go func() { end <- clwrite() }()
	time.Sleep(100 * time.Millisecond)
	err = fdw.Close()
	if err != nil {
		t.Fatal("Close W:", err)
	}
	waitN(t, end, 4)

	err = fdw.Close()
	if err != ErrClosed {
		t.Fatal("Close W:", err)
	}
	_, err = fdw.Write(b)
	if err != ErrClosed {
		t.Fatal("Write W:", err)
	}

	debugf("--------")

	go func() {
		b := make([]byte, 64*1024)
		for {
			_, err := fdr.Read(b)
			if err != nil {
				if err == io.EOF {
					break
				}
				end <- fmt.Errorf("Read R: %v", err)
				return
			}
		}
		end <- nil
	}()
	waitN(t, end, 1)

	err = fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}
}

func TestCloseWrite(t *testing.T) {
	mkFifo(t, 0)
	fdr := openFifo(t, 0, true)
	fdw := openFifo(t, 0, false)
	_ = fdr

	clwrite := func() error {
		// must fill write buffer
		b := make([]byte, 1024*1024)
		n, err := fdw.Write(b)
		if err != nil {
			if err != syscall.EPIPE {
				return err
			}
		}
		if n >= len(b) {
			return fmt.Errorf("Write n >= %d: %d", len(b), n)
		}
		return nil
	}

	end := make(chan error)

	go func() { end <- clwrite() }()
	go func() { end <- clwrite() }()
	go func() { end <- clwrite() }()
	time.Sleep(10 * time.Millisecond)
	err := fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}

	waitN(t, end, 3)
}

func TestRead(t *testing.T) {
	mkFifo(t, 0)
	fdr := openFifo(t, 0, true)
	fdw := openFifo(t, 0, false)

	end := make(chan error)

	go func() { end <- readStr(fdr, "0123") }()
	go func() { end <- readStr(fdr, "0123") }()
	time.Sleep(100 * time.Millisecond)
	go func() { end <- writeStr(fdw, "01230123") }()
	waitN(t, end, 3)

	debugf("--------")

	go func() { end <- readStr(fdr, "0123") }()
	go func() { end <- readStr(fdr, "0123") }()
	go func() { end <- readStr(fdr, "0123") }()
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := writeStr(fdw, "0123"); err != nil {
			end <- err
			return
		}
		time.Sleep(100 * time.Millisecond)
		if err := writeStr(fdw, "01230123"); err != nil {
			end <- err
			return
		}
		end <- nil
	}()
	waitN(t, end, 4)

	err := fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}

	err = fdw.Close()
	if err != nil {
		t.Fatal("Close W:", err)
	}
}

func TestWrite(t *testing.T) {
	mkFifo(t, 0)
	fdr := openFifo(t, 0, true)
	fdw := openFifo(t, 0, false)

	end := make(chan error)

	go func() { end <- writeBlock(fdw, 1, 512*1024, 0) }()
	go func() {
		// Give writer some headstart
		time.Sleep(100 * time.Millisecond)
		end <- readBlock(fdr, 512, 1024, 0)
	}()
	waitN(t, end, 2)

	debugf("--------")

	for i := 0; i < 4; i++ {
		go func() {
			end <- writeBlock(fdw, 128, 1024,
				10*time.Millisecond)
		}()
	}
	go func() {
		// Give writers some headstart
		time.Sleep(100 * time.Millisecond)
		end <- readBlock(fdr, 1, 512*1024, 0)
	}()
	waitN(t, end, 4)

	debugf("--------")

	for i := 0; i < 4; i++ {
		go func() {
			end <- writeBlock(fdw, 128, 1024,
				10*time.Millisecond)
		}()
	}
	for i := 0; i < 4; i++ {
		go func() {
			end <- readBlock(fdr, 256, 512, 0)
		}()
	}
	waitN(t, end, 8)

	err := fdw.Close()
	if err != nil {
		t.Fatal("Close W:", err)
	}
	err = fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}
}

func TestDeadlines(t *testing.T) {
	mkFifo(t, 0)
	fdr := openFifo(t, 0, true)
	fdw := openFifo(t, 0, false)

	end := make(chan error)

	err := fdr.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	if err != nil {
		t.Fatal("SetReadDeadline:", err)
	}
	go func() {
		b := make([]byte, 1)
		_, err = fdr.Read(b)
		if err != ErrTimeout {
			end <- fmt.Errorf("Read: %v", err)
			return
		}
		_, err = fdr.Read(b)
		if err != ErrTimeout {
			end <- fmt.Errorf("Read: %v", err)
			return
		}
		end <- nil
	}()
	waitNTmo(t, end, 1, 200*time.Millisecond)

	_, err = fdw.Write([]byte("0123"))
	if err != nil {
		t.Fatal("Write:", err)
	}
	err = fdr.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	if err != nil {
		t.Fatal("SetReadDeadline:", err)
	}
	time.Sleep(2 * time.Millisecond)
	go func() {
		b := make([]byte, 1)
		_, err = fdr.Read(b)
		if err != ErrTimeout {
			end <- fmt.Errorf("Read: %v", err)
			return
		}
		end <- nil
	}()
	waitNTmo(t, end, 1, 200*time.Millisecond)

	err = fdr.SetDeadline(time.Now().Add(100 * time.Millisecond))
	if err != nil {
		t.Fatal("SetDeadline:", err)
	}
	go func() {
		b := make([]byte, 4)
		_, err = fdr.Read(b)
		if err != nil {
			end <- fmt.Errorf("Read: %v", err)
			return
		}
		end <- nil
	}()
	waitNTmo(t, end, 1, 200*time.Millisecond)
	err = fdr.SetDeadline(time.Time{})
	if err != nil {
		t.Fatal("SetDeadline:", err)
	}

	err = fdw.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
	if err != nil {
		t.Fatal("SetWriteDeadline:", err)
	}
	go func() {
		b := make([]byte, 4096)
		var err error
		for {
			_, err = fdw.Write(b)
			if err != nil {
				break
			}
		}
		if err != ErrTimeout {
			end <- fmt.Errorf("Write: %v", err)
			return
		}
		end <- nil
	}()
	waitNTmo(t, end, 1, 500*time.Millisecond)

	err = fdw.Close()
	if err != nil {
		t.Fatal("Close W:", err)
	}
	err = fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}
}

// TestXBlock tests if a blocking misc call for an FD (fd1r---keeping
// the FD locked for the duration), blocks access to another FD
// (fd0r). With v1.0.0 it should, starting with v1.1.0, it shouldn't
func TestXBlock(t *testing.T) {
	mkFifo(t, 0)
	fd0r := openFifo(t, 0, true)
	fd0w := openFifo(t, 0, false)

	mkFifo(t, 1)
	fd1r := openFifo(t, 1, true)
	fd1w := openFifo(t, 1, false)

	fd0c := make(chan error)
	fd1c := make(chan error)

	go func() {
		fd0r.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		debugf("FD %d: Reading", fd0r.id)
		fd0c <- readStr(fd0r, "Hello")
	}()

	go func() {
		if err := fd1r.Lock(); err != nil {
			fd1c <- err
			return
		}
		debugf("FD %d: Locked", fd1r.id)
		time.Sleep(200 * time.Millisecond)
		debugf("FD %d: About to unlock", fd1r.id)
		fd1r.Unlock()
		fd1c <- nil
	}()

	// Wait for fd1r to get locked.
	time.Sleep(10 * time.Millisecond)
	// This will generate an ER (read event) on fd1r.
	writeStr(fd1w, "Block")
	// Wait for event on fd1r to be received from epoll
	time.Sleep(10 * time.Millisecond)

	// This will generate an ER on fd0r.
	writeStr(fd0w, "Hello")

	// In v1.0.0, code that delivers events, tries to grab the
	// same lock as misc operations (fd.Lock()) do, and epoll
	// event delivery is serialized. So the ER on fd1r will wait
	// for fd1r to be unlocked before it is delivered, and the ER
	// on fd0r will wait for the ER on fd1r. In the mean time the
	// deadline on fd0r will expire.

	// This should normally succeed.
	waitN(t, fd1c, 1)
	// This should fail with ErrTimeout if FD's do cross-block.
	waitN(t, fd0c, 1)

	clo := func(f *FD, s string) {
		err := f.Close()
		if err != nil {
			t.Fatalf("Close %s: %v", s, err)
		}
	}
	clo(fd0w, "fd0w")
	clo(fd0r, "fd0r")
	clo(fd1w, "fd1w")
	clo(fd1r, "fd1r")
}

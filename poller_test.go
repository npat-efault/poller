package poller

import (
	"fmt"
	"io"
	"syscall"
	"testing"
	"time"
)

// !! ATTENTION !! We unconditionally delete files named like this !!
const fifo = "/tmp/poller-test-fifo" // /tmp/poler-test-fifoXXX

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

func waitNTmo(t *testing.T, ch <-chan bool, n int, d time.Duration) {
	tmo := time.After(d)
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-tmo:
			t.Fatal("Timeout!")
		}
	}
}

func waitN(t *testing.T, ch <-chan bool, n int) {
	waitNTmo(t, ch, n, 15*time.Second)
}

func readStr(t *testing.T, fd *FD, s string) {
	b := make([]byte, len(s))
	n, err := fd.Read(b)
	if err != nil {
		t.Fatal("Read:", err)
	}
	if n != len(s) {
		t.Fatalf("Read %d != %d", n, len(s))
	}
	if string(b) != s {
		t.Fatalf("Read \"%s\" != \"%s\"", string(b), s)
	}
}

func writeStr(t *testing.T, fd *FD, s string) {
	n, err := fd.Write([]byte(s))
	if err != nil {
		t.Fatal("Write:", err)
	}
	if n != len(s) {
		t.Fatalf("Write %d != %d", n, len(s))
	}
}

func readBlock(t *testing.T, fd *FD, n, bs int, dly time.Duration) {
	b := make([]byte, bs)
	for i := 0; i < n; i++ {
		nn := 0
		for {
			n, err := fd.Read(b[nn:])
			if err != nil {
				t.Fatal("readBlock:", err)
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
}

func writeBlock(t *testing.T, fd *FD, n, bs int, dly time.Duration) {
	b := make([]byte, bs)
	for i := 0; i < n; i++ {
		nn, err := fd.Write(b)
		if err != nil {
			t.Fatal("writeBlock:", err)
		}
		if nn != bs {
			t.Fatalf("writeBlock %d != %d", nn, bs)
		}
		if dly != 0 {
			time.Sleep(dly)
		}
	}
}

func TestNewFDFail(t *testing.T) {
	sysfd, err := syscall.Open("/dev/null", O_RW, 0666)
	if err != nil {
		t.Fatal("Open /dev/null:", err)
	}
	fd, err := NewFD(sysfd)
	if err != syscall.EPERM {
		t.Fatal("NewFD:", err)
	}
	if fd != nil {
		t.Fatal("fd != nil!")
	}
	err = syscall.Close(sysfd)
	if err != nil {
		t.Fatal("Close /dev/null:", err)
	}
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

	clread := func() {
		b := make([]byte, 4)
		n, err := fdr.Read(b)
		if err != ErrClosed {
			t.Fatal("Read:", err)
		}
		if n != 0 {
			t.Fatal("Read n != 0:", n)
		}
	}

	clwrite := func() {
		// must fill write buffer
		b := make([]byte, 1024*1024)
		n, err := fdw.Write(b)
		if err != ErrClosed {
			t.Fatal("Write:", err)
		}
		if n >= len(b) {
			t.Fatalf("Write n >= %d: %d", len(b), n)
		}
	}

	end := make(chan bool)
	b := make([]byte, 1)

	go func() { clread(); end <- true }()
	go func() { clread(); end <- true }()
	go func() { clread(); end <- true }()
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

	debug("--------")

	fdr = openFifo(t, 0, true)

	go func() { clwrite(); end <- true }()
	go func() { clwrite(); end <- true }()
	go func() { clwrite(); end <- true }()
	go func() { clwrite(); end <- true }()
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

	debug("--------")

	go func() {
		b := make([]byte, 64*1024)
		for {
			_, err := fdr.Read(b)
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatal("Read R:", err)
			}
		}
		end <- true
	}()
	waitN(t, end, 1)

	err = fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}
}

func TestRead(t *testing.T) {
	mkFifo(t, 0)
	fdr := openFifo(t, 0, true)
	fdw := openFifo(t, 0, false)

	end := make(chan bool)

	go func() { readStr(t, fdr, "0123"); end <- true }()
	go func() { readStr(t, fdr, "0123"); end <- true }()
	time.Sleep(100 * time.Millisecond)
	go func() { writeStr(t, fdw, "01230123"); end <- true }()
	waitN(t, end, 3)

	debug("--------")

	go func() { readStr(t, fdr, "0123"); end <- true }()
	go func() { readStr(t, fdr, "0123"); end <- true }()
	go func() { readStr(t, fdr, "0123"); end <- true }()
	go func() {
		time.Sleep(100 * time.Millisecond)
		writeStr(t, fdw, "0123")
		time.Sleep(100 * time.Millisecond)
		writeStr(t, fdw, "01230123")
		end <- true
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

	end := make(chan bool)

	go func() {
		writeBlock(t, fdw, 1, 512*1024, 0)
		end <- true
	}()
	go func() {
		// Give writer some headstart
		time.Sleep(100 * time.Millisecond)
		readBlock(t, fdr, 512, 1024, 0)
		end <- true
	}()
	waitN(t, end, 2)

	debug("--------")

	for i := 0; i < 4; i++ {
		go func() {
			writeBlock(t, fdw, 128, 1024, 10*time.Millisecond)
			end <- true
		}()
	}
	go func() {
		// Give writers some headstart
		time.Sleep(100 * time.Millisecond)
		readBlock(t, fdr, 1, 512*1024, 0)
		end <- true
	}()
	waitN(t, end, 4)

	debug("--------")

	for i := 0; i < 4; i++ {
		go func() {
			writeBlock(t, fdw, 128, 1024, 10*time.Millisecond)
			end <- true
		}()
	}
	for i := 0; i < 4; i++ {
		go func() {
			readBlock(t, fdr, 256, 512, 0)
			end <- true
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

	_ = fdw
	end := make(chan bool)

	err := fdr.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	if err != nil {
		t.Fatal("SetReadDeadline:", err)
	}
	go func() {
		b := make([]byte, 1)
		_, err = fdr.Read(b)
		if err != ErrTimeout {
			t.Fatal("Read:", err)
		}
		_, err = fdr.Read(b)
		if err != ErrTimeout {
			t.Fatal("Read:", err)
		}
		end <- true
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
			t.Fatal("Read:", err)
		}
		end <- true
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
			t.Fatal("Read:", err)
		}
		end <- true
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
			t.Fatal("Write:", err)
		}
		end <- true
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

package poller

import (
	"syscall"
	"testing"
	"time"
)

// !! ATTENTION !! We unconditionally delete this file !!
const fifo = "/tmp/poller-test-fifo"

const (
	o_RW = (syscall.O_NOCTTY |
		syscall.O_CLOEXEC |
		syscall.O_RDWR |
		syscall.O_NONBLOCK) // Open file for read and write
	o_RO = (syscall.O_NOCTTY |
		syscall.O_CLOEXEC |
		syscall.O_RDONLY |
		syscall.O_NONBLOCK) // Open file for read
	o_WO = (syscall.O_NOCTTY |
		syscall.O_CLOEXEC |
		syscall.O_WRONLY |
		syscall.O_NONBLOCK) // Open file for write
	o_MODE = 0666
)

func mkfifo(t *testing.T) {
	_ = syscall.Unlink(fifo)
	err := syscall.Mkfifo(fifo, 0666)
	if err != nil {
		t.Fatal("mkfifo:", err)
	}
}

func TestRead(t *testing.T) {
	sysfd, err := syscall.Open("/dev/null", o_RW, o_MODE)
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

	mkfifo(t)

	sysfd_r, err := syscall.Open(fifo, o_RO, o_MODE)
	if err != nil {
		t.Fatal("Open fifo R:", err)
	}
	sysfd_w, err := syscall.Open(fifo, o_WO, o_MODE)
	if err != nil {
		t.Fatal("Open fifo W:", err)
	}

	fdr, err := NewFD(sysfd_r)
	if err != nil {
		t.Fatal("NewFD fifo R:", err)
	}
	fdw, err := NewFD(sysfd_w)
	if err != nil {
		t.Fatal("NewFD fifo W:", err)
	}

	read := func(s string) {
		b := make([]byte, len(s))
		n, err := fdr.Read(b)
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

	write := func(s string) {
		n, err := fdw.Write([]byte(s))
		if err != nil {
			t.Fatal("Write:", err)
		}
		if n != len(s) {
			t.Fatalf("Write %d != %d", n, len(s))
		}
	}

	end := make(chan bool)

	go func() { read("0123"); end <- true }()
	go func() { read("0123"); end <- true }()
	time.Sleep(100 * time.Millisecond)
	go func() { write("01230123"); end <- true }()
	<-end
	<-end
	<-end

	go func() { read("0123"); end <- true }()
	go func() { read("0123"); end <- true }()
	go func() {
		time.Sleep(100 * time.Millisecond)
		write("0123")
		time.Sleep(100 * time.Millisecond)
		write("0123")
		end <- true
	}()
	<-end
	<-end
	<-end

	slamed := func() {
		b := make([]byte, 4)
		n, err := fdr.Read(b)
		if err != ErrClosed {
			t.Fatal("Read:", err)
		}
		if n != 0 {
			t.Fatal("Read n != 0:", n)
		}
	}

	go func() { slamed(); end <- true }()
	go func() { slamed(); end <- true }()
	go func() { slamed(); end <- true }()
	time.Sleep(100 * time.Millisecond)
	err = fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}
	<-end
	<-end
	<-end

	err = fdr.Close()
	if err != ErrClosed {
		t.Fatal("Close R:", err)
	}

	err = fdw.Close()
	if err != nil {
		t.Fatal("Close W:", err)
	}
}

type ioFunc func([]byte) (int, error)

func blocker(t *testing.T, io ioFunc, n, bs int, dly time.Duration) {
	b := make([]byte, bs)
	for i := 0; i < n; i++ {
		nn := 0
		for {
			n, err := io(b[nn:])
			if err != nil {
				t.Fatal("Blocker:", err)
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

func TestWrite(t *testing.T) {
	mkfifo(t)

	sysfd_r, err := syscall.Open(fifo, o_RO, o_MODE)
	if err != nil {
		t.Fatal("Open fifo R:", err)
	}
	sysfd_w, err := syscall.Open(fifo, o_WO, o_MODE)
	if err != nil {
		t.Fatal("Open fifo W:", err)
	}

	fdr, err := NewFD(sysfd_r)
	if err != nil {
		t.Fatal("NewFD fifo R:", err)
	}
	fdw, err := NewFD(sysfd_w)
	if err != nil {
		t.Fatal("NewFD fifo W:", err)
	}

	end := make(chan bool)

	go func() {
		blocker(t, fdw.Write, 20, 4096, 0)
		end <- true
	}()

	go func() {
		// Give writer headstart
		time.Sleep(1 * time.Second)
		blocker(t, fdr.Read, 40, 2048, 100*time.Millisecond)
		end <- true
	}()

	tmo := time.After(15 * time.Second)
	select {
	case <-end:
	case <-tmo:
		t.Fatal("Timeout!")
	}
	select {
	case <-end:
	case <-tmo:
		t.Fatal("Timeout!")
	}

	go func() {
		blocker(t, fdw.Write, 1000, 10240, 0)
		end <- true
	}()

	go func() {
		blocker(t, fdr.Read, 5000, 2048, 0)
		end <- true
	}()

	tmo = time.After(15 * time.Second)
	select {
	case <-end:
	case <-tmo:
		t.Fatal("Timeout!")
	}
	select {
	case <-end:
	case <-tmo:
		t.Fatal("Timeout!")
	}

	err = fdw.Close()
	if err != nil {
		t.Fatal("Close W:", err)
	}
	err = fdr.Close()
	if err != nil {
		t.Fatal("Close R:", err)
	}
}

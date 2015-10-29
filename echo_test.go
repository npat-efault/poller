// Copyright (c) 2014, Nick Patavalis (npat@efault.net).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE.txt file.

/*

Create nPairs pairs of goroutines, and nPairs * 2 fifos. In every
goroutine pair, one goroutine is the Echoer and the other is the
Sender. They are connected by fifos like this:

  [ echoer ] <-fdr[2*i]------ fifo 2*i --------fdw[2*i]-- [ sender ]
  [   #i   ] --fdw[2*i+1]---- fifo 2*i+1 ----fdr[2*i+1]-> [   #i   ]

The Senders send messages (nMsg each) to the respective Echoers. The
Echoers echo the received messages back to the senders. The senders
check if the echoed-back messages are identical to the originals.

*/

package poller_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/npat-efault/poller"
)

const (
	nPairs = 8   // Number of Sender / Echoer pairs
	nMsg   = 100 // Number of messages sent to each Echoer
)

// !! ATTENTION !! We unconditionally delete files named like this !!
const fifo = "/tmp/poller-test-fifo" // /tmp/poller-test-fifoXXX

func packUInt32BE(p []byte, i uint32) []byte {
	return append(p,
		byte(i>>24),
		byte(i>>16),
		byte(i>>8),
		byte(i))
}

func packUInt16BE(p []byte, i uint16) []byte {
	return append(p,
		byte(i>>8),
		byte(i))
}

func unpackUInt32BE(p []byte) (uint32, []byte) {
	i := uint32(p[0])<<24 |
		uint32(p[1])<<16 |
		uint32(p[2])<<8 |
		uint32(p[3])
	return i, p[4:]
}

func unpackUInt16BE(p []byte) (uint16, []byte) {
	i := uint16(p[0])<<8 |
		uint16(p[1])
	return i, p[2:]
}

func packRandData(p []byte, n int) []byte {
	// This is *very* slow due to the repetitive locking /
	// unlocking of the common random source.
	for i := 0; i < n; i++ {
		p = append(p, byte(rand.Intn(0x100)))
	}
	return p
}

// Message (msg):
//
// +--+--+--+--+--+--+--+-- .... ... +--+
// | len |    seq    |       data       |
// +--+--+--+--+--+--+--+-- .... ... +--+
// |                 |                  |
// |<--headSz (6)--->|<--len - headSz-->|

const (
	lenSz  = 2
	seqSz  = 4
	headSz = lenSz + seqSz
)

type msg []byte

func randMsg(seq uint32) msg {
	n := rand.Intn(0x10000 - headSz)
	l := headSz + n
	m := make(msg, 0, l)
	m = packUInt16BE(m, uint16(l))
	m = packUInt32BE(m, seq)
	m = packRandData(m, n)
	return m
}

func (m msg) Len() int {
	n, _ := unpackUInt16BE(m)
	return int(n)
}

func (m msg) Seq() uint32 {
	sq, _ := unpackUInt32BE(m[lenSz:])
	return sq
}

func (m msg) Data() []byte {
	return m[headSz:]
}

func (m msg) Send(w io.Writer) error {
	_, err := w.Write(m)
	return err
}

func rcvMsg(r io.Reader) (msg, error) {
	m0 := make(msg, lenSz)
	_, err := io.ReadFull(r, m0)
	if err != nil {
		return nil, err
	}
	l := m0.Len()
	m := make(msg, l)
	copy(m, m0)
	_, err = io.ReadFull(r, m[lenSz:])
	if err != nil {
		return nil, err
	}
	return m, nil
}

func sendN(r, w *poller.FD, n int) error {
	var err error
	for i := 0; i < n; i++ {
		var ms, mr msg
		ms = randMsg(uint32(i))
		err = w.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			err = fmt.Errorf("set W dl #%d: %v", i, err)
			break
		}
		err = ms.Send(w)
		if err != nil {
			err = fmt.Errorf("cannot send #%d: %v", i, err)
			break
		}
		err = r.SetReadDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			err = fmt.Errorf("set R dl #%d: %v", i, err)
			break
		}
		mr, err = rcvMsg(r)
		if err != nil {
			err = fmt.Errorf("cannot rcv #%d: %v", i, err)
			break
		}
		if mr.Seq() != uint32(i) {
			err = fmt.Errorf("rcv seq %d != %d", mr.Seq(), i)
			break
		}
		if !bytes.Equal(ms.Data(), mr.Data()) {
			err = fmt.Errorf("data not equal for #%d", i)
			break
		}
	}
	return err
}

func echoN(r, w *poller.FD, n int) error {
	var err error
	for i := 0; i < n; i++ {
		var m msg
		err = r.SetReadDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			err = fmt.Errorf("set R dl #%d: %v", i, err)
			break
		}
		m, err = rcvMsg(r)
		if err != nil {
			err = fmt.Errorf("cannot rcv #%d: %v", i, err)
			break
		}
		if m.Seq() != uint32(i) {
			err = fmt.Errorf("rcv seq %d != %d", m.Seq(), i)
			break
		}
		err = w.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			err = fmt.Errorf("set W dl #%d: %v", i, err)
			break
		}
		err = m.Send(w)
		if err != nil {
			err = fmt.Errorf("cannot send #%d: %v", i, err)
			break
		}
	}
	return err
}

func fifoName(i int) string {
	return fifo + fmt.Sprintf("%03d", i)
}

func mkFifo(t *testing.T, i int) {
	name := fifoName(i)
	syscall.Unlink(name)
	err := syscall.Mkfifo(name, 0666)
	if err != nil {
		t.Fatalf("mkFifo %s: %v", name, err)
	}
}

func rmFifo(t *testing.T, i int) {
	name := fifoName(i)
	err := syscall.Unlink(name)
	if err != nil {
		t.Fatalf("rmFifo %s: %v", name, err)
	}
}

func openFifo(t *testing.T, i int, read bool) *poller.FD {
	name := fifoName(i)
	flags := poller.O_RO
	if !read {
		flags = poller.O_WO
	}
	fd, err := poller.Open(name, flags)
	if err != nil {
		t.Fatalf("Open %s: %v", name, err)
	}
	return fd
}

func waitNTmo(t *testing.T, ch <-chan error, n int, d time.Duration) {
	tmo := time.After(d)
	for i := 0; i < n; i++ {
		select {
		case err := <-ch:
			if err != nil {
				_, _, line, _ := runtime.Caller(1)
				t.Fatalf("%d: %v", line, err)
			}
		case <-tmo:
			_, _, line, _ := runtime.Caller(1)
			t.Fatalf("%d: Timeout!", line)
		}
	}
}

func TestEcho(t *testing.T) {
	fdr := make([]*poller.FD, nPairs*2)
	fdw := make([]*poller.FD, nPairs*2)
	for i := 0; i < nPairs; i++ {
		// even fifos: echoers read, senders write
		// odd fifos: echoes write, senders read
		mkFifo(t, 2*i)
		mkFifo(t, 2*i+1)
		fdr[2*i] = openFifo(t, 2*i, true)
		fdw[2*i] = openFifo(t, 2*i, false)
		fdr[2*i+1] = openFifo(t, 2*i+1, true)
		fdw[2*i+1] = openFifo(t, 2*i+1, false)
	}

	ch := make(chan error)

	for i := 0; i < nPairs; i++ {
		go func(i int) {
			err := echoN(fdr[2*i], fdw[2*i+1], nMsg)
			if err != nil {
				err = fmt.Errorf("Echoer %d: %v", i, err)
			}
			ch <- err
		}(i)
		go func(i int) {
			err := sendN(fdr[2*i+1], fdw[2*i], nMsg)
			if err != nil {
				err = fmt.Errorf("Sender %d: %v", i, err)
			}
			ch <- err
		}(i)
	}
	waitNTmo(t, ch, nPairs*2, 60*time.Second)

	for i := 0; i < nPairs; i++ {
		if err := fdr[2*i].Close(); err != nil {
			t.Fatalf("Failed to close fdr[%d]: %v", 2*i, err)
		}
		if err := fdw[2*i].Close(); err != nil {
			t.Fatalf("Failed to close fdw[%d]: %v", 2*i, err)
		}
		if err := fdr[2*i+1].Close(); err != nil {
			t.Fatalf("Failed to close fdr[%d]: %v", 2*i+1, err)
		}
		if err := fdw[2*i+1].Close(); err != nil {
			t.Fatalf("Failed to close fdw[%d]: %v", 2*i+1, err)
		}
		rmFifo(t, 2*i)
		rmFifo(t, 2*i+1)
	}
}

package client

import (
	"bytes"
	"errors"
)

const defaultScratchSize = 64 * 1024

var errBufTooSmall = errors.New("buffer is too small to fit a single message")

// client represents an instance of client connected to a set of chukcha servers
type Simple struct {
	addr []string

	buf     bytes.Buffer
	restBuf bytes.Buffer
}

// NewSimple creares a new client for the chukcha server
func NewSimple(addrs []string) *Simple {
	return &Simple{
		addr: addrs,
		buf:  bytes.Buffer{},
	}
}

// send sends the message to the chukcha server
func (s *Simple) Send(msgs []byte) error {
	_, err := s.buf.Write(msgs)
	return err
}

// Receive will either wait for a new messages or return an
// error in case something wrong.
// The scratch buffer can be used to read the data
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)

	}

	// scratchPtr := scratch
	startOff := 0

	if s.restBuf.Len() > 0 {
		if s.restBuf.Len() >= len(scratch) {
			return nil, errBufTooSmall
		}
		n, err := s.restBuf.Read(scratch)
		if err != nil {
			return nil, err
		}
		s.restBuf.Reset()
		startOff += n
	}

	n, err := s.buf.Read(scratch[startOff:])
	if err != nil {
		return nil, err
	}
	truncated, rest, err := cutToLastMessage(scratch[0 : n+startOff])
	if err != nil {
		return nil, err
	}
	s.restBuf.Reset()
	s.restBuf.Write(rest)
	return truncated, nil
}

func cutToLastMessage(res []byte) (truncated []byte, rest []byte, err error) {
	n := len(res)
	if n == 0 {
		return res, nil, nil
	}

	if res[n-1] == '\n' {
		return res, nil, nil
	}

	lastPros := bytes.LastIndexByte(res, '\n')
	if lastPros < 0 {
		return nil, nil, errBufTooSmall
	}
	return res[0 : lastPros+1], res[lastPros+1:], nil
}

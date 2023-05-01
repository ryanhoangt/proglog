package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8 // 8 bytes for record size
)

type store struct {
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size, // in case file existed
		buf: bufio.NewWriter(f),
	}, nil
}


func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// write binary-representation of len(p) to buf writer
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// write p to buf writer
	bw, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// update s.size
	bw += lenWidth
	s.size += uint64(bw)
	return uint64(bw), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush buf writer before reading
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	
	// read record size
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// read record data
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos + lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// implement io.ReaderAt
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// implement io.Closer
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
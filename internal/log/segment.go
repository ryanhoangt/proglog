package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/edohoangt/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// an abstraction that ties a store and an index together
type segment struct {
	store                  *store
	index                  *index
	config                 Config
	baseOffset, nextOffset uint64 // absolute offsets
}

// create a new segment for records with offsets after 'baseOffset'
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), 
		os.O_RDWR | os.O_CREATE | os.O_APPEND, 
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR | os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	
	// set nextOffset field
	if off, _, err := s.index.Read(-1); err != nil { // empty file
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	curOff := s.nextOffset
	record.Offset = curOff // absolute offset

	msgBytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(msgBytes)
	if err != nil {
		return 0, err
	}
	if err := s.index.Write(
		uint32(s.nextOffset - uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++
	return curOff, nil
}

// read the record from store and index given its absolute offset
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	msgBytes, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(msgBytes, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// close the segment and remove the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// close the segment i.e. the store and index. It alsos clear the file-mapped memory region in the process
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// return the nearest and lesser (or equal) multiple of k in j
func nearestMultiple(j, k uint64) uint64 {
	return (j / k) * k

	// if j >= 0 {
	// 	return (j / k) * k
	// }
	// return ((j - k + 1) / k) * k
}


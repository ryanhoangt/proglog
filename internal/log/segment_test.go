package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/edohoangt/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	rec := &api.Record{Value: []byte("hello world")}
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(rec)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		recRead, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, rec.Value, recRead.Value)
	}

	_, err = s.Append(rec)
	require.Equal(t, io.EOF, err)
	// check maxed index
	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(rec.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s.Close()
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	// check maxed store
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
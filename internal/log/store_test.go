package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testPayload = []byte("hello world")
	recordWidth = uint64(len(testPayload)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s) // test state recovery
}

func testAppend(t *testing.T, s *store) {
	t.Helper() // silence helper in error report

	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(testPayload)
		require.NoError(t, err)
		require.Equal(t, recordWidth*i, pos+n)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()

	var pos uint64 = 0
	for i := uint64(1); i < 4; i++ {
		rec, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, testPayload, rec)
		pos += recordWidth	
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n) // assert num of bytes read
		
		off += int64(n)
		recSize := enc.Uint64(b)
		b = make([]byte, recSize)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, testPayload, b)
		require.Equal(t, int(recSize), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(testPayload)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

// open the file, return file with its size
func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name, 
		os.O_RDWR | os.O_CREATE | os.O_APPEND, 
		0644,
	)
	if err != nil {
		return nil, 0, err 
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
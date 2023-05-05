package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "github.com/edohoangt/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	test_table := map[string]func(t *testing.T, log *Log) {
		"append and read a record succeeds": testAppendRead,
		"offset out of range error": testOutOfRangeErr,
		"init with existing segments": testInitExisting,
		"reader": testReader,
		"truncate": testTruncate,
	}

	for scenario, fn := range test_table {
		t.Run(scenario, func(t *testing.T) {
			// setup before-each
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			// call test fn
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	readRec, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, rec.Value, readRec.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	readRec, err := log.Read(1)
	require.Nil(t, readRec)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	
	for i := 0; i < 3; i++ {
		_, err := log.Append(rec)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
	
	log1, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)
	
	off, err = log1.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = log1.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	readRec := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], readRec)
	require.NoError(t, err)
	require.Equal(t, rec.Value, readRec.Value)
}

func testTruncate(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(rec)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)
	
	_, err = log.Read(0)
	require.Error(t, err)
}
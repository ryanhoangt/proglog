package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	} {
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 16},
	}
	
	for _, ent := range entries {
		err = idx.Write(ent.Off, ent.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(ent.Off))
		require.NoError(t, err)
		require.Equal(t, ent.Pos, pos)
	}

	// ensure error is thrown when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	err = idx.Close()
	require.NoError(t, err)
	// index should rebuild its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	fmt.Printf("off: %d, pos: %d\n", off, pos)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
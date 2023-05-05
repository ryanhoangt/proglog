package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/edohoangt/proglog/api/v1"
	"github.com/edohoangt/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	test_table := map[string]func (t *testing.T, client api.LogClient, config *Config) {
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds": testProduceConsumeStream,
		"consume past log boundary fails": testConsumePastBoundary,
	}

	for scenario, fn := range test_table {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// setup client
	l, err := net.Listen("tcp", ":0") // assign an arbitrary free port
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client = api.NewLogClient(cc)
	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	produceRes, err := client.Produce(ctx, &api.ProduceRequest{
		Record: rec,
	})
	require.NoError(t, err)
	
	consumeRes, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produceRes.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, rec.Value, consumeRes.Record.Value)
	require.Equal(t, rec.Offset, consumeRes.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	produceRes, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consumeRes, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produceRes.Offset + 1,
	})
	if consumeRes != nil {
		t.Fatal("consume not nil")
	}
	codeGot := grpc.Code(err)
	codeExpect := grpc.Code(api.ErrOffsetOutOfRange{})
	if codeGot != codeExpect {
		t.Fatalf("got err: %v, expect: %v", codeGot, codeExpect)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{
			Value: []byte("first message"),
			Offset: 0,
		}, 
		{
			Value: []byte("second message"),
			Offset: 1,
		},
	}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			res, err := stream.Recv() // bidirection
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, expect: %d", res.Offset, offset)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
			Offset: 0,
		})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value: record.Value,
				Offset: uint64(i),
			})
		}
	}
}

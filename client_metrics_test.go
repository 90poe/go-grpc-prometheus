package grpc_prometheus

import (
	"context"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestClientMetricsRaces(t *testing.T) {
	m := NewClientMetrics()
	tests := []struct {
		desc string
		f    func(t *testing.T)
	}{
		{
			desc: "EnableClientHandlingTimeHistogram",
			f: func(t *testing.T) {
				m.EnableClientHandlingTimeHistogram()
			},
		},
		{
			desc: "EnableClientStreamReceiveTimeHistogram",
			f: func(t *testing.T) {
				m.EnableClientStreamReceiveTimeHistogram()
			},
		},
		{
			desc: "EnableClientStreamSendTimeHistogram",
			f: func(t *testing.T) {
				m.EnableClientStreamSendTimeHistogram()
			},
		},
		{
			desc: "Describe",
			f: func(t *testing.T) {
				descriptor := make(chan *prom.Desc)
				m.Describe(descriptor)
				go func() {
					for range descriptor {
					}
				}()
			},
		},
		{
			desc: "Collect",
			f: func(t *testing.T) {
				descriptor := make(chan prom.Metric)
				m.Collect(descriptor)
				go func() {
					for range descriptor {
					}
				}()
			},
		},
		{
			desc: "UnaryClientInterceptor",
			f: func(t *testing.T) {
				var invoker grpc.UnaryInvoker = func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					return nil
				}
				err := m.UnaryClientInterceptor()(context.Background(), "SomeMethod", 1, 2, nil, invoker)
				require.NoError(t, err)
			},
		},
		{
			desc: "StreamClientInterceptor",
			f: func(t *testing.T) {
				var invoker grpc.Streamer = func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					return nil, nil
				}
				desc := &grpc.StreamDesc{
					StreamName: "SomeStream",
					Handler: func(srv interface{}, stream grpc.ServerStream) error {
						return nil
					},
					ServerStreams: true,
					ClientStreams: true,
				}
				_, err := m.StreamClientInterceptor()(context.Background(), desc, nil, "MethodName", invoker)
				require.NoError(t, err)
			},
		},
	}
	wait := make(chan struct{}, len(tests))
	ch := make(chan struct{})
	const (
		goroutines = 50
		retries    = 50
	)
	for i := 0; i < goroutines; i++ {
		go func() {
			wait <- struct{}{}
			<-ch
			for i := 0; i < retries; i++ {
				for _, test := range tests {
					test.f(t)
				}
			}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-wait
	}
	close(ch)
}

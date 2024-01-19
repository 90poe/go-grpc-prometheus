package grpc_prometheus

import (
	"context"
	"io"
	"sync"

	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClientMetrics represents a collection of metrics to be registered on a
// Prometheus metrics registry for a gRPC client.
type ClientMetrics struct {
	lock sync.RWMutex

	clientStartedCounter    *prom.CounterVec
	clientHandledCounter    *prom.CounterVec
	clientStreamMsgReceived *prom.CounterVec
	clientStreamMsgSent     *prom.CounterVec

	clientHandledHistogramEnabled bool
	clientHandledHistogramOpts    prom.HistogramOpts
	clientHandledHistogram        *prom.HistogramVec

	clientStreamRecvHistogramEnabled bool
	clientStreamRecvHistogramOpts    prom.HistogramOpts
	clientStreamRecvHistogram        *prom.HistogramVec

	clientStreamSendHistogramEnabled bool
	clientStreamSendHistogramOpts    prom.HistogramOpts
	clientStreamSendHistogram        *prom.HistogramVec
}

func (m *ClientMetrics) safeClone() *ClientMetrics {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return &ClientMetrics{
		clientStartedCounter:    m.clientStartedCounter,
		clientHandledCounter:    m.clientHandledCounter,
		clientStreamMsgReceived: m.clientStreamMsgReceived,
		clientStreamMsgSent:     m.clientStreamMsgSent,

		clientHandledHistogramEnabled: m.clientHandledHistogramEnabled,
		clientHandledHistogramOpts:    m.clientHandledHistogramOpts,
		clientHandledHistogram:        m.clientHandledHistogram,

		clientStreamRecvHistogramEnabled: m.clientStreamRecvHistogramEnabled,
		clientStreamRecvHistogramOpts:    m.clientStreamRecvHistogramOpts,
		clientStreamRecvHistogram:        m.clientStreamRecvHistogram,

		clientStreamSendHistogramEnabled: m.clientStreamSendHistogramEnabled,
		clientStreamSendHistogramOpts:    m.clientStreamSendHistogramOpts,
		clientStreamSendHistogram:        m.clientStreamSendHistogram,
	}
}

// NewClientMetrics returns a ClientMetrics object. Use a new instance of
// ClientMetrics when not using the default Prometheus metrics registry, for
// example when wanting to control which metrics are added to a registry as
// opposed to automatically adding metrics via init functions.
func NewClientMetrics(counterOpts ...CounterOption) *ClientMetrics {
	opts := counterOptions(counterOpts)
	return &ClientMetrics{
		clientStartedCounter: prom.NewCounterVec(
			opts.apply(prom.CounterOpts{
				Name: "grpc_client_started_total",
				Help: "Total number of RPCs started on the client.",
			}), []string{"grpc_type", "grpc_service", "grpc_method"}),

		clientHandledCounter: prom.NewCounterVec(
			opts.apply(prom.CounterOpts{
				Name: "grpc_client_handled_total",
				Help: "Total number of RPCs completed by the client, regardless of success or failure.",
			}), []string{"grpc_type", "grpc_service", "grpc_method", "grpc_code"}),

		clientStreamMsgReceived: prom.NewCounterVec(
			opts.apply(prom.CounterOpts{
				Name: "grpc_client_msg_received_total",
				Help: "Total number of RPC stream messages received by the client.",
			}), []string{"grpc_type", "grpc_service", "grpc_method"}),

		clientStreamMsgSent: prom.NewCounterVec(
			opts.apply(prom.CounterOpts{
				Name: "grpc_client_msg_sent_total",
				Help: "Total number of gRPC stream messages sent by the client.",
			}), []string{"grpc_type", "grpc_service", "grpc_method"}),

		clientHandledHistogramEnabled: false,
		clientHandledHistogramOpts: prom.HistogramOpts{
			Name:    "grpc_client_handling_seconds",
			Help:    "Histogram of response latency (seconds) of the gRPC until it is finished by the application.",
			Buckets: prom.DefBuckets,
		},
		clientHandledHistogram:           nil,
		clientStreamRecvHistogramEnabled: false,
		clientStreamRecvHistogramOpts: prom.HistogramOpts{
			Name:    "grpc_client_msg_recv_handling_seconds",
			Help:    "Histogram of response latency (seconds) of the gRPC single message receive.",
			Buckets: prom.DefBuckets,
		},
		clientStreamRecvHistogram:        nil,
		clientStreamSendHistogramEnabled: false,
		clientStreamSendHistogramOpts: prom.HistogramOpts{
			Name:    "grpc_client_msg_send_handling_seconds",
			Help:    "Histogram of response latency (seconds) of the gRPC single message send.",
			Buckets: prom.DefBuckets,
		},
		clientStreamSendHistogram: nil,
	}
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector to the provided channel and returns once
// the last descriptor has been sent.
func (m *ClientMetrics) Describe(ch chan<- *prom.Desc) {
	c := m.safeClone()
	c.clientStartedCounter.Describe(ch)
	c.clientHandledCounter.Describe(ch)
	c.clientStreamMsgReceived.Describe(ch)
	c.clientStreamMsgSent.Describe(ch)
	if c.clientHandledHistogramEnabled {
		c.clientHandledHistogram.Describe(ch)
	}
	if c.clientStreamRecvHistogramEnabled {
		c.clientStreamRecvHistogram.Describe(ch)
	}
	if c.clientStreamSendHistogramEnabled {
		c.clientStreamSendHistogram.Describe(ch)
	}
}

// Collect is called by the Prometheus registry when collecting
// metrics. The implementation sends each collected metric via the
// provided channel and returns once the last metric has been sent.
func (m *ClientMetrics) Collect(ch chan<- prom.Metric) {
	c := m.safeClone()
	c.clientStartedCounter.Collect(ch)
	c.clientHandledCounter.Collect(ch)
	c.clientStreamMsgReceived.Collect(ch)
	c.clientStreamMsgSent.Collect(ch)
	if c.clientHandledHistogramEnabled {
		c.clientHandledHistogram.Collect(ch)
	}
	if c.clientStreamRecvHistogramEnabled {
		c.clientStreamRecvHistogram.Collect(ch)
	}
	if c.clientStreamSendHistogramEnabled {
		c.clientStreamSendHistogram.Collect(ch)
	}
}

// EnableClientHandlingTimeHistogram turns on recording of handling time of RPCs.
// Histogram metrics can be very expensive for Prometheus to retain and query.
func (m *ClientMetrics) EnableClientHandlingTimeHistogram(opts ...HistogramOption) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, o := range opts {
		o(&m.clientHandledHistogramOpts)
	}
	if !m.clientHandledHistogramEnabled {
		m.clientHandledHistogram = prom.NewHistogramVec(
			m.clientHandledHistogramOpts,
			[]string{"grpc_type", "grpc_service", "grpc_method"},
		)
	}
	m.clientHandledHistogramEnabled = true
}

// EnableClientStreamReceiveTimeHistogram turns on recording of single message receive time of streaming RPCs.
// Histogram metrics can be very expensive for Prometheus to retain and query.
func (m *ClientMetrics) EnableClientStreamReceiveTimeHistogram(opts ...HistogramOption) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, o := range opts {
		o(&m.clientStreamRecvHistogramOpts)
	}

	if !m.clientStreamRecvHistogramEnabled {
		m.clientStreamRecvHistogram = prom.NewHistogramVec(
			m.clientStreamRecvHistogramOpts,
			[]string{"grpc_type", "grpc_service", "grpc_method"},
		)
	}

	m.clientStreamRecvHistogramEnabled = true
}

// EnableClientStreamSendTimeHistogram turns on recording of single message send time of streaming RPCs.
// Histogram metrics can be very expensive for Prometheus to retain and query.
func (m *ClientMetrics) EnableClientStreamSendTimeHistogram(opts ...HistogramOption) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, o := range opts {
		o(&m.clientStreamSendHistogramOpts)
	}

	if !m.clientStreamSendHistogramEnabled {
		m.clientStreamSendHistogram = prom.NewHistogramVec(
			m.clientStreamSendHistogramOpts,
			[]string{"grpc_type", "grpc_service", "grpc_method"},
		)
	}

	m.clientStreamSendHistogramEnabled = true
}

// UnaryClientInterceptor is a gRPC client-side interceptor that provides Prometheus monitoring for Unary RPCs.
func (m *ClientMetrics) UnaryClientInterceptor() func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		c := m.safeClone()
		monitor := newClientReporter(c, Unary, method)
		monitor.SentMessage()
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil {
			monitor.ReceivedMessage()
		}
		st, _ := status.FromError(err)
		monitor.Handled(st.Code())
		return err
	}
}

// StreamClientInterceptor is a gRPC client-side interceptor that provides Prometheus monitoring for Streaming RPCs.
func (m *ClientMetrics) StreamClientInterceptor() func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		c := m.safeClone()
		monitor := newClientReporter(c, clientStreamType(desc), method)
		clientStream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			st, _ := status.FromError(err)
			monitor.Handled(st.Code())
			return nil, err
		}
		return &monitoredClientStream{clientStream, monitor}, nil
	}
}

func clientStreamType(desc *grpc.StreamDesc) grpcType {
	if desc.ClientStreams && !desc.ServerStreams {
		return ClientStream
	} else if !desc.ClientStreams && desc.ServerStreams {
		return ServerStream
	}
	return BidiStream
}

// monitoredClientStream wraps grpc.ClientStream allowing each Sent/Recv of message to increment counters.
type monitoredClientStream struct {
	grpc.ClientStream
	monitor *clientReporter
}

func (s *monitoredClientStream) SendMsg(m interface{}) error {
	timer := s.monitor.SendMessageTimer()
	err := s.ClientStream.SendMsg(m)
	timer.ObserveDuration()
	if err == nil {
		s.monitor.SentMessage()
	}
	return err
}

func (s *monitoredClientStream) RecvMsg(m interface{}) error {
	timer := s.monitor.ReceiveMessageTimer()
	err := s.ClientStream.RecvMsg(m)
	timer.ObserveDuration()

	if err == nil {
		s.monitor.ReceivedMessage()
	} else if err == io.EOF {
		s.monitor.Handled(codes.OK)
	} else {
		st, _ := status.FromError(err)
		s.monitor.Handled(st.Code())
	}
	return err
}

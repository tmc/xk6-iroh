package perflab

import (
	"context"
	"fmt"
	"time"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

// goBackend is go-iroh, the pure-Go implementation. It is always linked,
// so "go" is the one impl every build of this extension can serve, and
// it is the default.
type goBackend struct{}

func init() { RegisterBackend(goBackend{}) }

func (goBackend) Name() string { return "go" }

// Capabilities: go-iroh carries the protocol crates, so this backend is
// the only one that can serve the blobs and gossip scenarios.
func (goBackend) Capabilities() Capabilities {
	return Capabilities{Blobs: true, Gossip: true}
}

func (goBackend) Bind(ctx context.Context, opts BindOptions) (BackendEndpoint, error) {
	irohOpts, err := goBindOptions(opts)
	if err != nil {
		return nil, err
	}
	ep, err := iroh.Bind(ctx, irohOpts...)
	if err != nil {
		return nil, fmt.Errorf("bind endpoint: %w", err)
	}
	return &goEndpoint{ep: ep}, nil
}

// goBindOptions translates BindOptions into go-iroh options. Forced
// relay mode also drops the IP transports, so a forced cell cannot
// quietly fall back to a direct path.
func goBindOptions(opts BindOptions) ([]iroh.Option, error) {
	switch opts.RelayMode {
	case "", "default":
		return nil, nil
	case "disabled":
		return []iroh.Option{iroh.WithRelayMode(relay.ModeDisabled())}, nil
	case "forced":
		u, err := netaddr.ParseRelayURL(opts.RelayURL)
		if err != nil {
			return nil, fmt.Errorf("parse relayURL: %w", err)
		}
		return []iroh.Option{
			iroh.WithRelayMode(relay.ModeCustomURLs(u)),
			iroh.WithoutIPTransports(),
		}, nil
	}
	return nil, fmt.Errorf("unknown relayMode %q", opts.RelayMode)
}

type goEndpoint struct{ ep *iroh.Endpoint }

// endpoint exposes the concrete go-iroh endpoint to the workloads that
// need more than the Backend interface offers, namely blobs and gossip.
func (e *goEndpoint) endpoint() *iroh.Endpoint { return e.ep }

func (e *goEndpoint) Connect(ctx context.Context, ticket, alpn string) (BackendConn, error) {
	addr, err := endpointticket.Decode(ticket)
	if err != nil {
		return nil, fmt.Errorf("decode target ticket: %w", err)
	}
	conn, err := e.ep.Connect(ctx, addr, alpn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return goConn{conn: conn}, nil
}

func (e *goEndpoint) Counters() map[string]uint64 { return goSocketCounters(e.ep) }

func (e *goEndpoint) Shutdown(ctx context.Context) error { return e.ep.Shutdown(ctx) }

// goSocketCounters names the socket counters MetricsSnapshot reports.
// relaySent and relayRecv are the pair that prove a relay-forced cell
// really took the relay path.
func goSocketCounters(ep *iroh.Endpoint) map[string]uint64 {
	s := ep.Metrics().Socket
	return map[string]uint64{
		"relaySent":    s.SendRelay,
		"relayRecv":    s.RecvDataRelay,
		"directSentV4": s.SendIPv4,
		"directSentV6": s.SendIPv6,
		"directRecvV4": s.RecvDataIPv4,
		"directRecvV6": s.RecvDataIPv6,
		"blackholed":   s.SendBlackholed,
		"pathsDirect":  s.PathsDirect,
		"pathsRelay":   s.PathsRelay,
	}
}

type goConn struct{ conn *iroh.Conn }

func (c goConn) OpenStream(ctx context.Context) (BackendStream, error) {
	s, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return goStream{s: s}, nil
}

func (c goConn) SendDatagram(p []byte) error { return c.conn.SendDatagram(p) }

func (c goConn) ReadDatagram(ctx context.Context) ([]byte, error) {
	return c.conn.ReadDatagram(ctx)
}

func (c goConn) MaxDatagramSize() (int, bool) { return c.conn.MaxDatagramSize() }

func (c goConn) Done() <-chan struct{} { return c.conn.Context().Done() }

func (c goConn) CloseWithError(code uint64, reason string) error {
	return c.conn.CloseWithError(code, reason)
}

type goStream struct{ s *iroh.Stream }

// stream exposes the concrete go-iroh stream to the blobs workload,
// whose protocol helpers take it directly.
func (s goStream) stream() *iroh.Stream { return s.s }

func (s goStream) Read(p []byte) (int, error)    { return s.s.Read(p) }
func (s goStream) Write(p []byte) (int, error)   { return s.s.Write(p) }
func (s goStream) SetDeadline(t time.Time) error { return s.s.SetDeadline(t) }

// CloseWrite finishes the sending half. go-iroh spells that Close: the
// read half stays usable, which is what lets the caller wait for the
// peer to drain the stream.
func (s goStream) CloseWrite() error { return s.s.Close() }

func (s goStream) CancelRead(code uint64)  { s.s.CancelRead(code) }
func (s goStream) CancelWrite(code uint64) { s.s.CancelWrite(code) }

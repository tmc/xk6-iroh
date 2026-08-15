package xk6irohffi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	iroh "git.coopcloud.tech/decentral1se/iroh-go"
	"github.com/tmc/go-iroh/endpointticket"
	xk6iroh "github.com/tmc/xk6-iroh"
)

func init() { xk6iroh.RegisterBackend(Backend{}) }

// Backend is Rust iroh reached through iroh-go.
type Backend struct{}

func (Backend) Name() string { return "ffi" }

// Capabilities: the bindings expose the endpoint layer only — no
// iroh-blobs and no iroh-gossip surface — so those scenarios cannot run
// against this implementation and say so rather than failing obscurely.
func (Backend) Capabilities() xk6iroh.Capabilities {
	return xk6iroh.Capabilities{Blobs: false, Gossip: false}
}

func (Backend) Bind(_ context.Context, opts xk6iroh.BindOptions) (xk6iroh.Endpoint, error) {
	// The context is unused throughout this backend: every binding call
	// is a blocking FFI call with no cancellation, so a bind or a read
	// cannot be interrupted by a cancelled context. Callers bound work
	// with their own timeouts instead.
	bindAddr := "127.0.0.1:0"
	alpns := [][]byte{}
	eo := iroh.EndpointOptions{BindAddr: &bindAddr, Alpns: &alpns}

	switch opts.RelayMode {
	case "", "default", "disabled":
		// Both map to a local-only endpoint. PresetN0 is deliberately
		// not used for "default": it is the n0 production preset, so it
		// would put load-test traffic and discovery on n0's public
		// relays. rust-peer and ffi-peer make the same choice — no relay
		// unless one is named explicitly.
		preset := iroh.PresetMinimal()
		eo.Preset = &preset
		mode := iroh.RelayModeDisabled()
		eo.RelayMode = &mode
	case "forced":
		// Refused rather than approximated. Forcing means direct IP
		// transports are off, and the bindings expose no way to disable
		// them: PresetMinimal and PresetN0DisableRelay both leave direct
		// paths enabled, so a "forced" cell here would quietly measure a
		// direct path and report it as relayed.
		return nil, fmt.Errorf("impl ffi cannot force the relay path: the bindings expose no way to disable direct transports, so a forced cell would silently measure a direct path")
	default:
		return nil, fmt.Errorf("unknown relayMode %q", opts.RelayMode)
	}

	ep, err := iroh.EndpointBind(eo)
	if err != nil {
		return nil, fmt.Errorf("bind endpoint: %w", irohErr(err))
	}
	return &endpoint{ep: ep}, nil
}

type endpoint struct{ ep *iroh.Endpoint }

func (e *endpoint) Connect(_ context.Context, ticket, alpn string) (xk6iroh.Conn, error) {
	addr, err := endpointAddr(ticket)
	if err != nil {
		return nil, err
	}
	conn, err := e.ep.Connect(addr, []byte(alpn))
	if err != nil {
		return nil, fmt.Errorf("connect: %w", irohErr(err))
	}
	return newConn(conn), nil
}

// endpointAddr resolves a perflab target ticket to an iroh EndpointAddr.
//
// iroh's own deserializer handles tickets iroh wrote, but not the ones
// go-iroh writes: EndpointTicketFromString rejects a perflab-server
// ticket with "Serde Deserialization Error". Rather than restrict this
// backend to Rust targets, fall back to decoding with go-iroh — pure Go,
// and only ticket parsing, so nothing on the measured data path changes.
//
// Worth knowing rather than working around silently: the two ticket
// encodings are not interchangeable in this direction, even though the
// connections they describe interoperate fine.
func endpointAddr(ticket string) (*iroh.EndpointAddr, error) {
	if t, err := iroh.EndpointTicketFromString(ticket); err == nil {
		return t.EndpointAddr(), nil
	}
	decoded, err := endpointticket.Decode(ticket)
	if err != nil {
		return nil, fmt.Errorf("decode target ticket: not an iroh ticket, and go-iroh rejected it too: %w", err)
	}
	id, err := iroh.EndpointIdFromString(decoded.ID.String())
	if err != nil {
		return nil, fmt.Errorf("convert endpoint id: %w", irohErr(err))
	}
	var addrs []string
	for _, ap := range decoded.IPAddrs() {
		addrs = append(addrs, ap.String())
	}
	var relayURL *string
	if us := decoded.RelayURLs(); len(us) > 0 {
		s := us[0].String()
		relayURL = &s
	}
	return iroh.NewEndpointAddr(id, relayURL, addrs), nil
}

// Counters translates the endpoint's metrics into the names
// MetricsSnapshot reports. Returning nil makes the caller fail with a
// clear message, which is the right outcome when the names this build of
// iroh exposes do not include the ones a cell is checking.
func (e *endpoint) Counters() map[string]uint64 {
	stats := e.ep.Stats()
	out := map[string]uint64{}
	for ours, theirs := range counterNames {
		if s, ok := stats[theirs]; ok {
			out[ours] = uint64(s.Value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// counterNames maps the perflab counter names onto iroh's, which are
// namespaced ("socket:send_relay"). relaySent and relayRecv are the pair
// a relay cell is judged by, so TestCounterNames checks the mapping still
// resolves against a live endpoint rather than letting a rename upstream
// quietly turn those counters into zeroes.
//
// perflab's blackholed has no counterpart in this build of iroh and is
// simply absent from the result.
var counterNames = map[string]string{
	"relaySent":    "socket:send_relay",
	"relayRecv":    "socket:recv_data_relay",
	"directSentV4": "socket:send_ipv4",
	"directSentV6": "socket:send_ipv6",
	"directRecvV4": "socket:recv_data_ipv4",
	"directRecvV6": "socket:recv_data_ipv6",
	"pathsDirect":  "socket:paths_direct",
	"pathsRelay":   "socket:paths_relay",
}

func (e *endpoint) Shutdown(context.Context) error { return e.ep.Close() }

type conn struct {
	c *iroh.Connection

	// done is closed when the connection dies. The bindings offer only a
	// blocking Closed(), so one goroutine per connection parks inside
	// Rust waiting for it. That is the same thread-per-blocking-call
	// shape as the rest of this backend.
	doneOnce sync.Once
	done     chan struct{}
}

func newConn(c *iroh.Connection) *conn {
	k := &conn{c: c, done: make(chan struct{})}
	go func() {
		k.c.Closed()
		k.doneOnce.Do(func() { close(k.done) })
	}()
	return k
}

func (k *conn) OpenStream(context.Context) (xk6iroh.Stream, error) {
	bi, err := k.c.OpenBi()
	if err != nil {
		return nil, irohErr(err)
	}
	return &stream{send: bi.Send(), recv: bi.Recv()}, nil
}

func (k *conn) SendDatagram(p []byte) error { return irohErr(k.c.SendDatagram(p)) }

func (k *conn) ReadDatagram(context.Context) ([]byte, error) {
	b, err := k.c.ReadDatagram()
	return b, irohErr(err)
}

func (k *conn) MaxDatagramSize() (int, bool) {
	n := k.c.MaxDatagramSize()
	if n == nil {
		return 0, false
	}
	return int(*n), true
}

func (k *conn) Done() <-chan struct{} { return k.done }

func (k *conn) CloseWithError(code uint64, reason string) error {
	err := k.c.Close(int64(code), []byte(reason))
	k.doneOnce.Do(func() { close(k.done) })
	return irohErr(err)
}

// stream adapts a BiStream to io.ReadWriter. The bindings return a fresh
// slice per read rather than filling one, so reads are buffered here and
// the caller's buffer size does not control the read size on the wire.
type stream struct {
	send *iroh.SendStream
	recv *iroh.RecvStream
	buf  []byte
	eof  bool
}

// readLimit is the per-call read size requested from the bindings. It
// matches ffi-peer and rust-peer, so a cell that compares this client
// with those peers is not also comparing read granularity.
const readLimit = 1 << 20

func (s *stream) Read(p []byte) (int, error) {
	if len(s.buf) == 0 {
		if s.eof {
			return 0, io.EOF
		}
		b, err := s.recv.Read(readLimit)
		if err != nil {
			return 0, irohErr(err)
		}
		if len(b) == 0 {
			s.eof = true
			return 0, io.EOF
		}
		s.buf = b
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

func (s *stream) Write(p []byte) (int, error) {
	n, err := s.send.Write(p)
	return int(n), irohErr(err)
}

// SetDeadline cannot be honored: the bindings have no per-stream
// deadline and no context, so a read blocks until it returns. It reports
// that rather than pretending, and the client ignores the result, so the
// effect is that stream deadlines do not bound an ffi cell — the k6
// iteration timeout does.
func (s *stream) SetDeadline(time.Time) error {
	return errors.New("impl ffi does not support stream deadlines")
}

// CloseWrite finishes the sending half, so the peer sees end of stream.
func (s *stream) CloseWrite() error { return irohErr(s.send.Finish()) }

func (s *stream) CancelRead(code uint64)  { _ = s.recv.Stop(code) }
func (s *stream) CancelWrite(code uint64) { _ = s.send.Reset(code) }

// irohErr renders a binding error as "kind: message" instead of the bare
// string "IrohError". The upstream iroh.As helper cannot: it asks for the
// value type while every error arrives as *IrohError, so its type
// assertion never holds and it returns the error untouched. Without this
// an FFI failure is undiagnosable, which is how a loaded host killing
// iterations once looked like an iroh version regression.
func irohErr(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*iroh.IrohError](err); ok {
		return fmt.Errorf("%s: %s", e.Message(), e.DebugMessage())
	}
	return err
}

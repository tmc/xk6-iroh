package xk6iroh

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// A Backend is one iroh implementation the client can drive. The pure-Go
// backend ("go", go-iroh, in backend_go.go) is always present. Others
// register themselves from separate modules, so an implementation that
// needs cgo or a Rust toolchain never becomes a dependency of this one:
//
//	xk6 build --with github.com/tmc/xk6-iroh
//
// stays pure Go, and adding a second --with for a backend module gains
// that implementation. Scripts select one with the client's impl option,
// which is stamped as the impl tag on every sample, so a run's numbers
// always say which implementation produced them.
//
// This interface is deliberately the endpoint layer only — bind,
// connect, streams, datagrams, counters. Bindings to iroh commonly stop
// there, so the protocol workloads are reported through Capabilities
// rather than assumed.
type Backend interface {
	// Name is the impl value scripts select this backend with.
	Name() string
	// Capabilities reports which workloads the backend can serve.
	Capabilities() Capabilities
	// Bind creates an endpoint.
	Bind(ctx context.Context, opts BindOptions) (Endpoint, error)
}

// Capabilities describes what a backend can serve beyond streams and
// datagrams, which every backend must support.
type Capabilities struct {
	// Blobs reports support for the iroh-blobs scenarios.
	Blobs bool
	// Gossip reports support for the iroh-gossip scenarios.
	Gossip bool
}

// BindOptions are the endpoint options a backend must honor.
type BindOptions struct {
	// RelayMode is "default", "disabled", or "forced". Forced also
	// disables direct IP transports, so every byte crosses the relay;
	// a backend that cannot disable them must fail rather than bind a
	// path that silently goes direct.
	RelayMode string
	// RelayURL is the relay for RelayMode "forced".
	RelayURL string
}

// An Endpoint is a bound endpoint, as returned by Backend.Bind.
type Endpoint interface {
	Connect(ctx context.Context, ticket, alpn string) (Conn, error)
	// Counters returns socket-level counters by the names
	// Client.MetricsSnapshot reports (relaySent, relayRecv, pathsRelay
	// and friends), or nil when the backend does not expose them.
	Counters() map[string]uint64
	Shutdown(ctx context.Context) error
}

// A Conn is a connection to the target peer.
type Conn interface {
	OpenStream(ctx context.Context) (Stream, error)
	SendDatagram(p []byte) error
	ReadDatagram(ctx context.Context) ([]byte, error)
	// MaxDatagramSize reports the peer's limit, false if unknown.
	MaxDatagramSize() (int, bool)
	// Done is closed when the connection dies, so the client redials.
	Done() <-chan struct{}
	CloseWithError(code uint64, reason string) error
}

// A Stream is one bidirectional stream. CloseWrite finishes the
// sending half; the peer then sees EOF, and reading to EOF afterwards is
// how the client observes that the peer drained the stream.
type Stream interface {
	io.ReadWriter
	SetDeadline(t time.Time) error
	CloseWrite() error
	CancelRead(code uint64)
	CancelWrite(code uint64)
}

var (
	backendsMu sync.RWMutex
	backends   = map[string]Backend{}
)

// RegisterBackend makes b selectable as impl: b.Name(). It panics on a
// duplicate name, which can only mean a build linked two backends
// claiming to be the same implementation.
func RegisterBackend(b Backend) {
	backendsMu.Lock()
	defer backendsMu.Unlock()
	name := b.Name()
	if _, dup := backends[name]; dup {
		panic("perflab: duplicate backend " + name)
	}
	backends[name] = b
}

// lookupBackend returns the named backend. The error names what this
// binary does have, because the usual cause is a backend that lives in
// another module and was not built in.
func lookupBackend(name string) (Backend, error) {
	backendsMu.RLock()
	defer backendsMu.RUnlock()
	if b, ok := backends[name]; ok {
		return b, nil
	}
	have := make([]string, 0, len(backends))
	for n := range backends {
		have = append(have, n)
	}
	sort.Strings(have)
	return nil, fmt.Errorf("unknown impl %q; this k6 binary has %v (a backend from another module needs its own --with at build time)", name, have)
}

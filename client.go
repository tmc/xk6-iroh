package perflab

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tmc/go-iroh-perflab/xk6-iroh/perflabpeer"
	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"go.k6.io/k6/v2/js/modules"
)

// DefaultALPN is the ALPN used when the script does not set one.
const DefaultALPN = "perflab/0"

// Config is the JS-facing Client configuration.
type Config struct {
	// Target is the endpoint ticket of the peer to dial.
	Target string `json:"target"`
	// ALPN is the connection ALPN; defaults to DefaultALPN.
	ALPN string `json:"alpn"`
	// EndpointScope is "vu" (default; one endpoint per VU) or
	// "shared" (one endpoint shared by all VUs).
	EndpointScope string `json:"endpointScope"`
	// Peer labels the target implementation ("go" or "rust"); it is
	// stamped as the peer tag on every emitted metric sample.
	Peer string `json:"peer"`
	// Impl selects the iroh implementation this client drives, by
	// backend name ("go", the default). It is stamped as the impl tag
	// on every sample, so peer names what the target runs and impl
	// names what the load generator runs. Selecting a backend that
	// this k6 binary was not built with is an error, never a silent
	// fallback: a mislabeled cell is worse than a failed one.
	Impl string `json:"impl"`
	// RelayMode is "default" (direct-only, this build's default),
	// "disabled" (explicitly no relay), or "forced" (relay only:
	// direct IP transports disabled). "forced" requires RelayURL.
	RelayMode string `json:"relayMode"`
	// RelayURL is the relay server URL for RelayMode "forced".
	RelayURL string `json:"relayURL"`
}

// Client is the JS-facing iroh client. Methods block; k6 runs each VU
// iteration on its own goroutine, so blocking calls are the v0 model.
type Client struct {
	config  Config
	target  netaddr.EndpointAddr
	backend Backend
	vu      modules.VU
	metrics *perflabMetrics
	root    *RootModule

	blobHash   blobs.Hash // hash from a blobs target ticket
	blobFormat blobs.BlobFormat
	hasBlob    bool

	mu         sync.Mutex
	endpoint   Endpoint // vu-scoped endpoint, lazily bound
	conn       Conn
	lastSocket map[string]uint64 // counters emitted by the last MetricsSnapshot

	gossip       *gossip.Gossip // lazily created by Gossip
	gossipTopic  *gossip.Topic
	gossipEchoes chan []byte // echo payloads from the topic reader
}

func newClient(mi *ModuleInstance, config Config) (*Client, error) {
	if config.Target == "" {
		return nil, fmt.Errorf("config.target is required")
	}
	if config.ALPN == "" {
		config.ALPN = DefaultALPN
	}
	switch config.EndpointScope {
	case "":
		config.EndpointScope = "vu"
	case "vu", "shared":
	default:
		return nil, fmt.Errorf("unknown endpointScope %q (want vu or shared)", config.EndpointScope)
	}
	// Peer is a free-form cell label stamped as the peer tag on every
	// metric sample; "go" and "rust" name the GG/GR cells, but scenarios
	// like relay-versus.js use labels such as "gorelay". Only "rust"
	// changes the JSONL lang field (see cellName).
	if config.Peer == "" {
		config.Peer = "go"
	}
	if config.Impl == "" {
		config.Impl = "go"
	}
	backend, err := lookupBackend(config.Impl)
	if err != nil {
		return nil, err
	}
	switch config.RelayMode {
	case "":
		config.RelayMode = "default"
	case "default", "disabled":
	case "forced":
		if config.RelayURL == "" {
			return nil, fmt.Errorf("relayMode forced requires relayURL")
		}
	default:
		return nil, fmt.Errorf("unknown relayMode %q (want default, disabled, or forced)", config.RelayMode)
	}
	c := &Client{
		config:  config,
		backend: backend,
		vu:      mi.vu,
		metrics: mi.metrics,
		root:    mi.root,
	}
	// Target is either a plain endpoint ticket or an iroh-blobs ticket
	// (endpoint address + hash). A blobs ticket selects the blobs ALPN
	// unless the script overrides it.
	addr, err := decodeTarget(config.Target)
	if err != nil {
		return nil, err
	}
	if bt, berr := blobs.ParseTicket(config.Target); berr == nil {
		c.blobHash = bt.Hash()
		c.blobFormat = bt.Format()
		c.hasBlob = true
		if config.ALPN == DefaultALPN {
			c.config.ALPN = blobs.ALPN
		}
	}
	c.target = addr
	return c, nil
}

// decodeTarget accepts either form of target a scenario may hold: a plain
// endpoint ticket, or an iroh-blobs ticket carrying an endpoint address
// and a hash. Both name an endpoint to dial, which is the only thing a
// caller wanting to connect needs from them.
//
// It exists because there are two places that must agree about this: the
// client, which reads the hash out of a blobs ticket, and the backend,
// which dials. When only the client understood blobs tickets, every blobs
// scenario failed at dial with "wrong prefix, expected endpoint" -- the
// ticket had been decoded successfully once already, a few lines away.
func decodeTarget(ticket string) (netaddr.EndpointAddr, error) {
	addr, err := endpointticket.Decode(ticket)
	if err == nil {
		return addr, nil
	}
	bt, berr := blobs.ParseTicket(ticket)
	if berr != nil {
		return netaddr.EndpointAddr{}, fmt.Errorf("decode target ticket: %w", err)
	}
	return bt.Addr(), nil
}

// bindOptions returns the backend-independent endpoint options
// implementing the configured relay mode.
func (c *Client) bindOptions() BindOptions {
	return BindOptions{RelayMode: c.config.RelayMode, RelayURL: c.config.RelayURL}
}

// optsKey identifies the client's endpoint-affecting configuration for
// shared-endpoint compatibility checks.
func (c *Client) optsKey() string {
	return c.config.RelayMode + "\x00" + c.config.RelayURL
}

// endpointFor returns the endpoint for the configured scope, binding it
// on first use.
func (c *Client) endpointFor(ctx context.Context) (Endpoint, error) {
	if c.config.EndpointScope == "shared" {
		return c.root.sharedEndpoint(ctx, c.optsKey(), c.backend, c.bindOptions())
	}
	if c.endpoint != nil {
		return c.endpoint, nil
	}
	ep, err := c.backend.Bind(ctx, c.bindOptions())
	if err != nil {
		return nil, err
	}
	c.endpoint = ep
	return ep, nil
}

// goEndpointFor returns the concrete go-iroh endpoint, for the workloads
// the Backend interface does not cover. Backends whose bindings stop at
// the endpoint layer cannot serve those, and say so by name.
func (c *Client) goEndpointFor(ctx context.Context, workload string) (*iroh.Endpoint, error) {
	ep, err := c.endpointFor(ctx)
	if err != nil {
		return nil, err
	}
	ge, ok := ep.(interface{ endpoint() *iroh.Endpoint })
	if !ok {
		return nil, fmt.Errorf("impl %q does not support %s", c.config.Impl, workload)
	}
	return ge.endpoint(), nil
}

// tags returns the client's base tag set (peer plus any extra pairs).
func (c *Client) tags(extra map[string]string) map[string]string {
	out := map[string]string{"peer": c.config.Peer, "impl": c.config.Impl}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// connect returns the client's connection, dialing on first use, and
// records iroh_dial_latency for each dial.
func (c *Client) connect(ctx context.Context) (Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		select {
		case <-c.conn.Done():
			c.conn = nil // connection died; redial
		default:
			return c.conn, nil
		}
	}
	ep, err := c.endpointFor(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	conn, err := ep.Connect(ctx, c.config.Target, c.config.ALPN)
	if err != nil {
		return nil, err
	}
	c.metrics.push(c.vu, c.metrics.dialLatency, metricsDuration(time.Since(start)), c.tags(nil))
	c.conn = conn
	return conn, nil
}

// metricsDuration converts a duration to k6's time metric unit (ms).
func metricsDuration(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// Dial establishes the connection to the target, dialing eagerly rather
// than on the first transfer.
func (c *Client) Dial() error {
	_, err := c.connect(c.vu.Context())
	if err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, c.tags(map[string]string{"stage": "dial"}))
	}
	return err
}

// StreamOpts configures Client.SendStreams.
// Step identifies one stage of a load schedule, for scenarios that
// vary offered load within a run.
//
// The script declares it per iteration rather than the extension
// deriving it from the clock: k6's executors do not expose the stage a
// given iteration was scheduled in, and an iteration that starts in one
// stage and finishes in the next belongs to the one that demanded it.
// Bucketing by timestamp afterwards would assign those to whichever
// stage they happened to land in, which is exactly the boundary where a
// saturating system misbehaves.
//
// A zero Step means constant load: the scenario has one stage and the
// corpus records no step axis at all.
type Step struct {
	// Name identifies the stage. It becomes part of the corpus rung,
	// so it must be stable across arms -- two arms compared at "step 3"
	// have to mean the same offered load by the same name.
	Name string `json:"name"`
	// OfferedRate is the load the schedule demanded during this stage,
	// in iterations per second. It is what delivered throughput is
	// reported against.
	OfferedRate float64 `json:"offeredRate"`
}

type StreamOpts struct {
	// Step names the load-schedule stage this fan-out belongs to; the
	// zero value means constant load. See Step.
	Step Step `json:"step"`
	// Streams is the number of concurrent streams to open.
	Streams int `json:"streams"`
	// Bytes is the number of bytes to send per stream.
	Bytes int64 `json:"bytes"`
	// MsgSize is the write chunk size in bytes.
	MsgSize int `json:"msgSize"`
	// TimeoutMs bounds the whole fan-out; a stream that has not
	// completed by then fails with stage "timeout" instead of hanging
	// the iteration (relay paths can stall indefinitely). Default 120s.
	TimeoutMs int `json:"timeoutMs"`
}

// StreamResult is returned to JS from SendStreams.
type StreamResult struct {
	Streams   int    `json:"streams"`
	Completed int    `json:"completed"`
	BytesSent int64  `json:"bytesSent"`
	Error     string `json:"error,omitempty"`
}

// SendStreams opens opts.Streams concurrent bidirectional streams on the
// client connection and writes opts.Bytes on each in opts.MsgSize chunks,
// then half-closes and waits for the peer to drain (EOF). Per-stream
// throughput and completion samples are aggregated in Go.
func (c *Client) SendStreams(opts StreamOpts) (StreamResult, error) {
	if opts.Streams <= 0 {
		opts.Streams = 1
	}
	if opts.Bytes <= 0 {
		opts.Bytes = 1 << 20
	}
	if opts.MsgSize <= 0 {
		opts.MsgSize = 64 << 10
	}
	if opts.TimeoutMs <= 0 {
		opts.TimeoutMs = 120000
	}
	res := StreamResult{Streams: opts.Streams}
	ctx, cancel := context.WithTimeout(c.vu.Context(), time.Duration(opts.TimeoutMs)*time.Millisecond)
	defer cancel()
	tags := withStep(c.tags(transferTags(opts.Streams, opts.MsgSize)), opts.Step)

	// Failures land in res.Error (and the iroh_errors metric), not in a
	// thrown JS exception: threshold gates, not script aborts, judge runs.
	// EchoDatagrams follows the same contract.
	conn, err := c.connect(ctx)
	if err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "dial"))
		res.Error = err.Error()
		return res, nil
	}

	outcomes := make([]streamOutcome, opts.Streams)
	start := time.Now()
	var wg sync.WaitGroup
	for i := range opts.Streams {
		wg.Go(func() {
			outcomes[i] = sendOneStream(ctx, conn, opts.Bytes, opts.MsgSize)
		})
	}
	wg.Wait()
	wall := time.Since(start)

	var firstErr error
	for _, o := range outcomes {
		completed := o.err == nil && o.sent == opts.Bytes
		c.metrics.push(c.vu, c.metrics.streamCompletion, boolValue(completed), tags)
		if o.sent > 0 {
			c.metrics.push(c.vu, c.metrics.bytesSent, float64(o.sent), tags)
			res.BytesSent += o.sent
		}
		if completed {
			res.Completed++
			if o.duration > 0 {
				c.metrics.push(c.vu, c.metrics.streamThroughput, float64(o.sent)/o.duration.Seconds(), tags)
			}
		} else {
			c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, o.stage))
			if firstErr == nil && o.err != nil {
				firstErr = o.err
			}
		}
	}
	// Aggregate bandwidth of the whole fan-out: total bytes over the
	// wall time of the concurrent transfer.
	if res.BytesSent > 0 && wall > 0 {
		c.metrics.push(c.vu, c.metrics.transferThroughput, float64(res.BytesSent)/wall.Seconds(), tags)
	}
	if firstErr != nil {
		res.Error = firstErr.Error()
	}
	c.root.recordTransfer(c.config.Peer, c.config.Impl, opts, res, wall, outcomes)
	return res, nil
}

// DatagramOpts configures Client.EchoDatagrams.
type DatagramOpts struct {
	// Count is the number of echo round trips to run.
	Count int `json:"count"`
	// Size is the datagram payload size in bytes.
	Size int `json:"size"`
	// TimeoutMs is the per-datagram echo deadline in milliseconds.
	TimeoutMs int `json:"timeoutMs"`
}

// DatagramResult is returned to JS from EchoDatagrams.
type DatagramResult struct {
	Sent   int    `json:"sent"`
	Echoed int    `json:"echoed"`
	Lost   int    `json:"lost"`
	Error  string `json:"error,omitempty"`
}

// EchoDatagrams sends opts.Count datagrams one at a time and waits for
// the peer to echo each back, recording iroh_datagram_rtt per round trip
// and iroh_datagram_loss for datagrams that miss the deadline. The first
// 8 bytes of each payload carry a sequence number so late echoes are not
// credited to the wrong send.
func (c *Client) EchoDatagrams(opts DatagramOpts) (DatagramResult, error) {
	if opts.Count <= 0 {
		opts.Count = 100
	}
	if opts.Size < 8 {
		opts.Size = 512
	}
	if opts.TimeoutMs <= 0 {
		opts.TimeoutMs = 1000
	}
	var res DatagramResult
	ctx := c.vu.Context()
	tags := c.tags(map[string]string{"msg_size": fmt.Sprint(opts.Size)})

	conn, err := c.connect(ctx)
	if err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "dial"))
		res.Error = err.Error()
		return res, nil
	}
	if max, ok := conn.MaxDatagramSize(); ok && opts.Size > max {
		opts.Size = max
	}
	buf := make([]byte, opts.Size)
	timeout := time.Duration(opts.TimeoutMs) * time.Millisecond
	for seq := range opts.Count {
		binary.BigEndian.PutUint64(buf[:8], uint64(seq))
		start := time.Now()
		if err := conn.SendDatagram(buf); err != nil {
			c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "datagram"))
			res.Error = fmt.Errorf("send datagram: %w", err).Error()
			return res, nil
		}
		res.Sent++
		echoed, err := c.awaitEcho(ctx, conn, uint64(seq), timeout)
		if err != nil {
			c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "datagram"))
			res.Error = err.Error()
			return res, nil
		}
		c.metrics.push(c.vu, c.metrics.datagramLoss, boolValue(!echoed), tags)
		if echoed {
			res.Echoed++
			c.metrics.push(c.vu, c.metrics.datagramRTT, metricsDuration(time.Since(start)), tags)
		} else {
			res.Lost++
		}
	}
	return res, nil
}

// awaitEcho reads datagrams until one carries seq or the timeout lapses.
// It reports false (no error) on timeout: datagram loss is an expected
// outcome, not a failure.
func (c *Client) awaitEcho(ctx context.Context, conn Conn, seq uint64, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		b, err := conn.ReadDatagram(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return false, nil
			}
			return false, fmt.Errorf("read datagram: %w", err)
		}
		if len(b) >= 8 && binary.BigEndian.Uint64(b[:8]) == seq {
			return true, nil
		}
	}
}

// MetricsSnapshot returns the endpoint's cumulative socket-level
// counters to JS and pushes the delta since the previous call as
// iroh_socket_counters samples (tag counter). Scenarios call it at the
// end of each iteration: relaySent/relayRecv prove or disprove that
// traffic took the relay path.
func (c *Client) MetricsSnapshot() (map[string]uint64, error) {
	c.mu.Lock()
	ep := c.endpoint
	c.mu.Unlock()
	if c.config.EndpointScope == "shared" {
		var err error
		ep, err = c.root.sharedEndpoint(c.vu.Context(), c.optsKey(), c.backend, c.bindOptions())
		if err != nil {
			return nil, err
		}
	}
	if ep == nil {
		return nil, fmt.Errorf("no endpoint bound yet")
	}
	counters := ep.Counters()
	if counters == nil {
		return nil, fmt.Errorf("impl %q does not expose socket counters", c.config.Impl)
	}
	var delta map[string]uint64
	if c.config.EndpointScope == "shared" {
		// One baseline for the whole test: per-client baselines would
		// re-emit the same shared-endpoint delta once per VU.
		delta = c.root.sharedSocketDelta(counters)
	} else {
		c.mu.Lock()
		last := c.lastSocket
		c.lastSocket = counters
		c.mu.Unlock()
		delta = make(map[string]uint64, len(counters))
		for name, v := range counters {
			delta[name] = v - last[name]
		}
	}
	for name, d := range delta {
		if d > 0 {
			c.metrics.push(c.vu, c.metrics.socketCounters, float64(d), c.tags(map[string]string{"counter": name}))
		}
	}
	return counters, nil
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// streamOutcome is the result of one stream transfer. stage names the
// phase that failed (open, write, close, drain) for error classing.
type streamOutcome struct {
	sent     int64
	duration time.Duration
	err      error
	stage    string
}

// sendOneStream writes total bytes to one new stream in msgSize chunks,
// half-closes it, and waits for the peer to finish reading (EOF on the
// return direction), so the measured duration covers delivery.
// clientStreamDebug enables per-stream send-side offset dumps when
// PERFLAB_STREAM_DEBUG names a file (diagnostic; see StreamDebug).
var clientStreamDebug = sync.OnceValue(func() *perflabpeer.StreamDebug {
	d, err := perflabpeer.NewStreamDebug("send", os.Getenv("PERFLAB_STREAM_DEBUG"), 5*time.Second)
	if err != nil {
		return nil
	}
	return d
})

func sendOneStream(ctx context.Context, conn Conn, total int64, msgSize int) streamOutcome {
	start := time.Now()
	probe := clientStreamDebug().Register()
	defer probe.Done()
	probe.SetPhase("open")
	probe.BeginOp()
	s, err := conn.OpenStream(ctx)
	probe.EndOp(0)
	if err != nil {
		return streamOutcome{err: fmt.Errorf("open stream: %w", err), stage: "open"}
	}
	defer s.CancelRead(0)
	if deadline, ok := ctx.Deadline(); ok {
		s.SetDeadline(deadline)
	}
	probe.SetPhase("write")
	buf := make([]byte, msgSize)
	var sent int64
	for sent < total {
		n := int64(len(buf))
		if remaining := total - sent; remaining < n {
			n = remaining
		}
		probe.BeginOp()
		wn, werr := s.Write(buf[:n])
		probe.EndOp(wn)
		sent += int64(wn)
		if werr != nil {
			s.CancelWrite(0)
			return streamOutcome{sent: sent, duration: time.Since(start), err: fmt.Errorf("write stream: %w", werr), stage: "write"}
		}
	}
	probe.SetPhase("close")
	probe.BeginOp()
	err = s.CloseWrite()
	probe.EndOp(0)
	if err != nil {
		return streamOutcome{sent: sent, duration: time.Since(start), err: fmt.Errorf("close stream: %w", err), stage: "close"}
	}
	// The sink half-closes its side after draining; EOF here means the
	// peer consumed the full stream.
	probe.SetPhase("drain")
	probe.BeginOp()
	_, err = io.Copy(io.Discard, s)
	probe.EndOp(0)
	if err != nil {
		return streamOutcome{sent: sent, duration: time.Since(start), err: fmt.Errorf("await peer close: %w", err), stage: "drain"}
	}
	return streamOutcome{sent: sent, duration: time.Since(start)}
}

// Close closes the client's connection and vu-scoped endpoint. Shared
// endpoints stay open for other VUs.
// The lock is held only to take the handles, not across the teardown
// itself: Shutdown blocks for up to five seconds, and holding the mutex
// through it stalls any concurrent MetricsSnapshot at exactly the moment
// end-of-iteration metrics are collected. Nothing below reads client
// state, so the copy is all the exclusion this needs.
//
// The gossip topic is closed here too. It owns a goroutine ranging over
// its event channel, and a client that closed without ending it leaked
// one goroutine and a 1024-slot channel per iteration -- and under a
// shared endpoint, which stays open on purpose, nothing ever ended it.
func (c *Client) Close() error {
	c.mu.Lock()
	conn, ep, topic := c.conn, c.endpoint, c.gossipTopic
	c.conn, c.endpoint, c.gossipTopic = nil, nil, nil
	c.gossip, c.gossipEchoes = nil, nil
	c.mu.Unlock()

	if conn != nil {
		_ = conn.CloseWithError(0, "done")
	}
	if topic != nil {
		// Closing the topic ends Events, which returns the pump.
		_ = topic.Close()
	}
	if ep != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ep.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown endpoint: %w", err)
		}
	}
	return nil
}

// FetchOpts configures Client.FetchBlob.
type FetchOpts struct {
	// TimeoutMs bounds the whole fetch in milliseconds (default 600000).
	TimeoutMs int `json:"timeoutMs"`
	// StallMs fails the fetch when no bytes arrive for this long
	// (default 30000). A transfer may legitimately be slow, but a
	// zero-progress window means the flow-control path stopped
	// delivering, which throughput alone does not distinguish.
	StallMs int `json:"stallMs"`
}

// FetchResult is returned to JS from FetchBlob.
type FetchResult struct {
	Bytes     int64 `json:"bytes"`
	Completed bool  `json:"completed"`
	Stalled   bool  `json:"stalled"`
	// Entries is the number of child blobs fetched when the target is a
	// HashSeq (collection) ticket; zero for raw blob tickets.
	Entries int    `json:"entries"`
	Error   string `json:"error,omitempty"`
}

// progressWriter counts bytes and remembers when the last byte arrived.
// When probe is non-nil each write is reported as one op, so stream
// debug dumps show fetch progress and distinguish a stalled fetch
// (blocked_ms growing) from a slow one.
type progressWriter struct {
	n        int64
	lastUnix atomic.Int64 // UnixNano of last progress
	probe    *perflabpeer.StreamProbe
}

func (w *progressWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	w.lastUnix.Store(time.Now().UnixNano())
	w.probe.EndOp(len(p))
	w.probe.BeginOp()
	return len(p), nil
}

// FetchBlob downloads and verifies the blob named by the client's blobs
// target ticket using the iroh-blobs get protocol (bao-verified receive
// path — verification cost is part of the measurement). Failures land in
// the result, not JS exceptions, matching SendStreams.
func (c *Client) FetchBlob(opts FetchOpts) (FetchResult, error) {
	var res FetchResult
	if !c.hasBlob {
		return res, fmt.Errorf("fetchBlob requires a blobs ticket target")
	}
	if opts.TimeoutMs <= 0 {
		opts.TimeoutMs = 600000
	}
	if opts.StallMs <= 0 {
		opts.StallMs = 30000
	}
	if !c.backend.Capabilities().Blobs {
		return res, fmt.Errorf("impl %q does not support the blobs scenarios", c.config.Impl)
	}
	ctx, cancel := context.WithTimeout(c.vu.Context(), time.Duration(opts.TimeoutMs)*time.Millisecond)
	defer cancel()
	tags := c.tags(nil)

	conn, err := c.connect(ctx)
	if err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "dial"))
		res.Error = err.Error()
		return res, nil
	}
	probe := clientStreamDebug().Register()
	defer probe.Done()
	probe.SetPhase("open")
	probe.BeginOp()
	s, err := conn.OpenStream(ctx)
	probe.EndOp(0)
	if err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "open"))
		res.Error = err.Error()
		return res, nil
	}
	defer s.CancelRead(0)

	pw := &progressWriter{probe: probe}
	pw.lastUnix.Store(time.Now().UnixNano())
	stall := time.Duration(opts.StallMs) * time.Millisecond
	stallCtx, stallCancel := context.WithCancel(ctx)
	defer stallCancel()
	stalled := make(chan struct{})
	// The stall watchdog needs byte-level progress, which only the raw
	// DownloadBlob path reports; collection fetches rely on the timeout.
	if !c.blobFormat.IsHashSeq() {
		go func() {
			t := time.NewTicker(stall / 4)
			defer t.Stop()
			for {
				select {
				case <-stallCtx.Done():
					return
				case <-t.C:
					last := time.Unix(0, pw.lastUnix.Load())
					if time.Since(last) > stall {
						close(stalled)
						cancel()
						return
					}
				}
			}
		}()
	}

	start := time.Now()
	bs, ok := s.(interface{ stream() *iroh.Stream })
	if !ok {
		return res, fmt.Errorf("impl %q does not support the blobs scenarios", c.config.Impl)
	}
	probe.SetPhase("fetch")
	probe.BeginOp()
	if c.blobFormat.IsHashSeq() {
		// Collection: fetch the hash sequence, metadata, and every child
		// blob on one stream.
		var coll blobs.Collection
		var children [][]byte
		coll, children, err = blobs.GetCollectionBytes(stallCtx, bs.stream(), c.blobHash)
		for _, b := range children {
			pw.Write(b)
		}
		res.Entries = coll.Len()
	} else {
		err = blobs.DownloadBlob(stallCtx, bs.stream(), c.blobHash, pw)
	}
	wall := time.Since(start)
	probe.EndOp(0)
	stallCancel()
	select {
	case <-stalled:
		res.Stalled = true
	default:
	}

	res.Bytes = pw.n
	if pw.n > 0 {
		c.metrics.push(c.vu, c.metrics.blobBytes, float64(pw.n), tags)
	}
	if err != nil {
		stage := "fetch"
		if res.Stalled {
			stage = "stall"
		}
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, stage))
		res.Error = err.Error()
		return res, nil
	}
	res.Completed = true
	if wall > 0 {
		c.metrics.push(c.vu, c.metrics.blobThroughput, float64(pw.n)/wall.Seconds(), tags)
	}
	return res, nil
}

// RequestOpts configures Client.Request.
type RequestOpts struct {
	// Step names the load-schedule stage this request belongs to; the
	// zero value means constant load. See Step.
	Step Step `json:"step"`
	// Bytes is the request payload size (default 256).
	Bytes int `json:"bytes"`
	// TimeoutMs bounds one round trip in milliseconds (default 10000).
	TimeoutMs int `json:"timeoutMs"`
}

// RequestResult is returned to JS from Request.
type RequestResult struct {
	Sent     int64  `json:"sent"`
	Received int64  `json:"received"`
	Error    string `json:"error,omitempty"`
}

// Request models one RPC round trip against an echo-mode peer: open a
// stream, write the request, half-close, read the full response, and
// record iroh_request_rtt for the open-to-EOF wall time. High-rate
// scenarios use this to stress the stream open/close fast path.
func (c *Client) Request(opts RequestOpts) (RequestResult, error) {
	var res RequestResult
	if opts.Bytes <= 0 {
		opts.Bytes = 256
	}
	if opts.TimeoutMs <= 0 {
		opts.TimeoutMs = 10000
	}
	ctx, cancel := context.WithTimeout(c.vu.Context(), time.Duration(opts.TimeoutMs)*time.Millisecond)
	defer cancel()
	tags := withStep(c.tags(nil), opts.Step)

	conn, err := c.connect(ctx)
	if err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "dial"))
		res.Error = err.Error()
		return res, nil
	}
	start := time.Now()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "open"))
		res.Error = err.Error()
		return res, nil
	}
	defer s.CancelRead(0)
	if deadline, ok := ctx.Deadline(); ok {
		s.SetDeadline(deadline)
	}
	n, err := s.Write(make([]byte, opts.Bytes))
	res.Sent = int64(n)
	if err != nil {
		s.CancelWrite(0)
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "write"))
		res.Error = err.Error()
		return res, nil
	}
	if err := s.CloseWrite(); err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "close"))
		res.Error = err.Error()
		return res, nil
	}
	rn, err := io.Copy(io.Discard, s)
	res.Received = rn
	if err != nil {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, "drain"))
		res.Error = err.Error()
		return res, nil
	}
	rtt := time.Since(start)
	c.metrics.push(c.vu, c.metrics.requestRTT, metricsDuration(rtt), tags)
	c.root.recordRequest(c.config.Peer, c.config.Impl, opts, res, rtt)
	return res, nil
}

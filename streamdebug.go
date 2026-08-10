package perflab

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// StreamDebug periodically dumps per-stream transfer offsets and
// blocked-in-Read/Write state to a file, for diagnosing wedged
// connections: a writer ahead of the reader by a flow-control window
// with the reader blocked points at an undeliverable receive gap; equal
// offsets with the writer blocked point at credit starvation.
//
// A nil *StreamDebug is valid and disables all probes.
type StreamDebug struct {
	side string
	f    *os.File

	mu     sync.Mutex
	probes []*StreamProbe
	nextID int64
	done   chan struct{}
}

// NewStreamDebug appends dumps for side ("send" or "sink") to path
// every interval. It returns nil (probes disabled) when path is empty.
func NewStreamDebug(side, path string, interval time.Duration) (*StreamDebug, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open stream debug file: %w", err)
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	d := &StreamDebug{side: side, f: f, done: make(chan struct{})}
	go d.loop(interval)
	return d, nil
}

// Register adds a stream probe. Safe on a nil StreamDebug (returns nil).
func (d *StreamDebug) Register() *StreamProbe {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextID++
	p := &StreamProbe{id: d.nextID}
	d.probes = append(d.probes, p)
	return p
}

// Close stops the dump loop after writing one final dump.
func (d *StreamDebug) Close() error {
	if d == nil {
		return nil
	}
	close(d.done)
	d.dump()
	return d.f.Close()
}

func (d *StreamDebug) loop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-t.C:
			d.dump()
		}
	}
}

func (d *StreamDebug) dump() {
	d.mu.Lock()
	probes := d.probes
	d.mu.Unlock()
	now := time.Now()
	for _, p := range probes {
		if p.closed.Load() {
			continue
		}
		var blockedMs, idleMs int64
		if t := p.opStart.Load(); t != 0 {
			blockedMs = now.Sub(time.Unix(0, t)).Milliseconds()
		}
		if t := p.lastOp.Load(); t != 0 {
			idleMs = now.Sub(time.Unix(0, t)).Milliseconds()
		}
		phase, _ := p.phase.Load().(string)
		if phase == "" {
			phase = "-"
		}
		fmt.Fprintf(d.f, "%s side=%s stream=%d phase=%s bytes=%d blocked_ms=%d idle_ms=%d\n",
			now.UTC().Format(time.RFC3339), d.side, p.id, phase, p.bytes.Load(), blockedMs, idleMs)
	}
}

// StreamProbe tracks one stream's cumulative bytes and whether it is
// currently blocked inside a Read or Write. All methods are safe on a
// nil probe.
type StreamProbe struct {
	id      int64
	phase   atomic.Value // string: lifecycle stage of the current op
	bytes   atomic.Int64
	opStart atomic.Int64 // UnixNano when the current op began; 0 outside ops
	lastOp  atomic.Int64 // UnixNano when the last op returned
	closed  atomic.Bool
}

// BeginOp marks entry into a Read or Write call.
func (p *StreamProbe) BeginOp() {
	if p != nil {
		p.opStart.Store(time.Now().UnixNano())
	}
}

// SetPhase labels the stream's current lifecycle stage (e.g. "open",
// "write", "close", "drain", "accept", "read") so dumps distinguish a
// stall in stream data ops from one in open/close handshakes.
func (p *StreamProbe) SetPhase(phase string) {
	if p != nil {
		p.phase.Store(phase)
	}
}

// EndOp marks the op's return with the bytes it moved.
func (p *StreamProbe) EndOp(n int) {
	if p == nil {
		return
	}
	p.bytes.Add(int64(n))
	p.opStart.Store(0)
	p.lastOp.Store(time.Now().UnixNano())
}

// Done marks the stream finished so it drops out of future dumps.
func (p *StreamProbe) Done() {
	if p != nil {
		p.closed.Store(true)
	}
}

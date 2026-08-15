package xk6iroh

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/xk6-iroh/irohpeer"
)

// Gossip wire format: payload[0] is 'q' (query, client to swarm) or 'r'
// (reply, echoed by gossip-member peers); the next 16 bytes are the
// sequence number and the send time in unix nanoseconds; the rest is
// padding to MsgSize.
const gossipHeaderSize = 17

// GossipOpts configures Client.Gossip.
type GossipOpts struct {
	// Count is the number of messages to broadcast. Default 100.
	Count int `json:"count"`
	// MsgSize is the payload size in bytes (min 17). Default 512.
	MsgSize int `json:"msgSize"`
	// IntervalMs is the gap between broadcasts. Default 10.
	IntervalMs int `json:"intervalMs"`
	// TimeoutMs bounds the whole call, including the join. Default 60s.
	TimeoutMs int `json:"timeoutMs"`
	// Topic is the topic name. Default irohpeer.DefaultGossipTopic.
	Topic string `json:"topic"`
}

// GossipResult is returned to JS from Gossip.
type GossipResult struct {
	Sent   int    `json:"sent"`
	Echoed int    `json:"echoed"`
	Lost   int    `json:"lost"`
	Error  string `json:"error,omitempty"`
}

// Gossip joins the configured gossip topic (bootstrapping via the target
// peer on first use), broadcasts opts.Count messages, and counts the
// echo each gossip-member peer rebroadcasts. Each echo produces an
// iroh_gossip_rtt sample (broadcast to echo delivery); messages not
// echoed within the timeout count as iroh_gossip_loss.
func (c *Client) Gossip(opts GossipOpts) (GossipResult, error) {
	if opts.Count <= 0 {
		opts.Count = 100
	}
	if opts.MsgSize < gossipHeaderSize {
		if opts.MsgSize != 0 {
			return GossipResult{}, fmt.Errorf("msgSize must be at least %d", gossipHeaderSize)
		}
		opts.MsgSize = 512
	}
	if opts.IntervalMs == 0 {
		opts.IntervalMs = 10
	}
	if opts.TimeoutMs <= 0 {
		opts.TimeoutMs = 60000
	}
	if opts.Topic == "" {
		opts.Topic = irohpeer.DefaultGossipTopic
	}
	ctx, cancel := context.WithTimeout(c.vu.Context(), time.Duration(opts.TimeoutMs)*time.Millisecond)
	defer cancel()

	tags := c.tags(nil)
	res := GossipResult{}
	fail := func(stage string, err error) (GossipResult, error) {
		c.metrics.push(c.vu, c.metrics.errors, 1, withStage(tags, stage))
		res.Error = err.Error()
		return res, nil
	}

	if err := c.joinGossip(ctx, opts.Topic); err != nil {
		return fail("join", err)
	}

	// Take the topic and echo channel once, under the lock that Close
	// clears them under. Reading the fields directly would race a Close
	// on another goroutine, and the failure would be a nil dereference in
	// the middle of a run rather than an error the scenario can report.
	topic, echoes := c.gossipHandles()
	if topic == nil {
		return fail("join", errors.New("client closed"))
	}

	// Drain echoes queued by a previous iteration so stale replies are
	// not matched against this round's sequence numbers.
	for {
		select {
		case <-echoes:
			continue
		default:
		}
		break
	}

	payload := make([]byte, opts.MsgSize)
	payload[0] = 'q'
	sent := make(map[uint64]time.Time, opts.Count)
	interval := time.Duration(opts.IntervalMs) * time.Millisecond
	deadline := time.NewTimer(0)
	<-deadline.C

	for seq := uint64(1); seq <= uint64(opts.Count); seq++ {
		now := time.Now()
		binary.BigEndian.PutUint64(payload[1:], seq)
		binary.BigEndian.PutUint64(payload[9:], uint64(now.UnixNano()))
		if err := topic.Broadcast(ctx, payload); err != nil {
			res.Lost = opts.Count - res.Echoed
			r, _ := fail("broadcast", fmt.Errorf("broadcast seq %d: %w", seq, err))
			return r, nil
		}
		sent[seq] = now
		res.Sent++
		c.collectEchoes(echoes, sent, &res)
		if interval > 0 && seq < uint64(opts.Count) {
			deadline.Reset(interval)
			select {
			case <-deadline.C:
			case <-ctx.Done():
			}
		}
		if ctx.Err() != nil {
			break
		}
	}

	// Wait for outstanding echoes until the timeout.
	for len(sent) > 0 && ctx.Err() == nil {
		select {
		case p := <-echoes:
			c.matchEcho(p, sent, &res)
		case <-ctx.Done():
		}
	}
	res.Lost = len(sent)
	for range sent {
		c.metrics.push(c.vu, c.metrics.gossipLoss, 1, tags)
	}
	if res.Lost > 0 {
		res.Error = fmt.Sprintf("%d of %d messages not echoed within timeout", res.Lost, res.Sent)
	}
	return res, nil
}

// gossipHandles returns the joined topic and its echo channel, or nil if
// the client has been closed.
func (c *Client) gossipHandles() (*gossip.Topic, chan []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gossipTopic, c.gossipEchoes
}

// joinGossip creates the client's gossip instance and joins topicName
// with the target peer as bootstrap, once per client.
func (c *Client) joinGossip(ctx context.Context, topicName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gossipTopic != nil {
		return nil
	}
	if !c.backend.Capabilities().Gossip {
		return fmt.Errorf("impl %q does not support the gossip scenarios", c.config.Impl)
	}
	ep, err := c.goEndpointFor(ctx, "gossip")
	if err != nil {
		return err
	}
	c.gossip = gossip.NewGossip(ep)
	start := time.Now()
	t, err := c.gossip.SubscribeAndJoin(ctx, irohpeer.GossipTopic(topicName), []netaddr.EndpointAddr{c.target})
	if err != nil {
		c.gossip = nil
		return fmt.Errorf("join topic: %w", err)
	}
	c.metrics.push(c.vu, c.metrics.dialLatency, metricsDuration(time.Since(start)), c.tags(nil))
	c.gossipTopic = t
	c.gossipEchoes = make(chan []byte, 1024)
	echoes := c.gossipEchoes
	go func() {
		for ev, err := range t.Events() {
			if err != nil {
				return
			}
			if ev.Kind != gossip.Received || len(ev.Content) < gossipHeaderSize || ev.Content[0] != 'r' {
				continue
			}
			p := make([]byte, gossipHeaderSize)
			copy(p, ev.Content[:gossipHeaderSize])
			select {
			case echoes <- p:
			default: // slow collector; count as loss via sent map
			}
		}
	}()
	return nil
}

// collectEchoes drains any echoes received so far without blocking.
func (c *Client) collectEchoes(echoes chan []byte, sent map[uint64]time.Time, res *GossipResult) {
	for {
		select {
		case p := <-echoes:
			c.matchEcho(p, sent, res)
		default:
			return
		}
	}
}

// matchEcho matches one echo payload against outstanding sends and
// emits its RTT sample.
func (c *Client) matchEcho(p []byte, sent map[uint64]time.Time, res *GossipResult) {
	seq := binary.BigEndian.Uint64(p[1:])
	t0, ok := sent[seq]
	if !ok {
		return
	}
	delete(sent, seq)
	res.Echoed++
	c.metrics.push(c.vu, c.metrics.gossipRTT, metricsDuration(time.Since(t0)), c.tags(nil))
}

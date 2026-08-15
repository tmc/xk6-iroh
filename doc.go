// Package xk6iroh is a k6 extension (k6/x/iroh) that embeds go-iroh
// endpoints inside k6 virtual users to measure endpoint behavior under
// load.
//
// The module exposes a Client class to k6 scripts:
//
//	import iroh from 'k6/x/iroh';
//
//	const client = new iroh.Client({
//	    target: __ENV.TARGET_TICKET,   // endpoint ticket from the target peer
//	    alpn: 'perflab/0',
//	    endpointScope: 'vu',           // vu (default) | shared
//	    peer: __ENV.PEER || 'go',      // what the TARGET runs; tags every sample
//	    impl: __ENV.IMPL || 'go',      // which iroh THIS client drives
//	    relayMode: 'default',          // default | disabled | forced
//	    relayURL: __ENV.RELAY_URL,     // required for relayMode forced
//	});
//
// peer and impl are different axes: peer is a free-form label for the
// target's stack, while impl selects the implementation the load
// generator itself uses, from the backends linked into this k6 binary.
// "go" (go-iroh) is always present and is the default; others come from
// separate modules added with their own --with at build time, so a plain
// build stays pure Go. An impl this binary lacks is an error rather than
// a silent fallback, so a sample is always labelled with the
// implementation that produced it.
//
//	export default function () {
//	    const res = client.sendStreams({ streams: 16, bytes: 64 * 1024 * 1024, msgSize: 1024 });
//	}
//
// sendStreams opens the requested number of concurrent bidirectional
// streams on one connection and writes bytes on each in msgSize chunks.
// Per-stream throughput and completion are aggregated in Go and emitted
// as k6 samples tagged with streams and msg_size; no per-message samples
// are emitted.
//
// endpointScope selects endpoint lifetime: "vu" creates one lazy
// endpoint per virtual user; "shared" creates a single endpoint shared
// by all VUs (many streams and connections on one socket).
//
// echoDatagrams sends sequenced datagrams and waits for the peer's echo,
// emitting iroh_datagram_rtt and iroh_datagram_loss. metricsSnapshot
// returns the endpoint's cumulative socket counters and emits the delta
// since the last call as iroh_socket_counters samples; relaySent and
// relayRecv there prove whether traffic took the relay path.
//
// relayMode "forced" disables direct IP transports so all traffic goes
// through the relay named by relayURL — always a local relay (e.g.
// go-iroh's cmd/iroh-relay), never a production one.
//
// With the PERFLAB_JSONL environment variable set, every sendStreams
// fan-out appends one line of JSONL (rung/lang/sample/bytes/duration_ns
// plus provenance), a schema shared with go-iroh's benchmark harness so
// runs from both can be analyzed together.
//
// The target is any peer that accepts connections on the configured
// ALPN and drains or echoes what arrives; [irohpeer] holds the little
// both sides must agree on. Peers, scenarios and analysis tooling are a
// separate concern and are not part of this module.
package xk6iroh

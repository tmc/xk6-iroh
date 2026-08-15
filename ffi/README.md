# xk6-iroh/ffi

The `ffi` backend for the perflab k6 extension: the k6 client driving
Rust iroh through [iroh-go]'s uniffi/cgo bindings, selected per script
with `impl: 'ffi'`.

This measures the FFI boundary from the *client* side. `ffi-peer/` is the
same stack as a target; together they bracket the boundary from both
ends.

[iroh-go]: https://git.coopcloud.tech/decentral1se/iroh-go

## Why a separate module

A build-tagged file inside `xk6-iroh` would still put iroh-go and its
vendored static libraries into that module's `go.mod` and `go.sum` for
every consumer, tag or no tag, because `go mod tidy` resolves across all
build configurations — verified, not assumed. Keeping the backend here
means the plain

	xk6 build --with github.com/tmc/xk6-iroh

stays pure Go with no Rust toolchain, and only a build that asks for both
implementations pays for cgo.

## Build

	make build-ffi

That builds `libiroh.a` from `ffi/libiroh` and links a k6 with both
backends. By hand:

	xk6 build v2.2.0 --cgo=1 \
	  --with github.com/tmc/xk6-iroh=. \
	  --with github.com/tmc/xk6-iroh/ffi=./ffi

Then:

	TARGET_TICKET=$(cat ticket.txt) IMPL=ffi ./k6 run scenarios/multistream.js

`impl` is stamped on every sample and written to the JSONL, so a cell
always records which implementation generated its load. Selecting `ffi`
from a k6 built without this module is an error naming the backends that
binary does have — never a silent fallback to go-iroh.

## This backend is not currently reliable as a load generator

⚠ As of 2026-08-11 the `ffi` **client** fails roughly half the time at
4 streams × 4 MiB on darwin loopback. The failure is always the same
shape: zero streams opened, the sink's accept loop idle throughout, no
bytes moved anywhere. It never gets as far as opening a stream.

Measured, fresh sink every run:

| duration | pass | fail |
| --- | --- | --- |
| 6 s | 8 | 0 |
| 8 s | 4 | 6 |
| 12 s | 2 | 0 |

Duration-correlated but not duration-determined. The `go` client has
never failed in any configuration here or across cell B, and the failure
is not sink reuse or residue from a prior client — a fresh sink with no
prior client at all reproduces it, and the same client passes then fails
on one reused sink.

**Cause: a uniffi async completion callback that never fires.** A
SIGQUIT dump at the hang (43 goroutines) shows two `Connection.OpenBi`
calls and the `Connection.Closed` watchdog all blocked in
`chanrecv` inside `uniffiRustCallAsync` (`iroh_ffi.go:9796`), all three
on the *same* connection pointer. **Zero goroutines are inside cgo** —
no `runtime.cgocall`, no `_Cfunc_` frame anywhere in the dump. Nothing
is executing Rust. The Go side has already returned from the FFI call
and is parked on a Go channel waiting for a completion that never
arrives.

Two things follow. The defect is connection-wide rather than specific to
opening a stream — the `Closed` watchdog hanging on the same pointer is
the tell — so every outstanding async call on an affected connection
hangs. And it is **unrecoverable by construction**: `uniffiRustCallAsync`
waits on a bare channel with no timeout and no cancellation, so nothing
can unwind those goroutines. That is why the process ignores SIGTERM.

This is the missing `context.Context` (see the table below) surfacing as
a liveness bug rather than an ergonomic one. It is a defect in the
bindings, not in this backend, not in go-iroh, and not in the transport.

Note what it is *not*: it is not the same mechanism as the sink-side
concurrency ceiling. That is threads consumed by blocking reads with
work in flight; this hangs with zero streams in flight and nothing in
cgo. What the two share is only the absence of cancellation, which turns
both into unbounded waits instead of errors.

Two consequences matter more than the bug:

- **On failure k6 never exits.** SIGTERM is ignored; every occurrence
  needed SIGKILL. An unattended run hangs forever on the first failure
  rather than failing.
- **When it is not killed it exits 0**, reporting `0 out of 0`. A run
  that transferred nothing looks like a pass and emits no row, so
  anything downstream sees absence rather than breakage. The scenario
  gates now catch this (`iroh_bytes_sent: ['count>0']`), and the compare
  harness catches the no-row case with `-want`; neither existed when
  this bug was found.

Until this is diagnosed, treat `impl: 'ffi'` as a diagnostic tool rather
than a load generator, and never read a passing ffi-client cell without
checking it produced samples. The ffi **sink** (`ffi-peer/`) is
unaffected — every cell measured to date drove it with the `go` client.

## What this backend cannot do

Read this before trusting a number from it.

| Limit | Consequence |
| --- | --- |
| No iroh-blobs or iroh-gossip surface | `blobs-fetch.js`, `blobs-collection.js` and `gossip-overlay.js` cannot use `impl: 'ffi'`; they fail immediately, naming the scenario family |
| No way to disable direct transports | `relayMode: 'forced'` is **refused**. `PresetMinimal` and `PresetN0DisableRelay` both leave direct paths enabled, so a forced cell would measure a direct path and label it relayed |
| No `context.Context`, no cancellation | Every call blocks until it returns. Stream deadlines are unsupported, so an ffi cell is bounded by the k6 iteration timeout, not by `timeoutMs` |
| One OS thread per in-flight stream | Each blocked read parks a thread inside Rust, and connection liveness needs another parked in `Closed()` |

The last two are why this backend has a concurrency ceiling. Cell B
(`ffi-peer/README.md`, n=5, reads matched at 1 MiB across all sinks,
descriptive medians, **NOT statistically gated**) measured the slope from
4 to 16 streams at one write size:

| arm | 4 streams | 16 streams | slope |
| --- | --- | --- | --- |
| gg (go sink) | 246.90 | 248.73 | +0.7% |
| gr (rust sink) | 236.95 | 251.48 | +6.1% |
| ffi | 243.47 | 184.12 | **−24.4%** |

Both native stacks hold or gain where the FFI arm loses a quarter, and
the sample counts say the same thing without the throughput metric: 208
fan-outs completed for ffi against 280 and 284 native in the same 60 s.
At 16 streams the ffi arm's run ranges are disjoint from both native
arms, so that separation holds under the stricter reading; the medians
in the table are pooled across runs and are the weaker statement.

Repeatability points the same way as throughput. At 4 streams the FFI
arm is the least repeatable of the three (±4.9% against ±0.4% and
±0.5%), and at 16 it narrows to ±3.6% while separating cleanly. The arm
that cannot be placed at all is the FFI arm, at the rung where the
boundary is supposedly nearly free — which is what contention for a
thread per in-flight stream would look like, and is the reason item 5 of
`MEASUREMENT-PLAN.md` measures threads instead of inferring them.

Two corrections that matter more than the headline:

- **At 4 streams the FFI arm cannot be ordered against anything.** Its
  own repeat range there (237.10–261.79, ±4.9%) contains both native arms
  outright, where those arms sit at ±0.4% and ±0.5%. So no ffi comparison
  on that rung — ahead or behind — may be stated. What survives is a
  bound, not an ordering: all three sinks within about 4%. There is
  therefore no such thing as "the FFI penalty"; any single-shape ratio is
  an artifact of the shape picked, and the concurrency *slope* is the
  durable finding. The one ordering that does hold at 4 streams is gg
  ahead of gr, ratio 0.960, run ranges disjoint.
- **Small writes are not where this hurts.** 1 KiB writes cost the FFI
  arm about 5.5% against the native median; concurrency costs it 26%.
  Cell A's 2.05× at 1 KiB was mostly concurrency wearing a small-write
  costume, so read that number as superseded.

Read `impl: 'ffi'` results as a measurement of the boundary, not as a
load generator that can saturate a fast target: at high stream counts a
slow result is more likely the generator than the peer.

## Version pinning

The iroh version is decided by the archive that gets linked, not by any
Go file here. iroh-go vendors a `libiroh.a` built from *upstream's*
lockfile, so on linux a plain build silently links that older iroh
whatever `ffi/libiroh/Cargo.lock` says. `make build-ffi` puts the
locally built archive ahead of it on `CGO_LDFLAGS` and then verifies what
actually got linked. Confirm it directly on any binary you measure with:

	strings ./k6 | grep -o 'iroh-1\.[0-9.]*' | sort -u

Rust embeds crate source paths, so the binary names its own dependency
versions. Record that output alongside any cell's numbers.

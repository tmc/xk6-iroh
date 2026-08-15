# xk6-iroh/ffi

The `ffi` backend: the k6 client driving Rust iroh through [iroh-go]'s
uniffi/cgo bindings, selected per script with `impl: 'ffi'`.

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

On linux, no Rust toolchain is needed. iroh-go vendors a `libiroh.a` for
linux/amd64 and linux/arm64, and cgo picks it up:

	make build-ffi-vendored

On darwin there is no vendored archive, so one is built from
`ffi/libiroh`, which needs cargo:

	make build-ffi

The two differ in more than convenience: the vendored archive is built
from iroh-go's own lockfile and is currently **iroh 1.0.0**, where
`ffi/libiroh` pins **1.0.3**. Both targets print the version they linked.
For trying the backend out the difference rarely matters; for a number
you intend to quote, it does.

Then:

	TARGET_TICKET=$(cat ticket.txt) IMPL=ffi ./k6 run scenarios/multistream.js

`impl` is stamped on every sample and written to the JSONL, so a result
always records which implementation generated its load. Selecting `ffi`
from a k6 built without this module is an error naming the backends that
binary does have, rather than a silent fallback to go-iroh.

## Differences from the `go` backend

| Limit | Consequence |
| --- | --- |
| No iroh-blobs or iroh-gossip surface | Blobs and gossip scenarios cannot use `impl: 'ffi'`; they fail immediately, naming the scenario family |
| No way to disable direct transports | `relayMode: 'forced'` is refused. `PresetMinimal` and `PresetN0DisableRelay` both leave direct paths enabled, so a forced run would take a direct path while labelled relayed |
| No `context.Context`, no cancellation | Every call blocks until it returns. Stream deadlines are unsupported, so an ffi run is bounded by the k6 iteration timeout rather than by `timeoutMs` |
| One OS thread per in-flight stream | Each blocked read parks a thread inside Rust, and connection liveness needs another parked in `Closed()` |

The last two shape how this backend behaves under load. Its throughput
holds up at low stream counts and falls off as streams are added, where
both native stacks hold flat or gain — consistent with contention for a
thread per in-flight stream. Prefer it for measuring the FFI boundary
itself; at high stream counts a slow result is as likely to be the load
generator as the peer.

## Known issue: the client can hang

As of 2026-08-11 the `ffi` **client** intermittently fails to make
progress: zero streams opened, the peer's accept loop idle, no bytes
moved. It was reproducible at 4 streams × 4 MiB on darwin loopback,
correlated with run duration but not determined by it.

The cause is in the bindings rather than in this backend, go-iroh, or the
transport: a uniffi async completion callback that never fires. Every
outstanding async call on the affected connection parks on a Go channel
inside `uniffiRustCallAsync` with nothing executing in Rust, and because
that channel has no timeout and no cancellation, the goroutines cannot
unwind. That is also why an affected process does not respond to SIGTERM.

Two practical consequences:

- **A hung run needs SIGKILL**, so an unattended run will not fail on its
  own — it waits.
- **A run killed after hanging can still exit 0**, reporting `0 out of
  0`. Gate on bytes actually transferred (for example a threshold of
  `iroh_bytes_sent: ['count>0']`) so a run that moved nothing cannot read
  as a pass.

Until this is resolved, treat `impl: 'ffi'` as a way to measure the
boundary rather than as a dependable load generator, and check that any
passing ffi run actually produced samples.

## Which iroh did I link?

The iroh version is decided by the archive that gets linked, not by any
Go file here. Rust embeds crate source paths, so the binary names its own
dependency versions:

	strings ./k6 | grep -o 'iroh-1\.[0-9.]*' | sort -u

Worth recording alongside any result you keep.

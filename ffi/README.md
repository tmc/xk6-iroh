# xk6-iroh-ffi

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

	xk6 build --with github.com/tmc/go-iroh-perflab/xk6-iroh

stays pure Go with no Rust toolchain, and only a build that asks for both
implementations pays for cgo.

## Build

	make build-ffi

That builds `libiroh.a` from `ffi-peer/libiroh` and links a k6 with both
backends. By hand:

	xk6 build v2.2.0 --cgo=1 \
	  --with github.com/tmc/go-iroh-perflab/xk6-iroh=./xk6-iroh \
	  --with github.com/tmc/go-iroh-perflab/xk6-iroh-ffi=./xk6-iroh-ffi

Then:

	TARGET_TICKET=$(cat ticket.txt) IMPL=ffi ./k6 run scenarios/multistream.js

`impl` is stamped on every sample and written to the JSONL, so a cell
always records which implementation generated its load. Selecting `ffi`
from a k6 built without this module is an error naming the backends that
binary does have — never a silent fallback to go-iroh.

## What this backend cannot do

Read this before trusting a number from it.

| Limit | Consequence |
| --- | --- |
| No iroh-blobs or iroh-gossip surface | `blobs-fetch.js`, `blobs-collection.js` and `gossip-overlay.js` cannot use `impl: 'ffi'`; they fail immediately, naming the scenario family |
| No way to disable direct transports | `relayMode: 'forced'` is **refused**. `PresetMinimal` and `PresetN0DisableRelay` both leave direct paths enabled, so a forced cell would measure a direct path and label it relayed |
| No `context.Context`, no cancellation | Every call blocks until it returns. Stream deadlines are unsupported, so an ffi cell is bounded by the k6 iteration timeout, not by `timeoutMs` |
| One OS thread per in-flight stream | Each blocked read parks a thread inside Rust, and connection liveness needs another parked in `Closed()` |

The last two are why this backend has a concurrency ceiling. Cell A
(`ffi-peer/README.md`) measured the boundary getting *worse* as streams
were added — 298 → 208 MB/s from 4 to 16 streams, while native Rust went
329 → 426 over the same step. Read `impl: 'ffi'` results as a measurement
of the boundary, not as a load generator that can saturate a fast target:
a slow result may be the generator rather than the peer.

## Version pinning

The iroh version is decided by the archive that gets linked, not by any
Go file here. iroh-go vendors a `libiroh.a` built from *upstream's*
lockfile, so on linux a plain build silently links that older iroh
whatever `ffi-peer/libiroh/Cargo.lock` says. `make build-ffi` puts the
locally built archive ahead of it on `CGO_LDFLAGS` and then verifies what
actually got linked. Confirm it directly on any binary you measure with:

	strings ./k6 | grep -o 'iroh-1\.[0-9.]*' | sort -u

Rust embeds crate source paths, so the binary names its own dependency
versions. Record that output alongside any cell's numbers.

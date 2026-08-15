# xk6-iroh

A [k6](https://k6.io) extension that puts [iroh](https://iroh.computer)
endpoints inside virtual users, so a load test can drive QUIC connections,
streams and datagrams the way it drives HTTP.

Two implementations are available, selected per script:

| `impl` | drives | needs |
| --- | --- | --- |
| `go` | [go-iroh](https://github.com/tmc/go-iroh), pure Go | nothing beyond the Go toolchain |
| `ffi` | Rust iroh through iroh-go's uniffi/cgo bindings | a Rust toolchain and cgo |

They are separate modules on purpose. A build-tagged file inside one
module would still put iroh-go and its vendored static libraries into
`go.mod` and `go.sum` for every consumer, tag or no tag, because
`go mod tidy` resolves across build configurations.

## Build

	go install go.k6.io/xk6/cmd/xk6@latest

Pure Go, `impl: 'go'` only:

	xk6 build --with github.com/tmc/xk6-iroh

Both backends, so a script can pick at runtime:

	make build-ffi

`make build-ffi` rather than `xk6 build` directly, because the ffi
backend must link `libiroh.a` built from `ffi/libiroh/Cargo.lock`. The
archive iroh-go vendors is built from *its* lockfile, which pins an older
iroh, and it would otherwise be linked silently. The target puts the
locally built one ahead of it and then reads the version strings back out
of the resulting k6 binary, failing the build if they disagree with the
lockfile — the binary decides which iroh was measured, the lockfile only
records an intention.

## Use

```js
import iroh from 'k6/x/iroh';

const client = new iroh.Client({
    target: __ENV.TARGET_TICKET,   // endpoint ticket from the target peer
    alpn: 'perflab/0',
    impl: __ENV.IMPL || 'go',      // which iroh THIS client drives
});

export default function () {
    client.sendStreams({ streams: 16, bytes: 64 * 1024 * 1024, msgSize: 1024 });
}
```

Selecting an `impl` this binary was not built with is an error, not a
fallback to `go`: a result labeled with an implementation it did not run
is worse than no result.

See the package documentation for the full option set — endpoint scope,
relay mode, datagram echo, and socket counters that prove whether traffic
took the relay path.

## What this repository is not

It is the instrument, not the measurements. Reference peers, scenarios,
the statistical harness, and the published results live in
[go-iroh-perflab](https://github.com/tmc/go-iroh-perflab), which consumes
this module and pins the revision it measured with. Nothing here should
grow a dependency on that repository.

The `ffi` backend does not implement every capability the `go` backend
does; `ffi/README.md` lists what it cannot do, which is a shorter list to
read before trusting a number than to discover afterwards.

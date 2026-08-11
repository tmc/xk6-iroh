// Package xk6irohffi registers the "ffi" backend with the perflab k6
// extension: Go driving Rust iroh through iroh-go's uniffi/cgo bindings.
//
// It is a separate module from xk6-iroh on purpose. A build-tagged file
// inside that module would still put iroh-go and its vendored static
// libraries into its go.mod and go.sum for every consumer, tag or no
// tag, because go mod tidy resolves across build configurations. Keeping
// it here means
//
//	xk6 build --with github.com/tmc/go-iroh-perflab/xk6-iroh
//
// stays pure Go with no Rust toolchain, and a build that wants both
// implementations adds a second --with:
//
//	xk6 build --with github.com/tmc/go-iroh-perflab/xk6-iroh=./xk6-iroh \
//	          --with github.com/tmc/go-iroh-perflab/xk6-iroh-ffi=./xk6-iroh-ffi
//
// Scripts then select it with impl: 'ffi'. See README.md for what this
// backend cannot do, which is a shorter list to read before trusting a
// number than to discover afterwards.
//
// # Building
//
// Use make build-ffi rather than xk6 build directly. It builds libiroh.a
// from ffi-peer/libiroh, puts it ahead of the archive iroh-go vendors for
// linux/musl — which is pinned to an older iroh and would otherwise be
// linked silently — and then reads the version strings back out of the
// resulting k6 binary, failing the build if they disagree with
// Cargo.lock. The binary decides which iroh was measured; the lockfile
// only records an intention.
//
// On darwin the archive must be built locally in any case: see
// cgo_darwin.go for the link line and why CoreWLAN is on it.
package xk6irohffi

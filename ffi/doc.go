// Package xk6irohffi registers the "ffi" backend with the xk6-iroh k6
// extension: Go driving Rust iroh through iroh-go's uniffi/cgo bindings.
//
// It is a separate module from xk6-iroh on purpose. A build-tagged file
// inside that module would still put iroh-go and its vendored static
// libraries into its go.mod and go.sum for every consumer, tag or no
// tag, because go mod tidy resolves across build configurations. Keeping
// it here means
//
//	xk6 build --with github.com/tmc/xk6-iroh
//
// stays pure Go with no Rust toolchain, and a build that wants both
// implementations adds a second --with:
//
//	xk6 build --with github.com/tmc/xk6-iroh=. \
//	          --with github.com/tmc/xk6-iroh/ffi=./ffi
//
// Scripts then select it with impl: 'ffi'. See README.md for how this
// backend differs from the pure-Go one, and for a known liveness issue
// in the bindings.
//
// # Building
//
// On linux, make build-ffi-vendored needs no Rust toolchain: iroh-go
// vendors a libiroh.a for linux/amd64 and linux/arm64 and cgo finds it
// unaided. It is built from iroh-go's lockfile and so pins an older
// iroh than ffi/libiroh does, and the target prints what it linked.
//
// make build-ffi builds libiroh.a from ffi/libiroh instead, and is the
// only option on darwin, where nothing is vendored. It puts the local
// archive ahead of the vendored one, then reads the version strings back
// out of the resulting k6 binary and fails the build if they disagree
// with Cargo.lock: the binary decides which iroh is present, and the
// lockfile only records an intention.
//
// See cgo_darwin.go for the darwin link line and why CoreWLAN is on it.
package xk6irohffi

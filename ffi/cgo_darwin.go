package xk6irohffi

// iroh-go vendors libiroh.a for linux/musl only and its own cgo LDFLAGS
// cover just those targets, so on darwin the archive has to be built
// locally (make build-ffi, from ffi-peer/libiroh) and linked from here.
// This mirrors the line ffi-peer/main.go carries for the same reason.
//
// CoreWLAN is load-bearing rather than defensive: iroh's macOS network
// monitor looks up the CWWiFiClient ObjC class at runtime, and without
// the framework on the link line the class is never registered and
// EndpointBind panics with "class CWWiFiClient could not be found".
//
// The linker warns that the Rust objects target a newer macOS than the
// Go link step. Those warnings are benign.

// #cgo darwin LDFLAGS: -L${SRCDIR}/../ffi-peer/libiroh/target/release -liroh -lc++ -framework CoreFoundation -framework Security -framework SystemConfiguration -framework CoreWLAN
import "C"

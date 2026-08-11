// Package perflabpeer holds what the k6 client and perflab-server both
// need: the gossip topic derivation they must agree on, and the
// stream-level diagnostics either side can turn on.
//
// It exists to keep k6 out of a target binary. The parent package
// registers a k6 module and so imports the JavaScript runtime; a server
// that imported it for one constant linked all of it. Nothing here may
// import k6 or sobek.
package perflabpeer

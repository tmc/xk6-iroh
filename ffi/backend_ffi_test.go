package xk6irohffi

import (
	"context"
	"testing"

	xk6iroh "github.com/tmc/xk6-iroh"
)

// TestCounterNames checks that every name in counterNames still exists in
// a live endpoint's metrics. A rename upstream would otherwise turn the
// relay counters into silent zeroes, and those are exactly what a
// relay-forced cell is judged by.
//
// Needs the locally built libiroh (make build-ffi).
func TestCounterNames(t *testing.T) {
	ep, err := Backend{}.Bind(context.Background(), xk6iroh.BindOptions{RelayMode: "default"})
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Shutdown(context.Background())

	stats := ep.(*endpoint).ep.Stats()
	for ours, theirs := range counterNames {
		if _, ok := stats[theirs]; !ok {
			t.Errorf("counter %q maps to %q, which this iroh does not expose", ours, theirs)
		}
	}
	if got := ep.Counters(); len(got) != len(counterNames) {
		t.Errorf("Counters() returned %d entries, want %d", len(got), len(counterNames))
	}
}

// TestForcedRelayRefused pins the contract that a relay-forced bind fails
// rather than silently producing a direct path labeled as relayed.
func TestForcedRelayRefused(t *testing.T) {
	_, err := Backend{}.Bind(context.Background(), xk6iroh.BindOptions{
		RelayMode: "forced",
		RelayURL:  "http://127.0.0.1:3340",
	})
	if err == nil {
		t.Fatal("forced relay bind succeeded; it must fail while the bindings cannot disable direct transports")
	}
}

package perflab

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// stubBackend binds according to a scripted sequence of errors: the nth
// Bind returns errs[n], or succeeds once the sequence is exhausted.
type stubBackend struct {
	mu    sync.Mutex
	errs  []error
	binds int
}

func (b *stubBackend) Name() string               { return "stub" }
func (b *stubBackend) Capabilities() Capabilities { return Capabilities{} }

func (b *stubBackend) Bind(context.Context, BindOptions) (Endpoint, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.binds
	b.binds++
	if n < len(b.errs) {
		return nil, b.errs[n]
	}
	return stubEndpoint{}, nil
}

type stubEndpoint struct{ Endpoint }

// TestSharedEndpointRetriesAfterFailure is the regression test for a
// bind failure being remembered for the life of the test. The first VU
// to arrive can lose a race for a port; every VU after it must still be
// able to bind.
func TestSharedEndpointRetriesAfterFailure(t *testing.T) {
	bindFailed := errors.New("address already in use")
	backend := &stubBackend{errs: []error{bindFailed}}
	root := new(RootModule)

	if _, err := root.sharedEndpoint(context.Background(), "k", backend, BindOptions{}); !errors.Is(err, bindFailed) {
		t.Fatalf("first bind: err = %v, want %v", err, bindFailed)
	}
	ep, err := root.sharedEndpoint(context.Background(), "k", backend, BindOptions{})
	if err != nil {
		t.Fatalf("second bind: unexpected error %v", err)
	}
	if ep == nil {
		t.Fatal("second bind returned a nil endpoint")
	}
	if backend.binds != 2 {
		t.Errorf("Bind called %d times, want 2", backend.binds)
	}
}

// TestSharedEndpointBindsOnce checks the property the retry must not
// cost: concurrent VUs share one endpoint and one bind.
func TestSharedEndpointBindsOnce(t *testing.T) {
	backend := &stubBackend{}
	root := new(RootModule)

	const vus = 8
	eps := make([]Endpoint, vus)
	var wg sync.WaitGroup
	for i := range vus {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ep, err := root.sharedEndpoint(context.Background(), "k", backend, BindOptions{})
			if err != nil {
				t.Errorf("bind: %v", err)
				return
			}
			eps[i] = ep
		}()
	}
	wg.Wait()

	if backend.binds != 1 {
		t.Errorf("Bind called %d times, want 1", backend.binds)
	}
	for i, ep := range eps {
		if ep != eps[0] {
			t.Errorf("vu %d got a different endpoint than vu 0", i)
		}
	}
}

// TestSharedEndpointRejectsDifferentOptions checks that a second client
// asking for a different relay configuration is told so rather than
// handed an endpoint bound for someone else.
func TestSharedEndpointRejectsDifferentOptions(t *testing.T) {
	root := new(RootModule)
	backend := &stubBackend{}

	if _, err := root.sharedEndpoint(context.Background(), "relay=off", backend, BindOptions{}); err != nil {
		t.Fatalf("first bind: %v", err)
	}
	_, err := root.sharedEndpoint(context.Background(), "relay=forced", backend, BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Errorf("mismatched options: err = %v, want one mentioning \"already bound\"", err)
	}
}

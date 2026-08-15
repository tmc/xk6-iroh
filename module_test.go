package xk6iroh

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"go.k6.io/k6/v2/js/modulestest"
)

// newTestRuntime builds a modulestest runtime with the iroh module loaded.
func newTestRuntime(t *testing.T) *modulestest.Runtime {
	t.Helper()
	rt := modulestest.NewRuntime(t)
	if err := rt.SetupModuleSystem(map[string]any{importPath: new(RootModule)}, nil, nil); err != nil {
		t.Fatalf("setup module system: %v", err)
	}
	return rt
}

// testEndpointTicket returns a syntactically valid endpoint ticket.
func testEndpointTicket(t *testing.T) string {
	t.Helper()
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return endpointticket.Encode(netaddr.NewEndpointAddr(key.EndpointID(sk.Public())))
}

func TestClientConfigValidation(t *testing.T) {
	ticket := testEndpointTicket(t)
	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{"missing target", `{}`, "config.target is required"},
		{"bad target", `{target: "nonsense"}`, "decode target ticket"},
		{"unknown key", `{target: "` + ticket + `", endpontScope: "vu"}`, `unknown field "endpontScope"`},
		{"bad endpointScope", `{target: "` + ticket + `", endpointScope: "global"}`, "unknown endpointScope"},
		{"bad relayMode", `{target: "` + ticket + `", relayMode: "sometimes"}`, "unknown relayMode"},
		{"forced without url", `{target: "` + ticket + `", relayMode: "forced"}`, "requires relayURL"},
		{"valid", `{target: "` + ticket + `"}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := newTestRuntime(t)
			_, err := rt.RunOnEventLoop(`
				const iroh = require("` + importPath + `");
				new iroh.Client(` + tt.config + `);
			`)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestNamedExport(t *testing.T) {
	rt := newTestRuntime(t)
	v, err := rt.RunOnEventLoop(`
		const { Client } = require("` + importPath + `");
		typeof Client;
	`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := v.String(); got != "function" {
		t.Fatalf("named Client export is %q, want function", got)
	}
}

func TestBlobsTicketTarget(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	addr := netaddr.NewEndpointAddr(key.EndpointID(sk.Public()))
	ticket := blobs.NewTicket(addr, blobs.NewHash([]byte("perflab")), blobs.Raw).EncodeString()

	rt := newTestRuntime(t)
	if _, err := rt.RunOnEventLoop(`
		const iroh = require("` + importPath + `");
		new iroh.Client({target: "` + ticket + `"});
	`); err != nil {
		t.Fatalf("blobs ticket rejected: %v", err)
	}
}

func TestExportValueUnknownField(t *testing.T) {
	rt := newTestRuntime(t)
	var opts StreamOpts
	v, err := rt.RunOnEventLoop(`({streams: 4, chunkSize: 1024})`)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if err := exportValue(v, &opts); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("exportValue = %v, want unknown-field error", err)
	}
}

// TestDecodeTargetAcceptsBlobsTicket is the regression this function was
// extracted for. The client decoded a blobs ticket happily and then handed
// the same string to the backend, which understood only endpoint tickets,
// so every blobs scenario failed at dial with "wrong prefix, expected
// endpoint" -- a few lines after the ticket had been decoded successfully.
func TestDecodeTargetAcceptsBlobsTicket(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	addr := netaddr.NewEndpointAddr(key.EndpointID(sk.Public()))
	endpoint := endpointticket.Encode(addr)
	blob := blobs.NewTicket(addr, blobs.NewHash([]byte("perflab")), blobs.Raw).EncodeString()

	for _, tt := range []struct{ name, ticket string }{
		{"endpoint", endpoint},
		{"blobs", blob},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeTarget(tt.ticket)
			if err != nil {
				t.Fatalf("decodeTarget: %v", err)
			}
			if got.ID != addr.ID {
				t.Errorf("decoded a different endpoint: %v, want %v", got.ID, addr.ID)
			}
		})
	}
	if _, err := decodeTarget("nonsense"); err == nil {
		t.Error("decodeTarget accepted a string that is neither ticket")
	}
}

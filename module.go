package perflab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/grafana/sobek"
	"github.com/tmc/go-iroh/iroh"
	"go.k6.io/k6/v2/js/common"
	"go.k6.io/k6/v2/js/modules"
)

type (
	// RootModule is the per-test module state shared by all VUs. The
	// shared endpoint is never shut down explicitly; it lives until the
	// k6 process exits, which is the k6 lifecycle for RootModule state.
	RootModule struct {
		mu         sync.Mutex
		endpoint   *iroh.Endpoint // endpointScope: "shared"
		epErr      error
		epOnce     sync.Once
		epOptsKey  string            // relay options the endpoint was bound with
		lastShared map[string]uint64 // shared-endpoint socket counter baseline
		results    resultLog
	}

	// ModuleInstance is the per-VU module state.
	ModuleInstance struct {
		root    *RootModule
		vu      modules.VU
		metrics *perflabMetrics
		exports *sobek.Object
	}
)

var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &ModuleInstance{}
)

// NewModuleInstance implements modules.Module.
func (root *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	rt := vu.Runtime()
	m, err := registerMetrics(vu.InitEnv().Registry)
	if err != nil {
		common.Throw(rt, err)
	}
	root.mu.Lock()
	if root.results.lookupEnv == nil {
		root.results.lookupEnv = vu.InitEnv().LookupEnv
	}
	root.mu.Unlock()
	mi := &ModuleInstance{
		root:    root,
		vu:      vu,
		metrics: m,
		exports: rt.NewObject(),
	}

	must := func(err error) {
		if err != nil {
			common.Throw(rt, err)
		}
	}
	clientCtor := func(call sobek.ConstructorCall) *sobek.Object {
		var config Config
		if err := exportValue(call.Argument(0), &config); err != nil {
			common.Throw(rt, fmt.Errorf("invalid config: %w", err))
		}
		client, err := newClient(mi, config)
		if err != nil {
			common.Throw(rt, err)
		}
		// Option-taking methods decode their argument through
		// exportValue rather than sobek's native struct conversion:
		// the native path maps JS keys to snake_case field names, so
		// camelCase options (msgSize, timeoutMs) would be silently
		// dropped — and unknown keys must be errors, not no-ops.
		for name, fn := range map[string]any{
			"dial": client.Dial,
			"sendStreams": func(v sobek.Value) (StreamResult, error) {
				var opts StreamOpts
				if err := decodeOpts(v, &opts); err != nil {
					return StreamResult{}, err
				}
				return client.SendStreams(opts)
			},
			"echoDatagrams": func(v sobek.Value) (DatagramResult, error) {
				var opts DatagramOpts
				if err := decodeOpts(v, &opts); err != nil {
					return DatagramResult{}, err
				}
				return client.EchoDatagrams(opts)
			},
			"fetchBlob": func(v sobek.Value) (FetchResult, error) {
				var opts FetchOpts
				if err := decodeOpts(v, &opts); err != nil {
					return FetchResult{}, err
				}
				return client.FetchBlob(opts)
			},
			"gossip": func(v sobek.Value) (GossipResult, error) {
				var opts GossipOpts
				if err := decodeOpts(v, &opts); err != nil {
					return GossipResult{}, err
				}
				return client.Gossip(opts)
			},
			"request": func(v sobek.Value) (RequestResult, error) {
				var opts RequestOpts
				if err := decodeOpts(v, &opts); err != nil {
					return RequestResult{}, err
				}
				return client.Request(opts)
			},
			"metricsSnapshot": client.MetricsSnapshot,
			"close":           client.Close,
		} {
			must(call.This.DefineDataProperty(
				name,
				rt.ToValue(fn),
				sobek.FLAG_FALSE,
				sobek.FLAG_FALSE,
				sobek.FLAG_TRUE,
			))
		}
		return call.This
	}
	must(mi.exports.DefineDataProperty(
		"Client",
		rt.ToValue(clientCtor),
		sobek.FLAG_FALSE,
		sobek.FLAG_FALSE,
		sobek.FLAG_TRUE,
	))
	return mi
}

// Exports implements modules.Instance. Client is available both on the
// default export and as a named export, so both import styles work:
//
//	import iroh from 'k6/x/iroh';        new iroh.Client({...})
//	import { Client } from 'k6/x/iroh';  new Client({...})
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{
		Default: mi.exports,
		Named:   map[string]any{"Client": mi.exports.Get("Client")},
	}
}

// sharedEndpoint returns the test-wide shared endpoint, binding it on
// first use with the options of the first caller. optsKey identifies
// the caller's relay configuration; a later caller with a different
// configuration gets an error instead of a silently wrong endpoint.
func (root *RootModule) sharedEndpoint(ctx context.Context, optsKey string, options func() ([]iroh.Option, error)) (*iroh.Endpoint, error) {
	root.epOnce.Do(func() {
		root.mu.Lock()
		defer root.mu.Unlock()
		opts, err := options()
		if err != nil {
			root.epErr = err
			return
		}
		root.epOptsKey = optsKey
		root.endpoint, root.epErr = iroh.Bind(context.WithoutCancel(ctx), opts...)
		if root.epErr != nil {
			root.epErr = fmt.Errorf("bind shared endpoint: %w", root.epErr)
		}
	})
	if root.epErr == nil && optsKey != root.epOptsKey {
		return nil, fmt.Errorf("shared endpoint already bound with options %q, client wants %q", root.epOptsKey, optsKey)
	}
	return root.endpoint, root.epErr
}

// sharedSocketDelta returns the change in the shared endpoint's socket
// counters since the previous call, keeping one baseline for the whole
// test so concurrent VUs do not each re-emit the same global delta.
func (root *RootModule) sharedSocketDelta(counters map[string]uint64) map[string]uint64 {
	root.mu.Lock()
	defer root.mu.Unlock()
	delta := make(map[string]uint64, len(counters))
	for name, v := range counters {
		delta[name] = v - root.lastShared[name]
	}
	root.lastShared = counters
	return delta
}

// decodeOpts decodes an optional JS options argument: absent, null, or
// undefined leaves dst at its zero value (methods apply defaults).
func decodeOpts(v sobek.Value, dst any) error {
	if v == nil || sobek.IsUndefined(v) || sobek.IsNull(v) {
		return nil
	}
	return exportValue(v, dst)
}

// exportValue converts a JS value to a Go struct via JSON. Unknown
// keys are an error: a silently ignored option (a typo'd msgSize, say)
// would produce a benchmark that measures the wrong thing.
func exportValue(v sobek.Value, dst any) error {
	data, err := json.Marshal(v.Export())
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("unmarshal value: %w", err)
	}
	return nil
}

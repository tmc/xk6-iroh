package xk6iroh

import (
	"fmt"
	"maps"
	"strconv"
	"time"

	"go.k6.io/k6/v2/js/modules"
	"go.k6.io/k6/v2/metrics"
)

// perflabMetrics holds the custom metrics registered for the module.
type perflabMetrics struct {
	dialLatency        *metrics.Metric
	streamThroughput   *metrics.Metric
	transferThroughput *metrics.Metric
	streamCompletion   *metrics.Metric
	bytesSent          *metrics.Metric
	datagramRTT        *metrics.Metric
	datagramLoss       *metrics.Metric
	socketCounters     *metrics.Metric
	errors             *metrics.Metric
	blobBytes          *metrics.Metric
	blobThroughput     *metrics.Metric
	requestRTT         *metrics.Metric
	gossipRTT          *metrics.Metric
	gossipLoss         *metrics.Metric
}

func registerMetrics(registry *metrics.Registry) (*perflabMetrics, error) {
	m := &perflabMetrics{}
	for _, def := range []struct {
		metric    **metrics.Metric
		name      string
		typ       metrics.MetricType
		valueType metrics.ValueType
	}{
		{&m.dialLatency, "iroh_dial_latency", metrics.Trend, metrics.Time},
		{&m.streamThroughput, "iroh_stream_throughput", metrics.Trend, metrics.Default},
		{&m.transferThroughput, "iroh_transfer_throughput", metrics.Trend, metrics.Default},
		{&m.streamCompletion, "iroh_stream_completion", metrics.Rate, metrics.Default},
		{&m.bytesSent, "iroh_bytes_sent", metrics.Counter, metrics.Data},
		{&m.datagramRTT, "iroh_datagram_rtt", metrics.Trend, metrics.Time},
		{&m.datagramLoss, "iroh_datagram_loss", metrics.Rate, metrics.Default},
		{&m.socketCounters, "iroh_socket_counters", metrics.Counter, metrics.Default},
		{&m.errors, "iroh_errors", metrics.Counter, metrics.Default},
		{&m.blobBytes, "iroh_blob_bytes", metrics.Counter, metrics.Data},
		{&m.blobThroughput, "iroh_blob_throughput", metrics.Trend, metrics.Default},
		{&m.requestRTT, "iroh_request_rtt", metrics.Trend, metrics.Time},
		{&m.gossipRTT, "iroh_gossip_rtt", metrics.Trend, metrics.Time},
		{&m.gossipLoss, "iroh_gossip_loss", metrics.Rate, metrics.Default},
	} {
		metric, err := registry.NewMetric(def.name, def.typ, def.valueType)
		if err != nil {
			return nil, fmt.Errorf("register %s: %w", def.name, err)
		}
		*def.metric = metric
	}
	return m, nil
}

// push emits one sample for metric with the VU's current tags plus extra
// tag pairs. It is a no-op outside of VU code (nil state).
func (m *perflabMetrics) push(vu modules.VU, metric *metrics.Metric, value float64, extra map[string]string) {
	state := vu.State()
	if state == nil {
		return
	}
	ctm := state.Tags.GetCurrentValues()
	tags := ctm.Tags
	for k, v := range extra {
		tags = tags.With(k, v)
	}
	metrics.PushIfNotDone(vu.Context(), state.Samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: metric, Tags: tags},
		Time:       time.Now(),
		Metadata:   ctm.Metadata,
		Value:      value,
	})
}

// transferTags builds the standard tag set for stream transfer samples.
func transferTags(streams, msgSize int) map[string]string {
	return map[string]string{
		"streams":  strconv.Itoa(streams),
		"msg_size": strconv.Itoa(msgSize),
	}
}

// withStep copies tags and adds the load-schedule stage, so k6's own
// percentiles can be taken per step rather than over a whole ramp. A
// zero step adds nothing: the constant-load scenarios keep exactly the
// tag set they had before steps existed.
//
// Cardinality is the script's responsibility. A ramp declares its
// stages up front (8-10 of them), which is the same order as the
// streams and msg_size tags already carried here; a step named from a
// timestamp or an iteration counter would not be.
func withStep(tags map[string]string, step Step) map[string]string {
	if step.Name == "" {
		return tags
	}
	out := make(map[string]string, len(tags)+1)
	maps.Copy(out, tags)
	out["step"] = step.Name
	return out
}

// withStage copies tags and adds the error stage. Stages are a small
// fixed set (dial, open, write, close, drain) to keep tag cardinality low.
func withStage(tags map[string]string, stage string) map[string]string {
	out := make(map[string]string, len(tags)+1)
	maps.Copy(out, tags)
	out["stage"] = stage
	return out
}

package perflab

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

// resultLine is one per-transfer record in the JSONL schema shared with
// go-iroh's benchmark harness: rung, lang, sample, bytes, and duration_ns
// are the fields a Mann-Whitney analyzer over this corpus requires;
// flow_bytes and flow_duration_ns feed its fairness metric; the remaining
// fields are perflab provenance that such an analyzer ignores.
type resultLine struct {
	Rung           string  `json:"rung"`
	Lang           string  `json:"lang"`
	Sample         int     `json:"sample"`
	Bytes          int64   `json:"bytes"`
	DurationNS     int64   `json:"duration_ns"`
	FlowBytes      []int64 `json:"flow_bytes,omitempty"`
	FlowDurationNS []int64 `json:"flow_duration_ns,omitempty"`

	Schema   string `json:"schema"`
	Scenario string `json:"scenario"`
	Peer     string `json:"peer"`
	Impl     string `json:"impl"`

	// Step names the stage of a load schedule this sample belongs to,
	// and OfferedRate is the arrival rate that stage demanded. They are
	// empty for the constant-load scenarios, which have one stage.
	//
	// A ramp reports delivered load against offered, so the offered
	// value has to travel with the sample rather than be reconstructed
	// afterwards. Time bucketing cannot do it: an iteration that starts
	// in one stage and ends in the next belongs to the stage that
	// demanded it, which only the caller knows. Step also enters Rung
	// below, so cmd/perflab-compare treats two stages the way it treats
	// two builds -- as things that must not pool.
	Step        string  `json:"step,omitempty"`
	OfferedRate float64 `json:"offered_rate,omitempty"`

	// GoIroh is a property of the CLIENT: it is read from this k6
	// binary's own build info, and the client is go-iroh in every cell.
	GoIroh   string `json:"go_iroh"`
	RustIroh string `json:"rust_iroh,omitempty"`

	// The sink axis. A cell varies the client or the sink, and until
	// these existed the corpus could not say which: an A/B that rebuilt
	// both from one checkout moved them together and reported the pair
	// under a single name. The client cannot observe the sink -- it is
	// another process -- so the runner declares it, and compare treats a
	// declaration that disagrees within an arm the same way it treats
	// two go-iroh builds pooled into one.
	SinkImpl    string `json:"sink_impl,omitempty"`
	SinkVersion string `json:"sink_version,omitempty"`
	// SinkReadBuf is recorded because CONFOUNDS.md requires any cell
	// claiming a receive-path delta to state its read size, and a cell
	// that does not carry it cannot make that claim later.
	SinkReadBuf int `json:"sink_read_buf,omitempty"`
	// SinkTokio is the tokio runtime flavor for a Rust-backed sink. It
	// is empty for a Go sink, which has no such knob.
	SinkTokio string `json:"sink_tokio,omitempty"`
	// SinkFlowControl declares whether the sink's QUIC receive windows
	// were left at the stack's shipped defaults or matched across
	// implementations. The two are different experiments -- "how do the
	// stacks perform as shipped" and "how do the receive paths compare
	// at equal windows" -- and their samples must never pool. The
	// windows are unmatched today and 2.4x apart at the stream level,
	// unbounded vs 15 MiB at the connection level; see CONFOUNDS.md.
	SinkFlowControl string `json:"sink_flow_control,omitempty"`

	Host      string `json:"host"`
	Seed      string `json:"seed,omitempty"`
	Timestamp string `json:"ts"`
}

// resultLog appends unified-schema JSONL lines to the file named by the
// PERFLAB_JSONL environment variable. It is shared by all VUs.
type resultLog struct {
	mu      sync.Mutex
	f       *os.File
	err     error
	opened  bool
	samples map[string]int // per (rung, lang) sample counter

	// lookupEnv reads configuration through k6's init environment so
	// values honor k6 run --env; nil falls back to the process env.
	lookupEnv func(string) (string, bool)

	scenario string
	seed     string
	rustIroh string
	goIroh   string
	host     string

	sinkImpl    string
	sinkVersion string
	sinkReadBuf int
	sinkTokio   string

	sinkFlowControl string
}

// open lazily opens the JSONL file; it reports false when JSONL output
// is not configured.
func (l *resultLog) open() bool {
	if l.opened {
		return l.f != nil
	}
	l.opened = true
	path := l.env("PERFLAB_JSONL")
	if path == "" {
		return false
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		l.err = fmt.Errorf("open PERFLAB_JSONL: %w", err)
		fmt.Fprintln(os.Stderr, "perflab:", l.err)
		return false
	}
	l.f = f
	l.samples = make(map[string]int)
	l.scenario = "adhoc"
	if v := l.env("PERFLAB_SCENARIO"); v != "" {
		l.scenario = v
	}
	l.seed = l.env("PERFLAB_SEED")
	l.rustIroh = l.env("RUST_IROH_VERSION")
	l.goIroh = goIrohVersion()
	// The sink runs in another process, so its identity is declared by
	// whoever started it rather than observed here. Absent stays absent:
	// an unset sink axis records nothing, which compare reads as "not
	// stated" -- unlike a guessed default, which would read as a fact.
	l.sinkImpl = l.env("PERFLAB_SINK_IMPL")
	l.sinkVersion = l.env("PERFLAB_SINK_VERSION")
	l.sinkReadBuf, _ = strconv.Atoi(l.env("PERFLAB_SINK_READ_BUF"))
	l.sinkTokio = l.env("PERFLAB_SINK_TOKIO")
	l.sinkFlowControl = l.env("PERFLAB_SINK_FLOW_CONTROL")
	host, _ := os.Hostname()
	l.host = fmt.Sprintf("%s/%s/%s", runtime.GOOS, runtime.GOARCH, host)
	return true
}

func (l *resultLog) env(key string) string {
	if l.lookupEnv != nil {
		v, _ := l.lookupEnv(key)
		return v
	}
	return os.Getenv(key)
}

// goIrohCommit is stamped at link time by the Makefile with the commit
// the go-iroh checkout was at:
//
//	-ldflags "-X github.com/tmc/go-iroh-perflab/xk6-iroh.goIrohCommit=$sha"
//
// A module build needs no stamp -- the pseudo-version already carries the
// commit -- but a replace directive erases it, and a replace is how every
// A/B pair is built.
var goIrohCommit string

// goIrohVersion reports which go-iroh produced the run.
//
// A replaced module reports version "(devel)" and a filesystem path, so
// the build info alone identifies an A/B arm only by the directory
// someone happened to build it in -- and those directories outlive
// neither the session nor the comparison. Without a stamp this says
// UNSTAMPED rather than printing the path, because a comparison whose
// arms cannot be named is not a comparison, and a plausible-looking
// string is worse than an admitted gap.
func goIrohVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range bi.Deps {
			if dep.Path == "github.com/tmc/go-iroh" {
				return irohVersion(dep, goIrohCommit)
			}
		}
	}
	return irohVersion(nil, goIrohCommit)
}

// irohVersion is the decision goIrohVersion makes, separated from the
// build info it reads so the cases a single binary cannot exhibit are
// still testable.
func irohVersion(dep *debug.Module, stamp string) string {
	switch {
	case dep == nil:
		if stamp != "" {
			return stamp
		}
		return "unknown"
	case dep.Replace != nil:
		if stamp != "" {
			return stamp
		}
		return fmt.Sprintf("UNSTAMPED (replace %s)", dep.Replace.Path)
	case stamp != "" && !strings.Contains(dep.Version, stamp):
		// The stamp and the module disagree about what was built.
		// Report both rather than picking one.
		return fmt.Sprintf("%s (stamped %s)", dep.Version, stamp)
	default:
		return dep.Version
	}
}

// cellName maps a peer label to its comparison-cell name: the client
// side is always the go-iroh k6 extension, so "go" and "rust" name the
// GG/GR cells. Any other label (e.g. an A/B pair like "base" and
// "perfopt" comparing two go-iroh builds) passes through verbatim so
// cmd/perflab-compare can pair the arms.
func cellName(peer string) string {
	switch peer {
	case "go":
		return "gg"
	case "rust":
		return "gr"
	}
	return peer
}

// newLine fills the fields every record shares: the cell, the
// provenance and the run's identity. The caller supplies the shape.
// l.mu must be held.
func (l *resultLog) newLine(peer, impl, rung string, step Step) resultLine {
	if step.Name != "" {
		// The step is part of the cell name, not a column beside it: a
		// rung that pooled two stages of a ramp would report the mean
		// of a curve, which is the one number a ramp exists to avoid.
		rung += "-step-" + step.Name
	}
	line := resultLine{
		Rung: rung,
		// lang names the perflab CELL (gg = go client vs go peer,
		// gr = go client vs rust peer), deliberately distinct from the
		// native corpus values ("go", "rust") used by go-iroh's
		// benchmark harness, so the two can
		// never be pooled as if gr were Rust-native.
		Lang:     cellName(peer),
		Schema:   "perflab/1",
		Scenario: l.scenario,
		Peer:     peer,
		Impl:     impl,

		Step:        step.Name,
		OfferedRate: step.OfferedRate,

		GoIroh:   l.goIroh,
		RustIroh: l.rustIroh,

		SinkImpl:        l.sinkImpl,
		SinkVersion:     l.sinkVersion,
		SinkReadBuf:     l.sinkReadBuf,
		SinkTokio:       l.sinkTokio,
		SinkFlowControl: l.sinkFlowControl,

		Host:      l.host,
		Seed:      l.seed,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	key := line.Rung + "\x00" + line.Lang
	l.samples[key]++
	line.Sample = l.samples[key]
	return line
}

func (l *resultLog) write(line resultLine) {
	b, err := json.Marshal(line)
	if err != nil {
		fmt.Fprintln(os.Stderr, "perflab: marshal result line:", err)
		return
	}
	if _, err := l.f.Write(append(b, '\n')); err != nil {
		fmt.Fprintln(os.Stderr, "perflab: write result line:", err)
	}
}

// recordTransfer appends one JSONL line for a completed sendStreams
// fan-out. It is a no-op unless PERFLAB_JSONL is set.
func (root *RootModule) recordTransfer(peer, impl string, opts StreamOpts, res StreamResult, wall time.Duration, outcomes []streamOutcome) {
	l := &root.results
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.open() {
		return
	}
	rung := fmt.Sprintf("perflab-%s-streams-%d-msg-%d", l.scenario, opts.Streams, opts.MsgSize)
	line := l.newLine(peer, impl, rung, opts.Step)
	line.Bytes = res.BytesSent
	line.DurationNS = wall.Nanoseconds()
	for _, o := range outcomes {
		if o.err == nil && o.sent > 0 && o.duration > 0 {
			line.FlowBytes = append(line.FlowBytes, o.sent)
			line.FlowDurationNS = append(line.FlowDurationNS, o.duration.Nanoseconds())
		}
	}
	l.write(line)
}

// recordRequest appends one JSONL line for a completed request round
// trip. It is a no-op unless PERFLAB_JSONL is set.
//
// Requests were absent from the corpus until the ramp needed them: the
// rpc shape is one of the three the ramp runs, and a shape that emits
// no lines cannot be compared by cmd/perflab-compare at all. Bytes is
// the request payload and duration_ns the open-to-EOF round trip, so a
// line here means the same thing it does for a transfer -- this many
// bytes moved, in this long -- and the two rungs never pool because
// the shape is in the rung name.
func (root *RootModule) recordRequest(peer, impl string, opts RequestOpts, res RequestResult, rtt time.Duration) {
	l := &root.results
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.open() {
		return
	}
	rung := fmt.Sprintf("perflab-%s-request-%d", l.scenario, opts.Bytes)
	line := l.newLine(peer, impl, rung, opts.Step)
	line.Bytes = res.Sent
	line.DurationNS = rtt.Nanoseconds()
	l.write(line)
}

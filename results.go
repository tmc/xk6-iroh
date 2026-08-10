package perflab

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

// resultLine is one per-transfer record in the unified perfbench JSONL
// schema: rung, lang, sample, bytes, and duration_ns are the fields the
// Mann-Whitney analyzer requires; flow_bytes and flow_duration_ns feed
// its fairness metric; the remaining fields are perflab provenance that
// the analyzer ignores.
type resultLine struct {
	Rung           string  `json:"rung"`
	Lang           string  `json:"lang"`
	Sample         int     `json:"sample"`
	Bytes          int64   `json:"bytes"`
	DurationNS     int64   `json:"duration_ns"`
	FlowBytes      []int64 `json:"flow_bytes,omitempty"`
	FlowDurationNS []int64 `json:"flow_duration_ns,omitempty"`

	Schema    string `json:"schema"`
	Scenario  string `json:"scenario"`
	Peer      string `json:"peer"`
	GoIroh    string `json:"go_iroh"`
	RustIroh  string `json:"rust_iroh,omitempty"`
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

// goIrohVersion reports the go-iroh module version baked into the
// binary, including any replace directive target.
func goIrohVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range bi.Deps {
		if dep.Path != "github.com/tmc/go-iroh" {
			continue
		}
		if dep.Replace != nil {
			return fmt.Sprintf("%s (replace %s@%s)", dep.Version, dep.Replace.Path, dep.Replace.Version)
		}
		return dep.Version
	}
	return "unknown"
}

// cellName maps a peer label to its comparison-cell name: the client
// side is always the go-iroh k6 extension, so "go" and "rust" name the
// GG/GR cells. Any other label (e.g. an A/B pair like "base" and
// "perfopt" comparing two go-iroh builds) passes through verbatim so
// analysis/compare can pair the arms.
func cellName(peer string) string {
	switch peer {
	case "go":
		return "gg"
	case "rust":
		return "gr"
	}
	return peer
}

// recordTransfer appends one JSONL line for a completed sendStreams
// fan-out. It is a no-op unless PERFLAB_JSONL is set.
func (root *RootModule) recordTransfer(peer string, opts StreamOpts, res StreamResult, wall time.Duration, outcomes []streamOutcome) {
	l := &root.results
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.open() {
		return
	}
	line := resultLine{
		Rung: fmt.Sprintf("perflab-%s-streams-%d-msg-%d", l.scenario, opts.Streams, opts.MsgSize),
		// lang names the perflab CELL (gg = go client vs go peer,
		// gr = go client vs rust peer), deliberately distinct from the
		// perfbench native corpus values ("go", "rust") so the two can
		// never be pooled as if gr were Rust-native.
		Lang:      cellName(peer),
		Bytes:     res.BytesSent,
		Schema:    "perflab/1",
		Scenario:  l.scenario,
		Peer:      peer,
		GoIroh:    l.goIroh,
		RustIroh:  l.rustIroh,
		Host:      l.host,
		Seed:      l.seed,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	line.DurationNS = wall.Nanoseconds()
	key := line.Rung + "\x00" + line.Lang
	l.samples[key]++
	line.Sample = l.samples[key]
	for _, o := range outcomes {
		if o.err == nil && o.sent > 0 && o.duration > 0 {
			line.FlowBytes = append(line.FlowBytes, o.sent)
			line.FlowDurationNS = append(line.FlowDurationNS, o.duration.Nanoseconds())
		}
	}
	b, err := json.Marshal(line)
	if err != nil {
		fmt.Fprintln(os.Stderr, "perflab: marshal result line:", err)
		return
	}
	if _, err := l.f.Write(append(b, '\n')); err != nil {
		fmt.Fprintln(os.Stderr, "perflab: write result line:", err)
	}
}

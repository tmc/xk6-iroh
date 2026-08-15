package xk6iroh

import (
	"runtime/debug"
	"strings"
	"testing"
)

func TestIrohVersion(t *testing.T) {
	replaced := &debug.Module{
		Path:    "github.com/tmc/go-iroh",
		Version: "v0.0.0-20260809024910-c57a8efa9fba",
		Replace: &debug.Module{Path: "/tmp/scratch/go-iroh", Version: "(devel)"},
	}
	module := &debug.Module{
		Path:    "github.com/tmc/go-iroh",
		Version: "v0.0.0-20260809024910-c57a8efa9fba",
	}
	tests := []struct {
		name  string
		dep   *debug.Module
		stamp string
		want  string
	}{
		{"module build", module, "", "v0.0.0-20260809024910-c57a8efa9fba"},
		{"module build, agreeing stamp", module, "c57a8efa9fba", "v0.0.0-20260809024910-c57a8efa9fba"},
		{"module build, disagreeing stamp", module, "b940113", "v0.0.0-20260809024910-c57a8efa9fba (stamped b940113)"},
		{"replace, stamped", replaced, "b940113bd0d1", "b940113bd0d1"},
		{"replace, dirty stamp", replaced, "b940113bd0d1-dirty", "b940113bd0d1-dirty"},
		{"replace, unstamped", replaced, "", "UNSTAMPED (replace /tmp/scratch/go-iroh)"},
		{"no dependency", nil, "", "unknown"},
		{"no dependency, stamped", nil, "b940113", "b940113"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := irohVersion(tt.dep, tt.stamp); got != tt.want {
				t.Errorf("irohVersion(%v, %q) = %q, want %q", tt.dep, tt.stamp, got, tt.want)
			}
		})
	}
}

// An unstamped replace build must not report the build directory as if
// it identified the code: the path is ephemeral, and an A/B pair whose
// arms cannot be named is not a comparison.
func TestIrohVersionNeverReportsPathAsVersion(t *testing.T) {
	dep := &debug.Module{
		Path:    "github.com/tmc/go-iroh",
		Version: "v0.0.0-20260809024910-c57a8efa9fba",
		Replace: &debug.Module{Path: "/tmp/scratch/go-iroh-b940113", Version: "(devel)"},
	}
	got := irohVersion(dep, "")
	if !strings.Contains(got, "UNSTAMPED") {
		t.Errorf("irohVersion(replace, no stamp) = %q, want it to admit the gap", got)
	}
}

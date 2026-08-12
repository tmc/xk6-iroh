package perflabpeer

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"syscall"
	"time"
)

// A CPULog samples the peer process's own CPU consumption on an
// interval and appends one JSON object per sample.
//
// Throughput alone does not discriminate between implementations: two
// sinks can absorb the same bytes per second while one spends twice the
// CPU doing it, and the cheaper one is the one with headroom. The pair
// is what the comparison needs, and the load side of it is already
// recorded by the client's corpus -- this is the missing half, and it
// has to be sampled here because the sink runs in another process that
// the client cannot observe.
//
// Samples are timestamped and carry the delta over the interval rather
// than a running total, so an analyzer can align them to a load
// schedule by wall clock. That alignment is sound for CPU in a way it
// would not be for an iteration: CPU is a rate over a window, not a
// unit of work that belongs to whichever stage demanded it.
//
// The zero CPULog is not usable; use NewCPULog. A nil *CPULog is, and
// does nothing, so a caller that did not enable logging needs no
// branch.
type CPULog struct {
	f        *os.File
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
}

// cpuSample is one interval's consumption. The delta fields are what an
// analyzer reads; the cumulative ones are kept so a dropped or delayed
// sample cannot silently lose CPU time from the total.
type cpuSample struct {
	Timestamp string `json:"ts"`
	// ElapsedNS is wall time since the previous sample, which is the
	// denominator for CPUFrac. It is measured rather than assumed equal
	// to the interval: a loaded host delivers a late tick, and dividing
	// by the nominal interval there would overstate utilization.
	ElapsedNS int64 `json:"elapsed_ns"`
	UserNS    int64 `json:"user_ns"`
	SysNS     int64 `json:"sys_ns"`
	// CPUFrac is (user+sys)/elapsed, so 1.0 is one core saturated and
	// values above 1.0 are normal on a multicore host.
	CPUFrac float64 `json:"cpu_frac"`

	CumUserNS int64 `json:"cum_user_ns"`
	CumSysNS  int64 `json:"cum_sys_ns"`

	// Goroutines is a cheap leak signal over a soak. It is not a CPU
	// figure and is not part of CPUFrac.
	Goroutines int `json:"goroutines"`
}

// NewCPULog starts sampling into path, appending, at the given
// interval. An empty path disables logging and returns a nil *CPULog,
// which is safe to Close.
func NewCPULog(path string, interval time.Duration) (*CPULog, error) {
	if path == "" {
		return nil, nil
	}
	if interval <= 0 {
		return nil, fmt.Errorf("cpu log interval must be positive, got %v", interval)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open cpu log: %w", err)
	}
	c := &CPULog{
		f:        f,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go c.run()
	return c, nil
}

func (c *CPULog) run() {
	defer close(c.done)
	enc := json.NewEncoder(c.f)
	t := time.NewTicker(c.interval)
	defer t.Stop()

	lastUser, lastSys := cpuTimes()
	last := time.Now()
	for {
		select {
		case <-c.stop:
			return
		case now := <-t.C:
			user, sys := cpuTimes()
			elapsed := now.Sub(last)
			s := cpuSample{
				Timestamp:  now.UTC().Format(time.RFC3339Nano),
				ElapsedNS:  elapsed.Nanoseconds(),
				UserNS:     user - lastUser,
				SysNS:      sys - lastSys,
				CumUserNS:  user,
				CumSysNS:   sys,
				Goroutines: runtime.NumGoroutine(),
			}
			if elapsed > 0 {
				s.CPUFrac = float64(s.UserNS+s.SysNS) / float64(elapsed.Nanoseconds())
			}
			if err := enc.Encode(&s); err != nil {
				fmt.Fprintln(os.Stderr, "perflab: write cpu log:", err)
				return
			}
			lastUser, lastSys, last = user, sys, now
		}
	}
}

// Close stops sampling and closes the file. It is safe on a nil
// *CPULog.
func (c *CPULog) Close() error {
	if c == nil {
		return nil
	}
	close(c.stop)
	<-c.done
	return c.f.Close()
}

// cpuTimes reports this process's cumulative user and system CPU time.
// getrusage is used rather than reading /proc so the same code serves
// darwin and linux, which matters because the two hosts disagree about
// this measurement and comparing them requires it be taken the same
// way. It cannot fail for RUSAGE_SELF on either.
func cpuTimes() (user, sys int64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	return ru.Utime.Nano(), ru.Stime.Nano()
}

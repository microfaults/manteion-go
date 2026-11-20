package demo

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// The environment variables the demo reads. No other setting of the demo has
// one; the rest are fixed defaults that tests override by building a Config.
const (
	EnvAddr       = "MANTEION_DEMO_ADDR"
	EnvEdgeSecret = "MANTEION_DEMO_EDGE_SECRET"
	EnvRunCap     = "MANTEION_DEMO_RUN_CAP"
)

// Bounds of the edge secret's length, in bytes.
const (
	minEdgeSecret = 32
	maxEdgeSecret = 512
)

// Config is the demo's configuration. Durations and counts come from here so
// tests can run with milliseconds; ConfigFromEnv is the only constructor that
// checks bounds.
type Config struct {
	// Addr is the demo listener's address (MANTEION_DEMO_ADDR).
	Addr string
	// EdgeSecret is what POST /runs must carry in X-Demo-Edge-Secret
	// (MANTEION_DEMO_EDGE_SECRET). It redacts itself wherever it is printed,
	// logged or encoded; check a request with EdgeSecret.Matches.
	EdgeSecret Secret

	RunCap       time.Duration // watchdog from pickup to forced end (MANTEION_DEMO_RUN_CAP)
	StatusPoll   time.Duration // how often the worker reads the running experiment
	SampleEvery  time.Duration // how often the live metrics are sampled
	QueryTimeout time.Duration // deadline of each Prometheus query and health probe
	Heartbeat    time.Duration // event-stream heartbeat comment interval
	StreamMaxAge time.Duration // an event stream is closed after this long
	HealthTTL    time.Duration // how long a health answer is cached

	Capacity int // runs queued or running at once, the running one included
	Retain   int // finished demo experiments kept
	Recent   int // runs listed by GET /runs/recent
}

// DefaultConfig returns the defaults, without an edge secret.
func DefaultConfig() Config {
	return Config{
		Addr:         ":8090",
		RunCap:       5 * time.Minute,
		StatusPoll:   time.Second,
		SampleEvery:  2 * time.Second,
		QueryTimeout: 1500 * time.Millisecond,
		Heartbeat:    15 * time.Second,
		StreamMaxAge: 10 * time.Minute,
		HealthTTL:    5 * time.Second,
		Capacity:     3,
		Retain:       20,
		Recent:       10,
	}
}

// ConfigFromEnv reads the demo's environment through getenv (os.Getenv in
// production) over DefaultConfig. MANTEION_DEMO_EDGE_SECRET is required. An
// error names the variable at fault and never contains the secret.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("demo config: no environment to read")
	}
	cfg := DefaultConfig()
	if v := getenv(EnvAddr); v != "" {
		cfg.Addr = v
	}
	cfg.EdgeSecret = NewSecret(getenv(EnvEdgeSecret))
	if v := getenv(EnvRunCap); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("demo config: %s=%q is not a duration", EnvRunCap, v)
		}
		cfg.RunCap = d
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate checks every field against sane bounds; an error names the
// environment variable, or Config.<Field> for a field that has none.
func (c Config) validate() error {
	if err := checkAddr(c.Addr); err != nil {
		return err
	}
	if err := checkEdgeSecret(c.EdgeSecret.value()); err != nil {
		return err
	}
	durations := []struct {
		name     string
		v        time.Duration
		min, max time.Duration
	}{
		{EnvRunCap, c.RunCap, 30 * time.Second, 15 * time.Minute},
		{"Config.StatusPoll", c.StatusPoll, 100 * time.Millisecond, 30 * time.Second},
		{"Config.SampleEvery", c.SampleEvery, 500 * time.Millisecond, time.Minute},
		{"Config.QueryTimeout", c.QueryTimeout, 100 * time.Millisecond, 10 * time.Second},
		{"Config.Heartbeat", c.Heartbeat, time.Second, 5 * time.Minute},
		{"Config.StreamMaxAge", c.StreamMaxAge, time.Minute, time.Hour},
		{"Config.HealthTTL", c.HealthTTL, time.Second, 5 * time.Minute},
	}
	for _, d := range durations {
		if d.v < d.min || d.v > d.max {
			return fmt.Errorf("demo config: %s=%s is out of range [%s, %s]", d.name, d.v, d.min, d.max)
		}
	}
	if c.QueryTimeout >= c.SampleEvery {
		return fmt.Errorf("demo config: Config.QueryTimeout=%s must be shorter than Config.SampleEvery=%s", c.QueryTimeout, c.SampleEvery)
	}
	counts := []struct {
		name     string
		v        int
		min, max int
	}{
		{"Config.Capacity", c.Capacity, 1, 10},
		{"Config.Retain", c.Retain, 1, 1000},
		{"Config.Recent", c.Recent, 1, 100},
	}
	for _, n := range counts {
		if n.v < n.min || n.v > n.max {
			return fmt.Errorf("demo config: %s=%d is out of range [%d, %d]", n.name, n.v, n.min, n.max)
		}
	}
	if c.Recent > c.Retain {
		return fmt.Errorf("demo config: Config.Recent=%d must not exceed Config.Retain=%d", c.Recent, c.Retain)
	}
	return nil
}

// checkAddr accepts host:port with a numeric port in 1..65535 (host may be
// empty: ":8090").
func checkAddr(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil || strings.ContainsAny(addr, " \t\r\n") {
		return fmt.Errorf("demo config: %s=%q is not host:port", EnvAddr, addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("demo config: %s=%q needs a numeric port in 1..65535", EnvAddr, addr)
	}
	return nil
}

// checkEdgeSecret enforces the secret's shape. Its errors never contain the
// value, not even in part.
func checkEdgeSecret(s string) error {
	switch {
	case s == "":
		return fmt.Errorf("demo config: %s is required", EnvEdgeSecret)
	case strings.TrimSpace(s) != s:
		return fmt.Errorf("demo config: %s must not start or end with whitespace", EnvEdgeSecret)
	case len(s) < minEdgeSecret:
		return fmt.Errorf("demo config: %s must be at least %d characters", EnvEdgeSecret, minEdgeSecret)
	case len(s) > maxEdgeSecret:
		return fmt.Errorf("demo config: %s must be at most %d characters", EnvEdgeSecret, maxEdgeSecret)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return fmt.Errorf("demo config: %s must be printable ASCII without spaces (it travels in an HTTP header)", EnvEdgeSecret)
		}
	}
	return nil
}

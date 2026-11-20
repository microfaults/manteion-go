package demo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// goodSecret is 40 visible ASCII characters.
const goodSecret = "s3cr3t-edge-value-0123456789-abcdefghijk"

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestConfigFromEnv(t *testing.T) {
	cfg, err := ConfigFromEnv(envOf(map[string]string{"MANTEION_DEMO_EDGE_SECRET": goodSecret}))
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	want := Config{
		Addr:   ":8090",
		RunCap: 5 * time.Minute, StatusPoll: time.Second, SampleEvery: 2 * time.Second,
		QueryTimeout: 1500 * time.Millisecond, Heartbeat: 15 * time.Second, StreamMaxAge: 10 * time.Minute,
		HealthTTL: 5 * time.Second, Capacity: 3, Retain: 20, Recent: 10,
	}
	if !cfg.EdgeSecret.Matches(goodSecret) {
		t.Errorf("the edge secret was not kept")
	}
	got := cfg
	got.EdgeSecret = Secret{}
	if got != want {
		t.Errorf("defaults = %v; want %v", got, want)
	}

	_, err = ConfigFromEnv(envOf(nil))
	if err == nil || !strings.Contains(err.Error(), "MANTEION_DEMO_EDGE_SECRET") {
		t.Errorf("empty secret: err = %v; want an error naming MANTEION_DEMO_EDGE_SECRET", err)
	}

	_, err = ConfigFromEnv(envOf(map[string]string{"MANTEION_DEMO_EDGE_SECRET": goodSecret, "MANTEION_DEMO_RUN_CAP": "0s"}))
	if err == nil || !strings.Contains(err.Error(), "MANTEION_DEMO_RUN_CAP") {
		t.Errorf("run cap 0s: err = %v; want an error naming MANTEION_DEMO_RUN_CAP", err)
	}

	cfg, err = ConfigFromEnv(envOf(map[string]string{"MANTEION_DEMO_EDGE_SECRET": goodSecret, "MANTEION_DEMO_RUN_CAP": "60s"}))
	if err != nil || cfg.RunCap != 60*time.Second {
		t.Errorf("run cap 60s: cfg.RunCap = %v, err = %v; want 60s", cfg.RunCap, err)
	}

	cfg, err = ConfigFromEnv(envOf(map[string]string{"MANTEION_DEMO_EDGE_SECRET": goodSecret, "MANTEION_DEMO_ADDR": "127.0.0.1:9191"}))
	if err != nil || cfg.Addr != "127.0.0.1:9191" {
		t.Errorf("addr: cfg.Addr = %q, err = %v; want 127.0.0.1:9191", cfg.Addr, err)
	}

	if _, err := ConfigFromEnv(nil); err == nil {
		t.Errorf("nil getenv: want an error")
	}
}

func TestConfigFromEnv_EdgeSecret(t *testing.T) {
	cases := []struct {
		name   string
		secret string
		ok     bool
	}{
		{"exactly 32 characters", strings.Repeat("a", 32), true},
		{"512 characters", strings.Repeat("b", 512), true},
		{"empty", "", false},
		{"31 characters", strings.Repeat("c", 31), false},
		{"leading space", " " + goodSecret, false},
		{"trailing newline", goodSecret + "\n", false},
		{"trailing tab", goodSecret + "\t", false},
		{"only whitespace", strings.Repeat(" ", 40), false},
		{"inner space", "abcdefghijklmnop qrstuvwxyz0123456789", false},
		{"non-ASCII", "abcdefghijklmnopqrstuvwxyz0123456789é", false},
		{"control character", "abcdefghijklmnopqrstuvwxyz\x000123456789", false},
		{"513 characters", strings.Repeat("d", 513), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ConfigFromEnv(envOf(map[string]string{"MANTEION_DEMO_EDGE_SECRET": tc.secret}))
			if tc.ok {
				if err != nil || !cfg.EdgeSecret.Matches(tc.secret) {
					t.Fatalf("err = %v; want the secret accepted", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("accepted; want an error")
			}
			msg := err.Error()
			if !strings.Contains(msg, "MANTEION_DEMO_EDGE_SECRET") {
				t.Errorf("error %q does not name the variable", msg)
			}
			if trimmed := strings.TrimSpace(tc.secret); len(trimmed) >= 8 && strings.Contains(msg, trimmed[:8]) {
				t.Errorf("error %q echoes the secret", msg)
			}
			if cfg != (Config{}) {
				t.Errorf("config returned with the error: %+v", cfg)
			}
		})
	}
}

func TestConfigFromEnv_RunCapBounds(t *testing.T) {
	cases := map[string]bool{
		"30s": true, "60s": true, "5m": true, "15m": true,
		"29s": false, "15m1s": false, "0s": false, "-1m": false, "abc": false, " 5m": false, "1h": false,
	}
	for v, ok := range cases {
		_, err := ConfigFromEnv(envOf(map[string]string{"MANTEION_DEMO_EDGE_SECRET": goodSecret, "MANTEION_DEMO_RUN_CAP": v}))
		if ok && err != nil {
			t.Errorf("MANTEION_DEMO_RUN_CAP=%q: err = %v; want accepted", v, err)
		}
		if !ok && (err == nil || !strings.Contains(err.Error(), "MANTEION_DEMO_RUN_CAP")) {
			t.Errorf("MANTEION_DEMO_RUN_CAP=%q: err = %v; want an error naming the variable", v, err)
		}
	}
}

func TestConfigFromEnv_Addr(t *testing.T) {
	cases := map[string]bool{
		":8090": true, "127.0.0.1:8090": true, "[::1]:8090": true, ":65535": true,
		"8090": false, ":0": false, ":65536": false, ":http": false, " :8090": false, "localhost": false,
	}
	for v, ok := range cases {
		_, err := ConfigFromEnv(envOf(map[string]string{"MANTEION_DEMO_EDGE_SECRET": goodSecret, "MANTEION_DEMO_ADDR": v}))
		if ok && err != nil {
			t.Errorf("MANTEION_DEMO_ADDR=%q: err = %v; want accepted", v, err)
		}
		if !ok && (err == nil || !strings.Contains(err.Error(), "MANTEION_DEMO_ADDR")) {
			t.Errorf("MANTEION_DEMO_ADDR=%q: err = %v; want an error naming the variable", v, err)
		}
	}
}

// TestConfigValidate_Bounds: the values that have no environment variable are
// still checked, so a bad edit of a default fails at start-up, naming the field.
func TestConfigValidate_Bounds(t *testing.T) {
	base := DefaultConfig()
	base.EdgeSecret = NewSecret(goodSecret)
	if err := base.validate(); err != nil {
		t.Fatalf("defaults with a secret: %v", err)
	}
	cases := []struct {
		field string
		edit  func(c *Config)
	}{
		{"Config.StatusPoll", func(c *Config) { c.StatusPoll = time.Millisecond }},
		{"Config.StatusPoll", func(c *Config) { c.StatusPoll = time.Minute }},
		{"Config.SampleEvery", func(c *Config) { c.SampleEvery = 0 }},
		{"Config.QueryTimeout", func(c *Config) { c.QueryTimeout = 0 }},
		{"Config.QueryTimeout", func(c *Config) { c.QueryTimeout = c.SampleEvery }},
		{"Config.Heartbeat", func(c *Config) { c.Heartbeat = 10 * time.Minute }},
		{"Config.StreamMaxAge", func(c *Config) { c.StreamMaxAge = time.Second }},
		{"Config.HealthTTL", func(c *Config) { c.HealthTTL = 0 }},
		{"Config.Capacity", func(c *Config) { c.Capacity = 0 }},
		{"Config.Capacity", func(c *Config) { c.Capacity = 11 }},
		{"Config.Retain", func(c *Config) { c.Retain = 0 }},
		{"Config.Recent", func(c *Config) { c.Recent = 0 }},
		{"Config.Recent", func(c *Config) { c.Recent = c.Retain + 1 }},
	}
	for _, tc := range cases {
		c := base
		tc.edit(&c)
		err := c.validate()
		if err == nil || !strings.Contains(err.Error(), tc.field) {
			t.Errorf("%s out of range: err = %v; want an error naming it", tc.field, err)
		}
	}
}

// TestSecret_NeverPrinted: the edge secret shows only "[redacted]" (or, where
// fmt cannot call its methods, an address) however a Config holding it is
// printed, logged, encoded or wrapped in an error.
func TestSecret_NeverPrinted(t *testing.T) {
	cfg, err := ConfigFromEnv(envOf(map[string]string{"MANTEION_DEMO_EDGE_SECRET": goodSecret}))
	if err != nil {
		t.Fatal(err)
	}
	type unexported struct {
		cfg  Config
		note string
	}
	type embedded struct {
		Config
		note string
	}
	type pointer struct{ cfg *Config }
	holders := map[string]any{
		"Secret":              cfg.EdgeSecret,
		"Config":              cfg,
		"*Config":             &cfg,
		"unexported field":    unexported{cfg: cfg, note: "x"},
		"embedded":            embedded{Config: cfg, note: "x"},
		"*embedded":           &embedded{Config: cfg},
		"unexported pointer":  pointer{cfg: &cfg},
		"slice of Config":     []Config{cfg},
		"map of Config":       map[string]Config{"a": cfg},
		"interface in struct": struct{ v any }{v: cfg},
	}
	verbs := []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%T", "%10.3s", "%-v"}
	leaks := func(what, out string) {
		t.Helper()
		if strings.Contains(out, goodSecret) || strings.Contains(strings.ToLower(out), strings.ToLower(fmt.Sprintf("%x", goodSecret))) {
			t.Errorf("%s leaks the secret: %s", what, out)
		}
	}
	for name, h := range holders {
		for _, verb := range verbs {
			leaks(name+" "+verb, fmt.Sprintf(verb, h))
		}
		leaks(name+" Sprint", fmt.Sprint(h))
		leaks(name+" Sprintln", fmt.Sprintln(h))
		leaks(name+" Errorf", fmt.Errorf("start demo with %v: %w", h, errors.New("boom")).Error())

		var buf bytes.Buffer
		slog.New(slog.NewTextHandler(&buf, nil)).Info("config", "cfg", h)
		slog.New(slog.NewJSONHandler(&buf, nil)).Info("config", "cfg", h)
		slog.New(slog.NewTextHandler(&buf, nil)).Info("config", slog.Group("g", "cfg", h))
		leaks(name+" slog", buf.String())

		if b, err := json.Marshal(h); err == nil {
			leaks(name+" json", string(b))
		}
	}
	if out := fmt.Sprintf("%v", cfg); !strings.Contains(out, ":8090") || !strings.Contains(out, "[redacted]") {
		t.Errorf("printed config lost its other fields or the redaction mark: %s", out)
	}
	if !cfg.EdgeSecret.Matches(goodSecret) || cfg.EdgeSecret.Matches(goodSecret+"x") || cfg.EdgeSecret.Matches("") {
		t.Errorf("Matches is wrong")
	}
	if (Secret{}).Matches("") || NewSecret("").IsSet() || (Secret{}).Reveal() != "" {
		t.Errorf("the unset secret matches or reveals something")
	}
	if cfg.EdgeSecret.Reveal() != goodSecret || !cfg.EdgeSecret.Equal(NewSecret(goodSecret)) {
		t.Errorf("Reveal or Equal is wrong")
	}
}

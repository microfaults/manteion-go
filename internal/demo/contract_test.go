package demo

import (
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// decodeObject marshals v and decodes it back as a generic JSON object.
func decodeObject(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return m
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func wantKeys(t *testing.T, what string, m map[string]any, keys ...string) {
	t.Helper()
	slices.Sort(keys)
	if got := keysOf(m); !slices.Equal(got, keys) {
		t.Errorf("%s keys = %v; want exactly %v", what, got, keys)
	}
}

func intp(i int) *int { return &i }

func strp(s string) *string { return &s }

func f64p(f float64) *float64 { return &f }

// samplePhases are a queued run's phases of the default scenario: baseline
// with no fault, then memory and cpu, each with its kind, target and
// intensity.
func samplePhases() [3]PhaseView {
	var out [3]PhaseView
	for i := range out {
		out[i] = PhaseView{Name: PhaseNames[i], Status: PhasePending, DurationS: PhaseDurationS}
	}
	sc := DefaultScenario()
	out[1].Fault, out[1].Target, out[1].Intensity = strp("memory"), strp(sc.Memory.Target), strp(sc.Memory.Intensity)
	out[2].Fault, out[2].Target, out[2].Intensity = strp("cpu"), strp(sc.CPU.Target), strp(sc.CPU.Intensity)
	return out
}

func TestRunJSON_QueuedRun(t *testing.T) {
	run := Run{RunID: "demo-1", Status: RunQueued, Position: intp(1), QueuedAt: testNow,
		Phases: samplePhases(), Testbed: FixedTestbed()}
	m := decodeObject(t, run)
	wantKeys(t, "run", m, "run_id", "scenario", "status", "reason", "position", "queued_at", "started_at", "ended_at", "phases", "testbed")
	for _, k := range []string{"reason", "started_at", "ended_at"} {
		if m[k] != nil {
			t.Errorf("%s = %v; want null", k, m[k])
		}
	}
	if m["position"] != 1.0 || m["status"] != "queued" || m["queued_at"] != "2026-11-20T12:00:00.000Z" {
		t.Errorf("run = %v", m)
	}
	phases, ok := m["phases"].([]any)
	if !ok || len(phases) != 3 {
		t.Fatalf("phases = %v; want an array of 3", m["phases"])
	}
	for i, p := range phases {
		pm := p.(map[string]any)
		wantKeys(t, "phase", pm, "name", "fault", "target", "intensity", "status", "started_at", "ended_at", "duration_s", "result",
			"fault_observed", "measure")
		if pm["name"] != PhaseNames[i] || pm["status"] != "pending" || pm["duration_s"] != 40.0 ||
			pm["started_at"] != nil || pm["ended_at"] != nil || pm["result"] != nil ||
			pm["fault_observed"] != nil || pm["measure"] != nil {
			t.Errorf("phase %d = %v", i, pm)
		}
		want := [3][3]any{
			{nil, nil, nil},
			{"memory", "productcatalogservice", "moderate"},
			{"cpu", "productcatalogservice", "moderate"},
		}[i]
		if got := [3]any{pm["fault"], pm["target"], pm["intensity"]}; got != want {
			t.Errorf("phase %d fault, target, intensity = %v; want %v", i, got, want)
		}
	}
}

func TestRunJSON_EndedRun(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	start := time.Date(2026, 11, 20, 17, 31, 2, 123456789, ist)
	end := start.Add(3 * time.Minute)
	phases := samplePhases()
	phases[0].Status = PhaseCompleted
	phases[0].StartedAt = &start
	phases[0].EndedAt = &end
	phases[0].Result = &Result{Requests: 120, HTTPErrors: 2, P50Ms: 12.5, P95Ms: 40, P99Ms: 61.25}
	run := Run{RunID: "demo-2", Status: RunCancelled, Reason: ReasonTimedOut, QueuedAt: testNow,
		StartedAt: &start, EndedAt: &end, Phases: phases, Testbed: FixedTestbed()}

	m := decodeObject(t, run)
	if m["position"] != nil {
		t.Errorf("position = %v; want null once ended", m["position"])
	}
	if m["reason"] != "timed_out" || m["status"] != "cancelled" {
		t.Errorf("status/reason = %v/%v", m["status"], m["reason"])
	}
	if m["started_at"] != "2026-11-20T12:01:02.123Z" || m["ended_at"] != "2026-11-20T12:04:02.123Z" {
		t.Errorf("times = %v, %v; want UTC RFC 3339 with milliseconds", m["started_at"], m["ended_at"])
	}
	p0 := m["phases"].([]any)[0].(map[string]any)
	res, ok := p0["result"].(map[string]any)
	if !ok {
		t.Fatalf("phase 0 result = %v", p0["result"])
	}
	wantKeys(t, "result", res, "requests", "http_errors", "p50_ms", "p95_ms", "p99_ms")
	if res["requests"] != 120.0 || res["http_errors"] != 2.0 || res["p50_ms"] != 12.5 || res["p99_ms"] != 61.25 {
		t.Errorf("result = %v", res)
	}

	// The marshalled run decodes back into the same value (times in UTC).
	b, _ := json.Marshal(run)
	var back Run
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	if back.Reason != ReasonTimedOut || back.Status != RunCancelled || back.Position != nil ||
		!back.StartedAt.Equal(start.Truncate(time.Millisecond)) {
		t.Errorf("round trip = %+v", back)
	}
}

// TestRunJSON_ScenarioRun is the exact JSON of a completed run of the default
// scenario: the scenario's id, the phases baseline, memory and cpu, nulls on
// baseline, and on each fault phase its fault, target, intensity, proof and
// measured figures.
func TestRunJSON_ScenarioRun(t *testing.T) {
	sc := DefaultScenario()
	at := func(s int) *time.Time { v := testNow.Add(time.Duration(s) * time.Second); return &v }
	yes := true
	run := Run{RunID: "demo-3", Scenario: sc.ID, Status: RunCompleted, QueuedAt: testNow, StartedAt: at(2), EndedAt: at(125),
		Phases: [3]PhaseView{
			{Name: "baseline", Status: PhaseCompleted, StartedAt: at(2), EndedAt: at(42), DurationS: 40,
				Result: &Result{Requests: 120, P50Ms: 15.9, P95Ms: 65.8, P99Ms: 94.9}},
			{Name: "memory", Fault: strp(string(sc.Memory.Kind)), Target: strp(sc.Memory.Target), Intensity: strp(sc.Memory.Intensity),
				Status: PhaseCompleted, StartedAt: at(43), EndedAt: at(83), DurationS: 40,
				Result:        &Result{Requests: 119, HTTPErrors: 1, P50Ms: 16, P95Ms: 71.4, P99Ms: 97.2},
				FaultObserved: &yes, Measure: &Measure{Metric: "target_memory_bytes", Value: f64p(61456499), Baseline: f64p(45000000)}},
			{Name: "cpu", Fault: strp(string(sc.CPU.Kind)), Target: strp(sc.CPU.Target), Intensity: strp(sc.CPU.Intensity),
				Status: PhaseCompleted, StartedAt: at(84), EndedAt: at(124), DurationS: 40,
				Result:        &Result{Requests: 120, P50Ms: 14, P95Ms: 49.4, P99Ms: 98.3},
				FaultObserved: &yes, Measure: &Measure{Metric: "target_cpu_cores", Value: f64p(0.13), Baseline: f64p(0.0293)}},
		},
		Testbed: FixedTestbed()}

	const want = `{"run_id":"demo-3","scenario":"catalog-moderate","status":"completed","reason":null,"position":null,` +
		`"queued_at":"2026-11-20T12:00:00.000Z","started_at":"2026-11-20T12:00:02.000Z","ended_at":"2026-11-20T12:02:05.000Z","phases":[` +
		`{"name":"baseline","fault":null,"target":null,"intensity":null,"status":"completed",` +
		`"started_at":"2026-11-20T12:00:02.000Z","ended_at":"2026-11-20T12:00:42.000Z","duration_s":40,` +
		`"result":{"requests":120,"http_errors":0,"p50_ms":15.9,"p95_ms":65.8,"p99_ms":94.9},"fault_observed":null,"measure":null},` +
		`{"name":"memory","fault":"memory","target":"productcatalogservice","intensity":"moderate","status":"completed",` +
		`"started_at":"2026-11-20T12:00:43.000Z","ended_at":"2026-11-20T12:01:23.000Z","duration_s":40,` +
		`"result":{"requests":119,"http_errors":1,"p50_ms":16,"p95_ms":71.4,"p99_ms":97.2},"fault_observed":true,` +
		`"measure":{"metric":"target_memory_bytes","value":61456499,"baseline":45000000}},` +
		`{"name":"cpu","fault":"cpu","target":"productcatalogservice","intensity":"moderate","status":"completed",` +
		`"started_at":"2026-11-20T12:01:24.000Z","ended_at":"2026-11-20T12:02:04.000Z","duration_s":40,` +
		`"result":{"requests":120,"http_errors":0,"p50_ms":14,"p95_ms":49.4,"p99_ms":98.3},"fault_observed":true,` +
		`"measure":{"metric":"target_cpu_cores","value":0.13,"baseline":0.0293}}],` +
		`"testbed":{"machine":"One shared 6-core machine that also runs unrelated services.",` +
		`"cpu_limits_m":{"frontend":200,"productcatalogservice":200,"cartservice":200,"load_generator":950,"control_plane":200},` +
		`"phase_duration_s":40,"note":"Figures vary from run to run."}}`
	b, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != want {
		t.Errorf("run\n %s\nwant\n %s", b, want)
	}

	// It decodes back into a run that marshals to the same bytes.
	var back Run
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	if again, err := json.Marshal(back); err != nil || string(again) != want {
		t.Errorf("round trip = %s, %v", again, err)
	}
}

// TestMeasureJSON: both metric names marshal, a figure that is nil, NaN or
// infinite is null, and a metric outside the list refuses to marshal.
func TestMeasureJSON(t *testing.T) {
	cases := []struct {
		m    Measure
		want string
	}{
		{Measure{Metric: "target_memory_bytes", Value: f64p(1.5e8), Baseline: f64p(0)},
			`{"metric":"target_memory_bytes","value":150000000,"baseline":0}`},
		{Measure{Metric: "target_cpu_cores", Value: f64p(math.NaN()), Baseline: f64p(0.03)},
			`{"metric":"target_cpu_cores","value":null,"baseline":0.03}`},
		{Measure{Metric: "target_cpu_cores", Value: f64p(math.Inf(1)), Baseline: f64p(math.Inf(-1))},
			`{"metric":"target_cpu_cores","value":null,"baseline":null}`},
		{Measure{Metric: "target_memory_bytes"}, `{"metric":"target_memory_bytes","value":null,"baseline":null}`},
	}
	for _, tc := range cases {
		b, err := json.Marshal(tc.m)
		if err != nil || string(b) != tc.want {
			t.Errorf("measure %+v = %s, %v; want %s", tc.m, b, err, tc.want)
		}
	}
	for _, metric := range []string{"", "target_load", "cpu", "pq: 192.0.2.10"} {
		if b, err := json.Marshal(Measure{Metric: metric, Value: f64p(1)}); err == nil {
			t.Errorf("measure with metric %q marshalled: %s", metric, b)
		} else if strings.Contains(err.Error(), "192.0.2.10") {
			t.Errorf("error %q echoes the metric", err)
		}
	}

	// A fault phase whose proof could not be measured: fault_observed null.
	p := decodeObject(t, PhaseView{Name: "cpu", Fault: strp("cpu"), Target: strp("cartservice"), Intensity: strp("heavy"),
		Status: PhaseCompleted, DurationS: 40, Measure: &Measure{Metric: "target_cpu_cores", Baseline: f64p(0.02)}})
	if v, ok := p["fault_observed"]; !ok || v != nil {
		t.Errorf("fault_observed = %v (present %v); want null", v, ok)
	}
}

func TestTestbedJSON(t *testing.T) {
	const want = `{"machine":"One shared 6-core machine that also runs unrelated services.","cpu_limits_m":{"frontend":200,"productcatalogservice":200,"cartservice":200,"load_generator":950,"control_plane":200},"phase_duration_s":40,"note":"Figures vary from run to run."}`
	b, err := json.Marshal(FixedTestbed())
	if err != nil {
		t.Fatal(err)
	}
	var got, exp any
	_ = json.Unmarshal(b, &got)
	_ = json.Unmarshal([]byte(want), &exp)
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("testbed = %s\nwant      %s", b, want)
	}
	// Every service a scenario targets has its limit on the page.
	limits := decodeObject(t, FixedTestbed())["cpu_limits_m"].(map[string]any)
	for _, svc := range CatalogTargets() {
		if limits[svc] != 200.0 {
			t.Errorf("cpu_limits_m[%s] = %v; want 200", svc, limits[svc])
		}
	}
}

// TestScenarioViewJSON is the exact JSON of the default scenario's view; no
// scenario's view carries a target_load or any key outside the contract.
func TestScenarioViewJSON(t *testing.T) {
	js := func(s string) string { b, _ := json.Marshal(s); return string(b) }
	sc := DefaultScenario()
	want := `{"id":"catalog-moderate","title":` + js(sc.Title) + `,"description":` + js(sc.Description) + `,"default":true,` +
		`"workload":{"workflow":"demo-browse","dataset":"demo-products","description":` + js(sc.Workload.Description) + `},"phases":[` +
		`{"name":"baseline","duration_s":40,"fault":null,"target":null,"intensity":null},` +
		`{"name":"memory","duration_s":40,"fault":"memory","target":"productcatalogservice","intensity":"moderate"},` +
		`{"name":"cpu","duration_s":40,"fault":"cpu","target":"productcatalogservice","intensity":"moderate"}]}`
	b, err := json.Marshal(ViewOf(sc))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != want {
		t.Errorf("view\n %s\nwant\n %s", b, want)
	}

	var views []ScenarioView
	for _, sc := range Catalog() {
		v := ViewOf(sc)
		views = append(views, v)
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("%s: marshal: %v", sc.ID, err)
		}
		for _, leak := range []string{"target_load", "TargetLoad", "WorkflowID", "DatasetName"} {
			if strings.Contains(string(b), leak) {
				t.Errorf("%s: view %s contains %q", sc.ID, b, leak)
			}
		}
		m := decodeObject(t, v)
		wantKeys(t, sc.ID, m, "id", "title", "description", "default", "workload", "phases")
		wantKeys(t, sc.ID+" workload", m["workload"].(map[string]any), "workflow", "dataset", "description")
		for i, p := range m["phases"].([]any) {
			pm := p.(map[string]any)
			wantKeys(t, sc.ID+" phase", pm, "name", "duration_s", "fault", "target", "intensity")
			step := []*FaultStep{nil, &sc.Memory, &sc.CPU}[i]
			if step == nil {
				if pm["fault"] != nil || pm["target"] != nil || pm["intensity"] != nil {
					t.Errorf("%s: baseline phase %v; want no fault", sc.ID, pm)
				}
				continue
			}
			if pm["fault"] != string(step.Kind) || pm["target"] != step.Target || pm["intensity"] != step.Intensity {
				t.Errorf("%s: phase %v; want %+v", sc.ID, pm, *step)
			}
		}
	}

	// The list keeps the catalog's order; an empty list is [].
	m := decodeObject(t, ScenarioList{Scenarios: views})
	wantKeys(t, "list", m, "scenarios")
	for i, v := range m["scenarios"].([]any) {
		if v.(map[string]any)["id"] != Catalog()[i].ID {
			t.Errorf("scenario %d = %v; want %s", i, v.(map[string]any)["id"], Catalog()[i].ID)
		}
	}
	if b, _ := json.Marshal(ScenarioList{}); string(b) != `{"scenarios":[]}` {
		t.Errorf("empty list = %s", b)
	}
}

func TestStateJSON(t *testing.T) {
	b, err := json.Marshal(State{Capacity: 3, Accepting: true})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"capacity":3,"accepting":true,"queue":[],"running":null}` {
		t.Errorf("idle state = %s", b)
	}

	started := testNow
	st := State{Capacity: 3, Accepting: false,
		Queue: []QueueEntry{{RunID: "demo-1", Position: 0, Status: RunRunning}, {RunID: "demo-2", Position: 1, Status: RunQueued}},
		Running: &RunningPhase{RunID: "demo-1", Phase: "baseline", PhaseIndex: 0, PhaseStartedAt: &started,
			PhaseDurationS: 40, Progress: 0.25}}
	m := decodeObject(t, st)
	wantKeys(t, "state", m, "capacity", "accepting", "queue", "running")
	q := m["queue"].([]any)
	wantKeys(t, "queue entry", q[0].(map[string]any), "run_id", "position", "status")
	r := m["running"].(map[string]any)
	wantKeys(t, "running", r, "run_id", "phase", "phase_index", "phase_started_at", "phase_duration_s", "progress")
	if r["phase_started_at"] != "2026-11-20T12:00:00.000Z" || r["progress"] != 0.25 {
		t.Errorf("running = %v", r)
	}
	r2 := decodeObject(t, RunningPhase{RunID: "demo-1", Phase: "baseline", PhaseDurationS: 40})
	if v, ok := r2["phase_started_at"]; !ok || v != nil {
		t.Errorf("phase_started_at = %v (present %v); want null", v, ok)
	}
}

func TestSmallBodiesJSON(t *testing.T) {
	cases := []struct {
		v    any
		want string
	}{
		{Enqueued{RunID: "demo-1", Status: RunQueued, Position: 2}, `{"run_id":"demo-1","status":"queued","position":2}`},
		{ErrorBody{Error: CodeQueueFull}, `{"error":"queue_full"}`},
		{RecentRuns{}, `{"runs":[]}`},
		{QueueEvent{RunID: "demo-1", Position: 1}, `{"run_id":"demo-1","position":1}`},
		{PhaseEvent{RunID: "demo-1", Phase: "cpu", PhaseIndex: 2, Status: PhaseRunning, At: testNow},
			`{"run_id":"demo-1","phase":"cpu","phase_index":2,"status":"running","at":"2026-11-20T12:00:00.000Z"}`},
		{Health{OK: false, Accepting: true, Checks: HealthChecks{Database: true, LoadGenerator: true, Target: true, Idle: true}},
			`{"ok":false,"accepting":true,"checks":{"database":true,"load_generator":true,"metrics":false,"target":true,"idle":true}}`},
	}
	for _, tc := range cases {
		b, err := json.Marshal(tc.v)
		if err != nil {
			t.Errorf("%T: %v", tc.v, err)
			continue
		}
		if string(b) != tc.want {
			t.Errorf("%T = %s\nwant %s", tc.v, b, tc.want)
		}
	}
	if !(HealthChecks{true, true, true, true, true}).All() || (HealthChecks{true, true, false, true, true}).All() {
		t.Errorf("HealthChecks.All is wrong")
	}
}

func TestSampleJSON(t *testing.T) {
	s := Sample{RunID: "demo-1", Phase: "baseline", At: testNow, RPS: f64p(5), P95Ms: f64p(math.NaN()), ErrorRate: f64p(0)}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"run_id":"demo-1","phase":"baseline","at":"2026-11-20T12:00:00.000Z","rps":5,"p95_ms":null,"error_rate":0,"target_cpu_cores":null,"target_memory_bytes":null}` {
		t.Errorf("sample = %s", b)
	}
	s = Sample{RunID: "demo-1", Phase: "memory", At: testNow, TargetCPUCores: f64p(0.0293), TargetMemoryBytes: f64p(61456499)}
	if b, err := json.Marshal(s); err != nil ||
		string(b) != `{"run_id":"demo-1","phase":"memory","at":"2026-11-20T12:00:00.000Z","rps":null,"p95_ms":null,"error_rate":null,"target_cpu_cores":0.0293,"target_memory_bytes":61456499}` {
		t.Errorf("sample with the target's series = %s, %v", b, err)
	}
	s = Sample{RunID: "demo-1", Phase: "baseline", At: testNow, RPS: f64p(math.Inf(1)), ErrorRate: f64p(math.Inf(-1)),
		TargetCPUCores: f64p(math.NaN()), TargetMemoryBytes: f64p(math.Inf(1))}
	m := decodeObject(t, s)
	wantKeys(t, "sample", m, "run_id", "phase", "at", "rps", "p95_ms", "error_rate", "target_cpu_cores", "target_memory_bytes")
	if m["rps"] != nil || m["p95_ms"] != nil || m["error_rate"] != nil || m["target_cpu_cores"] != nil || m["target_memory_bytes"] != nil {
		t.Errorf("infinite or missing values = %v; want null", m)
	}
	if s.Empty() != true || (Sample{RPS: f64p(1)}).Empty() {
		t.Errorf("Sample.Empty is wrong")
	}
	if (Sample{TargetCPUCores: f64p(0.1)}).Empty() || (Sample{TargetMemoryBytes: f64p(1)}).Empty() {
		t.Errorf("a sample with only the target's series is empty; want it sent")
	}
}

func TestClosedLists(t *testing.T) {
	reasons := map[Reason]string{
		ReasonLabBusy: "lab_busy", ReasonLoadGeneratorUnavailable: "load_generator_unavailable",
		ReasonTargetUnavailable: "target_unavailable", ReasonPhaseFailed: "phase_failed",
		ReasonTimedOut: "timed_out", ReasonInterrupted: "interrupted", ReasonInternalError: "internal_error",
	}
	for r, want := range reasons {
		b, err := json.Marshal(r)
		if err != nil || string(b) != `"`+want+`"` {
			t.Errorf("reason %v = %s, %v; want %q", r, b, err, want)
		}
		var back Reason
		if err := json.Unmarshal(b, &back); err != nil || back != r {
			t.Errorf("reason %q decodes to %v, %v", want, back, err)
		}
	}
	if b, _ := json.Marshal(Reason{}); string(b) != "null" {
		t.Errorf("zero reason = %s; want null", b)
	}
	var none Reason
	if err := json.Unmarshal([]byte("null"), &none); err != nil || !none.IsZero() {
		t.Errorf("null reason decodes to %v, %v", none, err)
	}
	leak := `"pq: 192.0.2.10 exp-123"`
	if err := json.Unmarshal([]byte(leak), &none); err == nil || strings.Contains(err.Error(), "192.0.2.10") {
		t.Errorf("unknown reason: err = %v; want an error that does not echo the value", err)
	}
	if _, err := json.Marshal(Reason{v: "pq: 192.0.2.10"}); err == nil {
		t.Errorf("a reason outside the list marshalled")
	}

	codes := map[Code]struct {
		s      string
		status int
	}{
		CodeBadRequest: {"bad_request", 400}, CodeForbidden: {"forbidden", 403}, CodeNotFound: {"not_found", 404},
		CodeClientHasRun: {"client_has_run", 429}, CodeQueueFull: {"queue_full", 429}, CodeRateLimited: {"rate_limited", 429},
		CodeTooManyStreams: {"too_many_streams", 429}, CodeUnavailable: {"unavailable", 503}, CodeInternal: {"internal", 500},
	}
	for c, want := range codes {
		b, err := json.Marshal(c)
		if err != nil || string(b) != `"`+want.s+`"` || c.HTTPStatus() != want.status {
			t.Errorf("code %v = %s (%d), %v; want %q (%d)", c, b, c.HTTPStatus(), err, want.s, want.status)
		}
	}
	if _, err := json.Marshal(Code{}); err == nil {
		t.Errorf("the zero code marshalled")
	}
	if (Code{}).HTTPStatus() != 500 {
		t.Errorf("the zero code's status = %d; want 500", (Code{}).HTTPStatus())
	}

	for _, s := range []RunStatus{RunQueued, RunRunning, RunCompleted, RunFailed, RunCancelled} {
		if b, err := json.Marshal(s); err != nil || string(b) != `"`+string(s)+`"` {
			t.Errorf("run status %q = %s, %v", s, b, err)
		}
	}
	for _, s := range []PhaseStatus{PhasePending, PhaseRunning, PhaseCompleted, PhaseFailed, PhaseSkipped} {
		if b, err := json.Marshal(s); err != nil || string(b) != `"`+string(s)+`"` {
			t.Errorf("phase status %q = %s, %v", s, b, err)
		}
	}
	if _, err := json.Marshal(PhaseStatus("paused")); err == nil {
		t.Errorf("core phase status paused marshalled; the contract maps it to running")
	}
	if _, err := json.Marshal(RunStatus("planned")); err == nil {
		t.Errorf("core experiment status planned marshalled as a run status")
	}
	var ps PhaseStatus
	if err := json.Unmarshal([]byte(`"draining"`), &ps); err == nil {
		t.Errorf("phase status draining decoded")
	}
}

func TestPromQLStrings(t *testing.T) {
	if QueryRPS != `sum(rate(http_server_requests_total{service="frontend"}[30s]))` {
		t.Errorf("QueryRPS = %s", QueryRPS)
	}
	if QueryP95Ms != `1000 * histogram_quantile(0.95, sum by (le) (rate(http_server_request_duration_seconds_bucket{service="frontend"}[30s])))` {
		t.Errorf("QueryP95Ms = %s", QueryP95Ms)
	}
	if QueryErrorRate != `(sum(rate(http_server_requests_total{service="frontend",status_code=~"5.."}[30s])) or vector(0)) / sum(rate(http_server_requests_total{service="frontend"}[30s]))` {
		t.Errorf("QueryErrorRate = %s", QueryErrorRate)
	}
}

func TestContractNames(t *testing.T) {
	if PhaseNames != [3]string{"baseline", "memory", "cpu"} || PhaseDurationS != 40 {
		t.Errorf("phases = %v, %d s; want baseline, memory, cpu, 40 s", PhaseNames, PhaseDurationS)
	}
	if HeaderClient != "X-Demo-Client" || HeaderEdgeSecret != "X-Demo-Edge-Secret" || DefaultClient != "local" || MaxClientBytes != 200 {
		t.Errorf("headers = %q %q, default client %q, max %d", HeaderClient, HeaderEdgeSecret, DefaultClient, MaxClientBytes)
	}
	if EventState != "state" || EventQueue != "queue" || EventPhase != "phase" || EventSample != "sample" || EventEnd != "end" {
		t.Errorf("event types renamed")
	}
	if CreatedBy != "manteion-demo" || RunNamePrefix != "demo-run-" || RunIDPrefix != "demo" {
		t.Errorf("names = %q %q %q", CreatedBy, RunNamePrefix, RunIDPrefix)
	}
	if m, c := RuleName("demo-1", FaultMemory), RuleName("demo-1", FaultCPU); m != "demo-run-demo-1-memory" || c != "demo-run-demo-1-cpu" {
		t.Errorf("rule names = %q %q", m, c)
	}
}

// TestNonFiniteNumbersNeverFailAResponse: a NaN or infinite number in a run
// or a state marshals; latencies (typed num, not nullable) become 0, progress
// is clamped to 0..1 and non-finite progress is 0.
func TestNonFiniteNumbersNeverFailAResponse(t *testing.T) {
	phases := samplePhases()
	phases[0].Result = &Result{Requests: 10, P50Ms: math.NaN(), P95Ms: math.Inf(1), P99Ms: math.Inf(-1)}
	run := Run{RunID: "demo-1", Status: RunCompleted, QueuedAt: testNow, Phases: phases, Testbed: FixedTestbed()}
	m := decodeObject(t, run)
	res := m["phases"].([]any)[0].(map[string]any)["result"].(map[string]any)
	if res["p50_ms"] != 0.0 || res["p95_ms"] != 0.0 || res["p99_ms"] != 0.0 || res["requests"] != 10.0 {
		t.Errorf("non-finite latencies = %v; want 0", res)
	}

	for _, tc := range []struct{ in, want float64 }{
		{math.NaN(), 0}, {math.Inf(1), 0}, {math.Inf(-1), 0}, {-0.2, 0}, {1.5, 1}, {0.4, 0.4},
	} {
		st := State{Capacity: 3, Queue: []QueueEntry{{RunID: "demo-1", Status: RunRunning}},
			Running: &RunningPhase{RunID: "demo-1", Phase: "baseline", PhaseDurationS: 40, Progress: tc.in}}
		m := decodeObject(t, st)
		if got := m["running"].(map[string]any)["progress"]; got != tc.want {
			t.Errorf("progress %v marshals as %v; want %v", tc.in, got, tc.want)
		}
	}
}

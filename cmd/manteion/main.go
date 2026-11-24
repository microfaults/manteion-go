// Command manteion is the central coordination controller for the atropos
// ecosystem. It provides centralized rule management, zeus-go workflow
// orchestration, and SDK startup dependency enforcement.
//
// All state is persisted in PostgreSQL. Connection configured via
// MANTEION_DATABASE_URL environment variable.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"manteion-go/internal/api"
	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/atropos"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/db"
	"manteion-go/internal/demo"
	"manteion-go/internal/desired"
	"manteion-go/internal/logql"
	"manteion-go/internal/model"
	"manteion-go/internal/opaharness"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/policy"
	"manteion-go/internal/promql"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// @title           Manteion Control-Plane API
// @version         1.0
// @description     Central coordination controller for the atropos ecosystem.
// @description     Manages rules, faults, experiments, workflows, and SDK lifecycle.
// @servers.url            http://localhost:9090/api/v1
// @servers.description    Local dev (HTTP)
// @servers.url            https://localhost:9090/api/v1
// @servers.description    Local dev (HTTPS)
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Configuration from environment variables.
	addr := envOr("MANTEION_ADDR", ":9090")
	dsn := envOr("MANTEION_DATABASE_URL",
		"postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable")
	zeusURL := envOr("ZEUS_URL", "http://archer:8080")
	prometheusURL := envOr("PROMETHEUS_URL", "http://prometheus:9090")
	lokiURL := envOr("MANTEION_LOKI_URL", "http://loki:3100")
	cacheDir := envOr("CACHE_DIR", "/var/cache/manteion")
	// The convergence and drain barrier bounds (orchestrator setup below), which
	// also size the HTTP write timeout: startup refuses a value that is not a
	// positive duration of at most an hour (barrierTimeouts).
	convergeTimeout, drainTimeout, err := barrierTimeouts()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	// How often a running phase's poller asks zeus whether its load has ended
	// (pollIntervalEnv; set on the orchestrator below).
	pollInterval, err := pollIntervalEnv()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	// Policy automation (started below): "rego" (or "on"), "threshold" or "off".
	policyEngineMode := envOr("MANTEION_POLICY_ENGINE", "off")
	// Demo mode (MANTEION_DEMO=true; wired after Recover below). Its
	// configuration is refused here, with the rest, before anything starts.
	// demo.ConfigFromEnv's errors never contain the edge secret.
	demoOn := envOr("MANTEION_DEMO", "false") == "true"
	if err := demoModeCheck(demoOn, policyEngineMode); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	var demoCfg demo.Config
	if demoOn {
		if demoCfg, err = demo.ConfigFromEnv(os.Getenv); err != nil {
			logger.Error("invalid configuration", "error", err)
			os.Exit(1)
		}
	}

	// Connect to PostgreSQL and run migrations.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := db.Open(ctx, dsn)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close(database)

	// Create repositories. Generations is the desired-state change fabric: the
	// rule repo bumps it in-transaction, the API bumps it for fault configs, the
	// orchestrator bumps it on phase transitions, and every bump wakes the same
	// long-polls and SSE subscribers.
	gens := desired.NewGenerations(store.NewGenerationRepo(database), desired.NewBroker())
	ruleRepo := store.NewRuleRepo(database, gens)
	faultRepo := store.NewFaultRepo(database)
	faultConfigRepo := store.NewFaultConfigRepo(database)
	sdkRepo := store.NewSDKRepo(database)
	experimentRepo := store.NewExperimentRepo(database)
	workflowRepo := store.NewWorkflowRepo(database)
	workloadRepo := store.NewWorkloadRepo(database)
	traceRepo := store.NewTraceRepo(database)
	artifactRepo := store.NewReplayArtifactRepo(database)

	// Create zeus client.
	zeusClient := zeus.NewClient(zeusURL)

	// Create the atropos transport + fleet controller: instance status,
	// fidelity pulls and the live-instance resolver. Nothing is pushed to an
	// instance; rules, faults, the freeze and replay sets reach it through
	// the desired state it polls (replay sets are pulled by the instance).
	txClient := atropos.NewClient(atropos.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}))
	resolver := &atrocontrol.RepoResolver{Repo: sdkRepo}
	controller := atrocontrol.New(txClient, resolver,
		atrocontrol.WithDefaultTimeout(2*time.Second),
		atrocontrol.WithDefaultConcurrency(16),
		atrocontrol.WithLogger(logger),
	)

	policyRepo := store.NewPolicyRepo(database)

	// Create promql client, cache store, orchestrator, and policy engine.
	promClient := promql.NewClient(prometheusURL)
	cs := cachestore.New(cacheDir)
	if err := cs.EnsureRoot(); err != nil {
		logger.Error("cache store root not writable", "dir", cacheDir, "error", err)
		os.Exit(1)
	}
	phaseFaultEventRepo := store.NewPhaseFaultEventRepo(database)
	orch := orchestrator.New(experimentRepo, ruleRepo, faultRepo, workloadRepo, workflowRepo, controller, promClient, zeusClient, cs, phaseFaultEventRepo, artifactRepo, gens, logger)
	// MANTEION_POLL_INTERVAL: the zeus poll tick. A phase ends at the first
	// tick after zeus reports its load done, so the tick bounds that lag.
	orch.WithPollInterval(pollInterval)
	// Convergence barrier (desired-state contract §4): a phase entry or resume
	// bumps its services and waits up to MANTEION_CONVERGE_TIMEOUT for every
	// alive instance to observe the bump (and, on a frozen service, to hold the
	// replay set committed), failing the phase on timeout; an exit or drain
	// waits as long and only logs.
	orch.WithConvergeTimeout(convergeTimeout)
	// Drain barrier (MANT-2): MANTEION_DRAIN_TIMEOUT must be ≥ 3× the SDK poll
	// interval + flush time (default 30s); MANTEION_ALLOW_DEGRADED_BASELINE lets
	// isolation phases start from a degraded recording.
	orch.WithDrainTimeout(drainTimeout)
	orch.WithAllowDegradedBaseline(envOr("MANTEION_ALLOW_DEGRADED_BASELINE", "false") == "true")
	// MANTEION_ZEUS_DATASET_ID is the fallback zeus dataset for phase workflow
	// rows that carry no dataset_id of their own. Without either, zeus stages
	// an empty pool map, so a data_schema workflow drives unresolved
	// {{data.*}} traffic rather than failing.
	orch.WithZeusDatasetID(envOr("MANTEION_ZEUS_DATASET_ID", ""))
	policyEngine := policy.New(policyRepo, ruleRepo, promClient, logger)

	// OPA harness (docs/opa-harness.md): Rego policies over Prometheus/Loki
	// inputs whose outputs are rule mutations applied through ruleRepo, so
	// they reach instances by the same path as PUT /api/v1/rules/{id}. Always
	// constructed (the API's dry-run evaluate uses it); only Run is gated.
	opaPolicyRepo := store.NewOPAPolicyRepo(database)
	policyDecisionRepo := store.NewPolicyDecisionRepo(database)
	harness := opaharness.New(opaharness.Deps{
		Policies:  opaPolicyRepo,
		Decisions: policyDecisionRepo,
		Rules:     ruleRepo,
		Instances: sdkRepo,
		Providers: opaharness.Providers{
			model.InputSourcePrometheus: opaharness.VectorProvider{Querier: promClient},
			model.InputSourceLoki:       opaharness.VectorProvider{Querier: logql.NewClient(lokiURL)},
		},
		Mutator: opaharness.NewRuleMutator(ruleRepo, faultRepo),
		Logger:  logger,
	})

	// Restore in-flight runs from the DB. Must run before the API server
	// accepts requests, so callers see consistent state.
	if err := orch.Recover(ctx); err != nil {
		logger.Error("orchestrator recover failed", "error", err)
		os.Exit(1)
	}

	// The desired-state materializer: a function of the database that the SDK
	// handlers serve per instance, filtered by its capabilities. freeze_cfg and
	// replay_set come from the running phase that freezes the service and the
	// artifact assigned to it; one PhaseSources value fills both seams, so they
	// always name the same phase.
	phaseSources := &desired.PhaseSources{Phases: experimentRepo, Artifacts: artifactRepo}
	mat := &desired.Materializer{
		Generations: gens, Rules: ruleRepo, FaultConfigs: faultConfigRepo,
		CacheBox: experimentRepo, Specs: faultRepo,
		Freeze: phaseSources, ReplaySets: phaseSources,
	}

	// Create the API server with all dependencies.
	srv := api.NewServer(logger, database,
		ruleRepo, faultRepo, faultRepo, faultConfigRepo, sdkRepo,
		experimentRepo, phaseFaultEventRepo, workflowRepo, workloadRepo, traceRepo,
		zeusClient, gens, mat, orch, cs, artifactRepo, policyRepo,
		opaPolicyRepo, policyDecisionRepo, harness,
	)

	httpServer := &http.Server{
		Addr:        addr,
		Handler:     srv.Handler(),
		ReadTimeout: 10 * time.Second,
		// The lifecycle handlers answer only once the orchestrator is done,
		// and a stop can wait out a drain barrier and then an exit bump's
		// convergence wait (httpWriteTimeout): a shorter write deadline cuts
		// off the 202 of a stop that succeeded.
		WriteTimeout: httpWriteTimeout(convergeTimeout, drainTimeout),
		IdleTimeout:  60 * time.Second,
	}
	// Shutdown waits for in-flight requests, and a held long-poll (up to 60 s)
	// or an SDK's SSE stream would outlast its 10 s budget below: on Shutdown,
	// held polls answer at once as if their wait had elapsed and streams end.
	httpServer.RegisterOnShutdown(srv.BeginShutdown)

	// ---- Demo mode (MANTEION_DEMO=true): internal/demo on a second listener ----
	// The public demo's seven routes on their own mux and port; nothing of the
	// operator API is reachable there. Its start-up (startDemo, at the
	// listeners below) and its shutdown (shutdownInOrder) have an order of
	// their own. With demo mode off, dm and demoServer stay nil and nothing
	// below changes.
	var (
		dm         *demo.Demo
		demoServer *http.Server
	)
	if demoOn {
		dm = demo.New(demo.Deps{
			Orch:        orch,
			Experiments: experimentRepo,
			Rules:       ruleRepo,
			Specs:       faultRepo,
			Workflows:   workflowRepo,
			Zeus:        zeusClient,
			Instances:   controller,
			Prom:        promClient, // never nil: promql.NewClient above
			DB:          database,
			Clock:       demo.ClockFunc(time.Now),
			Logger:      logger,
		}, demoCfg)
		demoServer = newDemoServer(demoCfg.Addr, dm.Handler())
		logDemoSettings(logger, demoCfg, pollInterval)
	}
	// ---- end of demo mode's set-up ----

	// Policy automation. "rego" (or "on") starts the OPA harness
	// (docs/opa-harness.md); "threshold" starts the frozen PromQL engine
	// (docs/decisions/2026-06-policy-engine-freeze.md); "off" starts neither.
	// Demo mode refuses all but "off" (demoModeCheck). harnessDone is closed
	// once the harness loop has returned (at once when it is not started);
	// shutdown waits for it, so an evaluation in flight at the signal still
	// applies and records its decision.
	harnessDone := make(chan struct{})
	switch policyEngineMode {
	case "rego", "on":
		go func() {
			defer close(harnessDone)
			harness.Run(ctx)
		}()
	case "threshold":
		go policyEngine.Run(ctx)
	case "off":
		logger.Info("policy automation disabled (MANTEION_POLICY_ENGINE=off; rego|on starts the OPA harness, threshold the frozen PromQL engine)")
	default:
		logger.Warn("unknown MANTEION_POLICY_ENGINE value; policy automation disabled", "value", policyEngineMode)
	}
	if policyEngineMode != "rego" && policyEngineMode != "on" {
		close(harnessDone)
	}

	// Complete expired long-running fault configs (status bookkeeping +
	// generation bump; the SDK reconciles them away).
	srv.StartFaultConfigReaper(ctx, 30*time.Second)

	// Start SDK reaper — purges instances dead for > 10 minutes. Opt-in via
	// MANTEION_SDK_PURGE_ENABLED (default false) so dead pods can linger on the
	// dashboard for postmortem unless an operator wants them auto-cleaned.
	if envOr("MANTEION_SDK_PURGE_ENABLED", "false") == "true" {
		go func() {
			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					n, err := sdkRepo.PurgeDead(ctx, 10*time.Minute)
					if err != nil {
						logger.Error("sdk reaper: purge failed", "error", err)
					} else if n > 0 {
						logger.Info("sdk reaper: purged dead instances", "count", n)
					}
				}
			}
		}()
	}

	// Start the server(s): each binds its address now and serves in a
	// goroutine (listenAndServe), so the sockets accept connections once
	// listen returns.
	listen := func() {
		displayAddr := addr
		if displayAddr[0] == ':' {
			displayAddr = "localhost" + displayAddr
		}
		logger.Info("manteion starting",
			"url", "http://"+displayAddr,
			"zeus_url", zeusURL,
			"loki_url", lokiURL,
			"policy_engine", policyEngineMode,
		)
		listenAndServe(httpServer, "server error", logger)
		if demoServer != nil {
			logger.Info("demo listener starting", "addr", demoServer.Addr)
			listenAndServe(demoServer, "demo server error", logger)
		}
	}
	if dm != nil {
		// The demo's switch-off, then listen, then its sweep and worker; or,
		// on a signal during the switch-off, none of them.
		startDemo(ctx, dm, defaultDemoTimings, listen, logger)
	} else {
		listen()
	}

	// Block until signal.
	<-ctx.Done()
	logger.Info("shutdown signal received")

	// In demo mode the demo shuts down first, while both listeners still
	// serve; then the listeners get serverShutdownBudget (10 s) for in-flight
	// requests to complete (shutdownInOrder). The two are passed as nil
	// interfaces, never typed nils, when demo mode is off.
	var (
		stopDemo       demoLifecycle
		stopDemoServer shutdowner
	)
	if dm != nil {
		stopDemo, stopDemoServer = dm, demoServer
	}
	shutdownErr := shutdownInOrder(stopDemo, stopDemoServer, httpServer, defaultDemoTimings, func(shutdownCtx context.Context) {
		// The harness loop returns after its in-flight tick (its apply and
		// record are detached from ctx); wait for it within what is left of
		// the shutdown budget.
		select {
		case <-harnessDone:
		case <-shutdownCtx.Done():
			logger.Warn("opa harness: loop did not stop within the shutdown timeout")
		}
	}, logger)
	if shutdownErr != nil {
		os.Exit(1)
	}

	logger.Info("manteion stopped")
}

// listenAndServe is http.Server.ListenAndServe split in two: it binds
// srv.Addr now and serves it in a goroutine. A bind or serve error (other than
// http.ErrServerClosed) is logged as msg and ends the process with status 1.
func listenAndServe(srv *http.Server, msg string, logger *slog.Logger) {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		logger.Error(msg, "error", err)
		os.Exit(1)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error(msg, "error", err)
			os.Exit(1)
		}
	}()
}

// envOr returns the value of the environment variable key, or fallback if unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// maxBarrierTimeout caps MANTEION_CONVERGE_TIMEOUT and MANTEION_DRAIN_TIMEOUT.
// At the cap httpWriteTimeout is about three hours; without one, a large value
// of either would overflow it to a negative duration (no write deadline).
const maxBarrierTimeout = time.Hour

// barrierTimeouts reads the orchestrator's two barrier bounds:
// MANTEION_CONVERGE_TIMEOUT (the convergence barrier) and
// MANTEION_DRAIN_TIMEOUT (the drain barrier's report gate), 30 s each when
// unset. Both must be positive durations of at most maxBarrierTimeout: a zero
// or negative converge bound would fail every phase entry at its first look, a
// zero drain bound would end every drain before an instance could report, and
// both size httpWriteTimeout, which a negative drain bound shrinks (far enough
// below zero, to no write deadline at all) and a huge one overflows. A typo
// would silently run on the default.
func barrierTimeouts() (converge, drain time.Duration, err error) {
	if converge, err = boundedDurationEnv("MANTEION_CONVERGE_TIMEOUT", 30*time.Second, maxBarrierTimeout); err != nil {
		return 0, 0, err
	}
	if drain, err = boundedDurationEnv("MANTEION_DRAIN_TIMEOUT", 30*time.Second, maxBarrierTimeout); err != nil {
		return 0, 0, err
	}
	return converge, drain, nil
}

// defaultPollInterval is the orchestrator's own zeus poll tick
// (defaultZeusPollInterval in internal/orchestrator/poller.go), so leaving
// MANTEION_POLL_INTERVAL unset changes nothing. Keep the two equal.
const defaultPollInterval = 15 * time.Second

// maxPollInterval caps MANTEION_POLL_INTERVAL: a phase ends only at the first
// tick after zeus reports its load done, so a longer tick leaves the phase (and
// its fault) running that much longer with no load behind it.
const maxPollInterval = time.Minute

// pollIntervalEnv reads MANTEION_POLL_INTERVAL, how often a running phase's
// poller asks zeus for the state of its load drivers: defaultPollInterval when
// unset, otherwise a positive duration of at most maxPollInterval. Zero would
// panic the poller's ticker, and a typo would silently run on the default.
func pollIntervalEnv() (time.Duration, error) {
	return boundedDurationEnv("MANTEION_POLL_INTERVAL", defaultPollInterval, maxPollInterval)
}

// boundedDurationEnv is positiveDurationEnv with an upper limit: a value above
// limit is an error naming key.
func boundedDurationEnv(key string, fallback, limit time.Duration) (time.Duration, error) {
	d, err := positiveDurationEnv(key, fallback)
	if err != nil {
		return 0, err
	}
	if d > limit {
		return 0, fmt.Errorf("%s=%q must be at most %s", key, os.Getenv(key), limit)
	}
	return d, nil
}

// positiveDurationEnv reads the environment variable key as a Go duration
// that must be positive; unset is fallback. A value that is not a duration,
// or is zero or negative, is an error naming key.
func positiveDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a duration (want e.g. \"30s\"): %w", key, v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s=%q must be positive", key, v)
	}
	return d, nil
}

// httpWriteTimeout is the HTTP server's write deadline. A stop answers only
// once the phase has ended, and on the way it can wait out a recording
// phase's drain barrier — the convergence wait after its bump (up to
// convergeTimeout) and the report gate (up to drainTimeout) — and then the
// exit bump's convergence wait (up to convergeTimeout again); a start or a
// resume waits out one convergence. The extra 30 s covers the rest of the work
// (zeus calls, fidelity pulls, harvest). The long-poll and SSE handlers set
// their own deadlines.
func httpWriteTimeout(convergeTimeout, drainTimeout time.Duration) time.Duration {
	return 2*convergeTimeout + drainTimeout + 30*time.Second
}

// ---- demo mode ----

// demoModeCheck refuses demo mode beside a policy engine. The OPA harness
// ("rego" or "on") and the threshold engine both switch rules on and off by
// themselves, and the demo owns its run's rules: an engine could switch one
// on outside a run, or off in the middle of one. Without demo mode, any engine
// is allowed; an unknown value starts no engine and passes.
func demoModeCheck(demo bool, policyEngineMode string) error {
	if !demo {
		return nil
	}
	switch policyEngineMode {
	case "rego", "on", "threshold":
		return fmt.Errorf("MANTEION_DEMO=true needs MANTEION_POLICY_ENGINE=off, not %q: "+
			"a policy engine switches rules on and off by itself, and the demo owns its run's rules", policyEngineMode)
	}
	return nil
}

// newDemoServer is the demo listener's server. The handler is the demo's own
// (demo.Demo.Handler), with nothing around it: the event stream and the
// unread-body guard reach the connection through http.NewResponseController,
// which a wrapper without Unwrap hides. The event stream sets its own write
// deadline per frame, so WriteTimeout does not cut it. net/http must not
// answer OPTIONS * itself (DisableGeneralOptionsHandler), and the listener
// speaks HTTP/1.1 only: the stream's deadline handling is untested on HTTP/2.
func newDemoServer(addr string, h http.Handler) *http.Server {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	return &http.Server{
		Addr:                         addr,
		Handler:                      h,
		ReadHeaderTimeout:            5 * time.Second,
		ReadTimeout:                  10 * time.Second,
		WriteTimeout:                 30 * time.Second,
		IdleTimeout:                  60 * time.Second,
		MaxHeaderBytes:               16 << 10,
		DisableGeneralOptionsHandler: true,
		Protocols:                    protocols,
	}
}

// demoLifecycle is the part of *demo.Demo whose calls main orders
// (startDemo, shutdownInOrder). Nothing else calls them.
type demoLifecycle interface {
	SwitchOffAll(ctx context.Context) error
	Sweep(ctx context.Context) error
	Run(ctx context.Context)
	Shutdown(ctx context.Context) error
}

// shutdowner is the part of *http.Server that shutdownInOrder uses.
type shutdowner interface {
	Shutdown(ctx context.Context) error
}

// demoTimings bounds demo mode's steps in the start-up and shutdown orders.
// The start-up switch-off has no entry: the demo package bounds it itself.
type demoTimings struct {
	sweepTimeout    time.Duration // the start-up sweep
	shutdownTimeout time.Duration // the demo's own shutdown, before the servers'
}

var defaultDemoTimings = demoTimings{
	sweepTimeout:    2 * time.Minute,
	shutdownTimeout: 15 * time.Second,
}

// serverShutdownBudget is how long the servers' Shutdown (in-flight requests
// to complete) and the wait for the OPA harness loop share after the signal.
const serverShutdownBudget = 10 * time.Second

// startDemo runs demo mode's start-up around listen, which starts the
// listeners. In this order:
//
//  1. d.SwitchOffAll, once, before listen: the operator listener serves desired
//     state to the SDKs, and a demo rule left enabled by a kill would be
//     served the moment they can poll. It is database only, and bounds itself:
//     the demo package detaches it from ctx's cancellation and gives it 30 s
//     of its own, so neither a deadline here nor the signal would cut it
//     short, and a second try could only double a hung database's wait. A
//     failure is logged at error level and start-up goes on: the sweep repeats
//     the step, and no run is accepted before the sweep has run.
//  2. If ctx (the signal) is done by the time the switch-off returns, nothing
//     more: no listener is bound and no sweep or worker starts, so a rule the
//     switch-off could not reach is never served; main goes straight to its
//     shutdown (shutdownInOrder copes with servers that never served).
//  3. listen.
//  4. In a goroutine, d.Sweep under a deadline of t.sweepTimeout derived from
//     ctx: it cancels leftover demo experiments, and a cancel waits for the
//     fleet to converge, which the fleet can only do by polling the operator
//     listener. Its error is logged and does not stop the next step. Then,
//     once the sweep has returned, d.Run(ctx), until ctx is done or d.Shutdown
//     is called.
//
// It returns once listen has returned, or at once after step 2.
func startDemo(ctx context.Context, d demoLifecycle, t demoTimings, listen func(), logger *slog.Logger) {
	if err := d.SwitchOffAll(ctx); err != nil {
		logger.Error("demo: the start-up switch-off failed; starting anyway: the sweep repeats it, and no run is accepted before the sweep has run",
			"error", err)
	}
	if ctx.Err() != nil {
		logger.Info("demo: the shutdown signal came during the start-up switch-off; not starting the listeners, the sweep or the worker")
		return
	}

	listen()

	go func() {
		sctx, cancel := context.WithTimeout(ctx, t.sweepTimeout)
		err := d.Sweep(sctx)
		cancel()
		if err != nil {
			logger.Warn("demo: the start-up sweep did not complete; starting the worker anyway", "error", err)
		}
		d.Run(ctx)
	}()
}

// logDemoSettings logs, once at start-up, demo mode's settings in force that
// are not secret: its address, the zeus poll interval (MANTEION_POLL_INTERVAL,
// which bounds how long a phase outlives its load), the run cap and the
// queue's capacity. Never the edge secret, nor anything about it.
func logDemoSettings(logger *slog.Logger, cfg demo.Config, pollInterval time.Duration) {
	logger.Info("demo mode on", "addr", cfg.Addr, "poll_interval", pollInterval.String(),
		"run_cap", cfg.RunCap.String(), "queue_capacity", cfg.Capacity)
}

// shutdownInOrder runs main's shutdown once the signal has come, also when
// startDemo stopped before the listeners (a server that never served shuts
// down at once). In this order:
//
//  1. When d is not nil, d.Shutdown under a context of its own,
//     t.shutdownTimeout long (the signal's context is already done), while
//     both servers still serve: it switches the current run's rules off,
//     which the SDKs pick up only from the operator listener, and ends every
//     event stream, which a server's Shutdown would otherwise wait out (up to
//     the streams' 10-minute maximum age). Its error is logged and changes
//     nothing else.
//  2. demoSrv's Shutdown (when not nil), then srv's, under one context of
//     serverShutdownBudget, which starts only now.
//  3. after, within what is left of that budget.
//
// It returns the servers' errors (nil when both shut down cleanly); main
// exits 1 on one. Pass d and demoSrv as nil interfaces when demo mode is off.
func shutdownInOrder(d demoLifecycle, demoSrv, srv shutdowner, t demoTimings, after func(context.Context), logger *slog.Logger) error {
	if d != nil {
		logger.Info("demo: shutting down")
		dctx, cancel := context.WithTimeout(context.Background(), t.shutdownTimeout)
		err := d.Shutdown(dctx)
		cancel()
		if err != nil {
			logger.Warn("demo: shutdown did not complete; stopping the listeners anyway", "error", err)
		} else {
			logger.Info("demo: shut down")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), serverShutdownBudget)
	defer cancel()
	var demoErr error
	if demoSrv != nil {
		if demoErr = demoSrv.Shutdown(ctx); demoErr != nil {
			logger.Error("demo server shutdown error", "error", demoErr)
		} else {
			logger.Info("demo listener closed")
		}
	}
	err := srv.Shutdown(ctx)
	if err != nil {
		logger.Error("shutdown error", "error", err)
	}
	after(ctx)
	return errors.Join(demoErr, err)
}

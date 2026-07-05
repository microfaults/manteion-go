package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// migration is a single schema migration step.
type migration struct {
	Version     int
	Description string
	SQL         string
}

// migrations is the ordered list of schema migrations.
// New migrations are appended; existing entries must never be modified.
//
// SCHEMA EPOCH 2 (2026-06): the v1 history (migrations 1–25) was consolidated
// into a single schema definition and the version counter reset. The platform
// was pre-production with no data to preserve; the old migration list lives in
// git history (internal/db/migrations.go prior to this commit). Databases
// created under epoch 1 must be dropped and recreated — Migrate refuses to
// run against them (see the epoch guard below).
//
// Epoch 2 consolidates the phase-first experiment model, manteion-owned
// workflow definitions, the attacks definition/execution split, native enum
// types for stable vocabularies, and the unified fault wire schema
// (params + network JSONB mirroring atropos-go's FaultRequest).

// migration2 adds the phase fault-event audit trail (run-details backend).
const migration2 = `
CREATE TYPE fault_event_source AS ENUM ('rule', 'cachebox', 'fault_config');

CREATE TABLE phase_fault_events (
    id         TEXT PRIMARY KEY,
    phase_id   TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    source     fault_event_source NOT NULL,
    service    TEXT NOT NULL,
    kind       TEXT NOT NULL,
    detail     JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL, -- caller-supplied; no default
    ended_at   TIMESTAMPTZ
);
CREATE INDEX idx_phase_fault_events_phase ON phase_fault_events(phase_id, started_at);
`

// migration3 adds cachebox-fidelity coverage columns to phase_service_cache:
// request_count disambiguates "0 requests served" from "all misses"; and
// recorded_entry_count is the recording coverage (entries captured on a
// baseline phase / available to replay on an isolation phase).
const migration3 = `
ALTER TABLE phase_service_cache
    ADD COLUMN request_count        BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN recorded_entry_count BIGINT NOT NULL DEFAULT 0;
`

// migration4 adds the canonical_v2 keyer to the cachebox_key_strategy enum
// (design doc Q3): the new length-prefixed SHA-256 strategy and the default
// when a frozen-service config leaves key_strategy empty. Appended last so the
// enum label order still matches model.CacheBoxKeyStrategyValues (enum_parity_test).
const migration4 = `
ALTER TYPE cachebox_key_strategy ADD VALUE IF NOT EXISTS 'canonical_v2';
`

var migrations = []migration{
	{1, "consolidated schema v2 (epoch 2 — prior history in git)", schemaV2},
	{2, "phase_fault_events audit trail + fault_event_source enum", migration2},
	{3, "phase_service_cache: request_count + recorded_entry_count (fidelity coverage)", migration3},
	{4, "cachebox_key_strategy: add canonical_v2 (default keyer, design doc Q3)", migration4},
}

// Migrate applies any pending migrations to the database.
// It creates the schema_migrations tracking table if it does not exist.
func Migrate(ctx context.Context, db *sql.DB) error {
	// Ensure the tracking table exists.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INT PRIMARY KEY,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	// Epoch guard: a database whose recorded history extends past this
	// binary's migration list was created under a previous schema epoch.
	// Its version 1 is a DIFFERENT migration than our version 1, so the
	// skip-if-applied logic below would silently leave the old schema in
	// place. Refuse loudly instead.
	var maxApplied sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT max(version) FROM schema_migrations`,
	).Scan(&maxApplied); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	if maxApplied.Valid && maxApplied.Int64 > int64(len(migrations)) {
		return fmt.Errorf(
			"schema epoch mismatch: database has migration %d applied but this binary's history ends at %d "+
				"(epoch 2 consolidated reset, 2026-06); drop and recreate the database "+
				"(DROP SCHEMA public CASCADE; CREATE SCHEMA public;)",
			maxApplied.Int64, len(migrations),
		)
	}

	for _, m := range migrations {
		// Check if already applied.
		var exists bool
		err := db.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, m.Version,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check migration %d: %w", m.Version, err)
		}
		if exists {
			continue
		}

		// Apply in a transaction.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", m.Version, err)
		}

		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("apply migration %d (%s): %w", m.Version, m.Description, err)
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, m.Version,
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %d: %w", m.Version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.Version, err)
		}

		slog.Info("migration applied", "version", m.Version, "description", m.Description)
	}

	return nil
}

// schemaV2 is the consolidated epoch-2 schema.
//
// Conventions:
//   - IDs are server-minted "{prefix}-{uuidv7}" TEXT (internal/id). Ids that
//     reference zeus-minted objects (zeus_attack_id) stay opaque TEXT.
//   - Stable vocabularies are native enum types; fault_type stays TEXT and is
//     validated by the Go fault catalog (it grows with atropos releases).
//   - Timestamps are TIMESTAMPTZ; created_at/computed_at default to now().
//   - Hard deletes only; lifecycle is status columns + FK ON DELETE policy.
//   - Single-tenant by design: service is the only scoping dimension
//     (see docs/decisions/2026-06-no-multitenancy.md).
const schemaV2 = `
-- ========== ENUM TYPES ==========

CREATE TYPE experiment_status     AS ENUM ('planned','running','completed','failed','cancelled');
CREATE TYPE phase_status          AS ENUM ('pending','running','paused','completed','failed','skipped');
CREATE TYPE fault_category        AS ENUM ('inline','network','resource');
CREATE TYPE fault_host            AS ENUM ('proxy','inline','process');
CREATE TYPE network_direction     AS ENUM ('upstream','downstream');
CREATE TYPE injection_point       AS ENUM ('ingress','egress','transient','custom');
CREATE TYPE rule_mode             AS ENUM ('inline','background');
CREATE TYPE rule_action_type      AS ENUM ('fault_spec','fault_composition','cachebox');
CREATE TYPE start_policy          AS ENUM ('deduplicate_by_rule','always_start');
CREATE TYPE cachebox_mode         AS ENUM ('passthrough','replay','replay_with_delay');
CREATE TYPE cachebox_key_strategy AS ENUM ('exact','exact_with_host','exact_with_body');
CREATE TYPE execution_mode        AS ENUM ('parallel','sequential');
CREATE TYPE trace_backend         AS ENUM ('jaeger','prometheus','tempo');
CREATE TYPE fault_config_status   AS ENUM ('ready','active','completed','cancelled');
-- fault_type intentionally stays TEXT: the Go fault catalog (backed by
-- atropos-go/faultparams) is the validator, so new atropos fault types do
-- not require a migration.

-- ========== FAULT DOMAIN ==========

-- Atomic fault definition mapping to exactly one atropos fault type.
-- Common knobs (durations, ramps, host) are first-class columns; the
-- network envelope and type-specific params are JSONB validated by the
-- Go fault catalog — mirroring the wire shape (atropos FaultRequest).
CREATE TABLE fault_specs (
    id           TEXT PRIMARY KEY,                  -- 'spec-<uuidv7>'
    name         TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    category     fault_category NOT NULL,
    fault_type   TEXT NOT NULL,
    host         fault_host,
    params       JSONB NOT NULL,                    -- per-(category,fault_type); faultparams schema
    network      JSONB,                             -- {target, direction, scope}; network category only
    duration_ms  BIGINT,
    ramp_up_ms   BIGINT,
    ramp_down_ms BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fault_specs_network_check CHECK (category = 'network' OR network IS NULL),
    CONSTRAINT fault_specs_network_direction_check CHECK (
        network IS NULL
        OR network->>'direction' IS NULL
        OR network->>'direction' IN ('upstream','downstream'))
);

-- Groups faults for parallel/sequential execution; max tree depth 3
-- (enforced in model.ValidateComposition and ruleconv).
CREATE TABLE fault_compositions (
    id             TEXT PRIMARY KEY,                -- 'comp-<uuidv7>'
    name           TEXT NOT NULL,
    execution_mode execution_mode NOT NULL,
    duration_ms    BIGINT DEFAULT 0,
    ramp_up_ms     BIGINT DEFAULT 0,
    ramp_down_ms   BIGINT DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One ordered slot in a composition: exactly one of fault_spec / child
-- composition. BIGSERIAL is the schema's only surrogate auto-increment key —
-- members are positional rows with no natural identity.
CREATE TABLE fault_composition_members (
    id                   BIGSERIAL PRIMARY KEY,
    composition_id       TEXT NOT NULL REFERENCES fault_compositions(id) ON DELETE CASCADE,
    position             INT NOT NULL,
    fault_spec_id        TEXT REFERENCES fault_specs(id),
    child_composition_id TEXT REFERENCES fault_compositions(id),
    direction            network_direction,
    UNIQUE (composition_id, position),
    CHECK (
        (fault_spec_id IS NOT NULL AND child_composition_id IS NULL) OR
        (fault_spec_id IS NULL AND child_composition_id IS NOT NULL)
    )
);

-- ========== RULE DOMAIN ==========

-- Binds an action (fault spec / composition / cachebox) to a service +
-- match criteria. The action is a 3-way discriminated union enforced by
-- rules_action_check.
CREATE TABLE rules (
    id                    TEXT PRIMARY KEY,         -- 'rule-<uuidv7>'
    name                  TEXT NOT NULL,
    service               TEXT NOT NULL,
    enabled               BOOLEAN NOT NULL DEFAULT true,
    priority              INT NOT NULL DEFAULT 0,
    injection_point       injection_point,
    match_labels          JSONB,                    -- map[string]string, AND semantics
    action_type           rule_action_type NOT NULL DEFAULT 'fault_spec',
    fault_spec_id         TEXT REFERENCES fault_specs(id),
    fault_composition_id  TEXT REFERENCES fault_compositions(id),
    cachebox_mode         cachebox_mode,
    cachebox_key_strategy cachebox_key_strategy,
    mode                  rule_mode NOT NULL,
    start_policy          start_policy NOT NULL DEFAULT 'deduplicate_by_rule',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT rules_action_check CHECK (
        CASE action_type
            WHEN 'fault_spec'        THEN fault_spec_id IS NOT NULL AND fault_composition_id IS NULL AND cachebox_mode IS NULL
            WHEN 'fault_composition' THEN fault_composition_id IS NOT NULL AND fault_spec_id IS NULL AND cachebox_mode IS NULL
            WHEN 'cachebox'          THEN cachebox_mode IS NOT NULL AND fault_spec_id IS NULL AND fault_composition_id IS NULL
        END
    )
);
CREATE INDEX idx_rules_service ON rules(service);
CREATE INDEX idx_rules_enabled ON rules(enabled) WHERE enabled = true;

-- Singleton monotonic counter bumped in-tx on every rule mutation; the SDK
-- poll compares it for 304-vs-200. A store-wide invalidation token, not
-- per-row optimistic locking.
CREATE TABLE rule_version (
    id      INT PRIMARY KEY CHECK (id = 1),
    version BIGINT NOT NULL DEFAULT 0
);
INSERT INTO rule_version (id, version) VALUES (1, 0);

-- ========== SDK REGISTRY ==========

-- Registered atropos-go SDK instances (one per service pod). Liveness is
-- computed from last_poll_at vs poll_interval_ms; the reaper purges dead
-- rows. routes is the instance's published HTTP route inventory feeding
-- the workflow-builder catalog.
CREATE TABLE sdk_instances (
    id               TEXT PRIMARY KEY,              -- SDK-minted: "{hostname}-{8hex}"
    service          TEXT NOT NULL,
    version          TEXT NOT NULL DEFAULT '',
    address          TEXT NOT NULL DEFAULT '',
    poll_interval_ms BIGINT NOT NULL DEFAULT 10000,
    routes           JSONB,                         -- []SDKRoute
    registered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_poll_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_sdk_instances_service ON sdk_instances(service);

-- ========== LONG-RUNNING MANUAL FAULTS ==========

-- Fired explicitly (UI/API), delivered to SDKs via the poll active_faults
-- set, reconciled and watchdog-reaped SDK-side. Mirrors the unified fault
-- wire shape: params + network JSONB, durations/ramps first-class.
-- duration_ms = 0 means "until cancelled".
CREATE TABLE fault_configs (
    id                   TEXT PRIMARY KEY,          -- 'fc-<uuidv7>'
    name                 TEXT NOT NULL,
    description          TEXT NOT NULL DEFAULT '',
    service              TEXT NOT NULL,
    category             fault_category NOT NULL,
    fault_type           TEXT NOT NULL,
    params               JSONB,                     -- per-(category,fault_type); faultparams schema
    network              JSONB,                     -- {target, direction, scope}; network category only
    fault_composition_id TEXT REFERENCES fault_compositions(id) ON DELETE SET NULL,
    duration_ms          BIGINT NOT NULL DEFAULT 0,
    ramp_up_ms           BIGINT NOT NULL DEFAULT 0,
    ramp_down_ms         BIGINT NOT NULL DEFAULT 0,
    phase_id             TEXT,                      -- FK added after experiment_phases below
    status               fault_config_status NOT NULL DEFAULT 'ready',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    fired_at             TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    CONSTRAINT fault_configs_payload_check CHECK (
        params IS NOT NULL OR fault_composition_id IS NOT NULL
    ),
    CONSTRAINT fault_configs_network_check CHECK (category = 'network' OR network IS NULL)
);
CREATE INDEX idx_fault_configs_active_service
    ON fault_configs(service) WHERE status = 'active';
CREATE INDEX idx_fault_configs_reaper
    ON fault_configs(fired_at) WHERE status = 'active' AND duration_ms > 0;

-- ========== POLICY DOMAIN (WIP-frozen) ==========

-- Metric-triggered actions. The evaluation engine is deprecated/disabled by
-- default (MANTEION_POLICY_ENGINE=off); schema and CRUD endpoints are kept
-- for the eventual rebuild. See docs/decisions/2026-06-policy-engine-freeze.md.
CREATE TABLE policy_rules (
    id          TEXT PRIMARY KEY,                   -- 'policy-<uuidv7>'
    name        TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT true,
    condition   JSONB NOT NULL,                     -- {metric, operator, threshold}
    action      JSONB NOT NULL,                     -- discriminated union on action_type
    cooldown_ns BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ========== WORKFLOW DEFINITIONS (manteion-owned) ==========

-- The durable home of the zeus DSL v2 document. zeus is the validation +
-- execution runtime: create/update validate the doc against zeus's
-- stateless validate endpoint; phase start materializes it into zeus
-- (register with overwrite) before triggering runs. The whole document
-- lives in dsl — splitting fields into columns proved lossy (the epoch-1
-- workflows table silently dropped base_url/data_schema/default_delay).
CREATE TABLE workflows (
    id          TEXT PRIMARY KEY,                   -- 'wf-<uuidv7>'
    name        TEXT NOT NULL UNIQUE,
    version     TEXT NOT NULL DEFAULT '2',
    description TEXT NOT NULL DEFAULT '',
    dsl         JSONB NOT NULL,                     -- full zeus DSL v2 doc (id/name injected)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_workflows_created_at ON workflows(created_at DESC);

-- ========== EXPERIMENT DOMAIN (phase-first) ==========

-- Control-plane plan: metadata + ordered phases. Attack config and
-- measurement targets live on phases, not here.
CREATE TABLE experiments (
    id           TEXT PRIMARY KEY,                  -- 'exp-<uuidv7>'
    name         TEXT NOT NULL,
    description  TEXT,
    hypothesis   TEXT,
    created_by   TEXT,
    status       experiment_status NOT NULL DEFAULT 'planned',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
CREATE INDEX idx_experiments_status_created ON experiments(status, created_at DESC);

-- First-class ordered run unit. frozen_services ([]CacheBoxConfig JSONB)
-- encodes the experimental method: empty = baseline; entries = which
-- services run frozen (cache-box) and how. The position unique is
-- DEFERRABLE so the delete-all-then-reinsert attach pattern works in one tx.
CREATE TABLE experiment_phases (
    id              TEXT PRIMARY KEY,               -- 'phase-<uuidv7>'
    experiment_id   TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    position        INT NOT NULL,
    status          phase_status NOT NULL DEFAULT 'pending',
    frozen_services JSONB NOT NULL DEFAULT '[]',
    persist_cache   BOOLEAN NOT NULL DEFAULT FALSE,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    UNIQUE (experiment_id, name),
    CONSTRAINT experiment_phases_position_unique
        UNIQUE (experiment_id, position) DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX idx_experiment_phases_experiment ON experiment_phases(experiment_id, position);
CREATE INDEX idx_experiment_phases_running ON experiment_phases(status)
    WHERE status IN ('running','paused');

-- Now that experiment_phases exists, tie long-running fault configs to the
-- phase they (optionally) ran in.
ALTER TABLE fault_configs
    ADD CONSTRAINT fault_configs_phase_id_fkey
    FOREIGN KEY (phase_id) REFERENCES experiment_phases(id) ON DELETE SET NULL;

-- Per-(phase, workflow) attack config the orchestrator reads: "drive
-- workflow W at V vus for D sec". Association-with-attributes, not a pure
-- join. NOTE: no experiment-level workflow join table — the experiment's
-- workflow list is derivable (SELECT DISTINCT via phases).
CREATE TABLE phase_workflows (
    phase_id       TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    workflow_id    TEXT NOT NULL REFERENCES workflows(id) ON DELETE RESTRICT,
    vus            INT NOT NULL CHECK (vus > 0),
    rate_rps       DOUBLE PRECISION,
    duration_sec   INT NOT NULL CHECK (duration_sec > 0),
    target_url     TEXT,
    target_method  TEXT,
    zeus_attack_id TEXT,                            -- zeus-minted execution handle
    PRIMARY KEY (phase_id, workflow_id)
);
CREATE INDEX idx_phase_workflows_workflow ON phase_workflows(workflow_id);
CREATE INDEX idx_phase_workflows_zeus_attack ON phase_workflows(zeus_attack_id)
    WHERE zeus_attack_id IS NOT NULL;

-- Rules active during a phase. RESTRICT protects experiment provenance:
-- a rule referenced by any phase cannot be deleted.
CREATE TABLE phase_rules (
    phase_id TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    rule_id  TEXT NOT NULL REFERENCES rules(id) ON DELETE RESTRICT,
    position INT NOT NULL,
    PRIMARY KEY (phase_id, rule_id),
    CONSTRAINT phase_rules_position_unique
        UNIQUE (phase_id, position) DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX idx_phase_rules_rule ON phase_rules(rule_id);

-- ========== ATTACK DOMAIN (definition / execution split) ==========

-- Reusable attack definition (vegeta precision load). Carries no execution
-- state and no experiment association — triggering one creates an
-- attack_results row.
CREATE TABLE attacks (
    id             TEXT PRIMARY KEY,                -- 'atk-<uuidv7>'
    name           TEXT NOT NULL DEFAULT '',
    description    TEXT NOT NULL DEFAULT '',
    service        TEXT NOT NULL,
    target_url     TEXT NOT NULL,
    target_method  TEXT NOT NULL,
    target_headers JSONB,                           -- map[string]string
    rate           INT NOT NULL,
    duration_ms    BIGINT NOT NULL,
    dedup_bypass   TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One execution of an attack definition (N per attack). phase_id is the
-- optional "ran during this phase" tag — SET NULL keeps the result when
-- the phase goes away.
CREATE TABLE attack_results (
    id              TEXT PRIMARY KEY,               -- 'atkres-<uuidv7>'
    attack_id       TEXT NOT NULL REFERENCES attacks(id) ON DELETE CASCADE,
    phase_id        TEXT REFERENCES experiment_phases(id) ON DELETE SET NULL,
    zeus_attack_id  TEXT,
    meta_trace_id   TEXT,
    service         TEXT NOT NULL,
    total_requests  BIGINT NOT NULL,
    duration_ms     BIGINT NOT NULL,
    rate_actual     DOUBLE PRECISION NOT NULL,
    success_rate    DOUBLE PRECISION NOT NULL,
    status_codes    JSONB,                          -- map[status]count
    latency_p50_us  BIGINT NOT NULL,
    latency_p90_us  BIGINT NOT NULL,
    latency_p95_us  BIGINT NOT NULL,
    latency_p99_us  BIGINT NOT NULL,
    latency_min_us  BIGINT NOT NULL,
    latency_max_us  BIGINT NOT NULL,
    bytes_in_total  BIGINT NOT NULL DEFAULT 0,
    bytes_out_total BIGINT NOT NULL DEFAULT 0,
    errors          JSONB,                          -- []string
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);
CREATE INDEX idx_attack_results_attack ON attack_results(attack_id);
CREATE INDEX idx_attack_results_phase ON attack_results(phase_id)
    WHERE phase_id IS NOT NULL;

-- ========== PHASE RESULTS ==========

-- Per-(phase, workflow) end-to-end latency & throughput; upserted by the
-- result harvester (ON CONFLICT DO UPDATE, idempotent recompute).
CREATE TABLE phase_workflow_results (
    phase_id        TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    workflow_id     TEXT NOT NULL,
    request_count   BIGINT NOT NULL,
    error_count     BIGINT NOT NULL DEFAULT 0,
    error_rate      DOUBLE PRECISION NOT NULL DEFAULT 0,
    throughput_rps  DOUBLE PRECISION NOT NULL,
    latency_p50_us  BIGINT NOT NULL,
    latency_p95_us  BIGINT NOT NULL,
    latency_p99_us  BIGINT NOT NULL,
    latency_p999_us BIGINT NOT NULL,
    raw_metrics     JSONB,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (phase_id, workflow_id)
);

-- Per-(phase, service[, workflow]) request-side latency. workflow_id '' is
-- the service-wide row (PK members cannot be NULL).
CREATE TABLE phase_service_latency (
    phase_id       TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    service        TEXT NOT NULL,
    workflow_id    TEXT NOT NULL DEFAULT '',
    latency_p50_us BIGINT NOT NULL,
    latency_p95_us BIGINT NOT NULL,
    latency_p99_us BIGINT NOT NULL,
    request_count  BIGINT NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (phase_id, service, workflow_id)
);

-- Per-(phase, service) cache-box fidelity; row presence == cache engaged.
-- NOTE: phase_service_resources (cpu/mem) was dropped in the epoch reset —
-- it had no writers; re-add together with the metrics harvester if needed.
CREATE TABLE phase_service_cache (
    phase_id          TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    service           TEXT NOT NULL,
    cache_hit_rate    DOUBLE PRECISION NOT NULL,
    cache_exact_match DOUBLE PRECISION NOT NULL,
    cache_staleness   DOUBLE PRECISION NOT NULL,
    computed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (phase_id, service)
);

-- Per-experiment rollup, recomputed on phase/experiment terminal
-- transitions. worst/best p99 are max/min of per-phase p99s — NOT a true
-- experiment-level percentile (that would need merged histograms). The
-- phase FKs cascade like the experiment FK: the rollup is derived data,
-- recomputed after any deletion.
CREATE TABLE experiment_results (
    experiment_id         TEXT PRIMARY KEY REFERENCES experiments(id) ON DELETE CASCADE,
    phase_count           INT NOT NULL,
    completed_phase_count INT NOT NULL,
    total_request_count   BIGINT NOT NULL,
    total_error_count     BIGINT NOT NULL,
    overall_error_rate    DOUBLE PRECISION NOT NULL,
    worst_p99_us          BIGINT NOT NULL,
    worst_p99_phase_id    TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    best_p99_us           BIGINT NOT NULL,
    best_p99_phase_id     TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    computed_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ========== OBSERVABILITY POINTERS ==========

-- Pointer into an external trace/metrics backend (when+where to look, not
-- the data). Phase-scoped: anchors die with their phase.
CREATE TABLE trace_anchors (
    id            TEXT PRIMARY KEY,                 -- 'anchor-<uuidv7>'
    phase_id      TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    meta_trace_id TEXT NOT NULL,
    service       TEXT NOT NULL,
    backend       trace_backend NOT NULL,
    query_hint    JSONB,
    collected_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_trace_anchors_phase ON trace_anchors(phase_id);
`

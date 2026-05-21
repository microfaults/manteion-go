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
var migrations = []migration{
	{1, "initial schema", initialSchema},
	{2, "add trace_anchors index", `CREATE INDEX IF NOT EXISTS idx_trace_anchors_run ON trace_anchors(experiment_run_id);`},
	{3, "drop experiments.experiment_type", `ALTER TABLE experiments DROP COLUMN IF EXISTS experiment_type;`},
	{4, "add fault_composition duration/ramp columns", `
ALTER TABLE fault_compositions
    ADD COLUMN IF NOT EXISTS duration_ms BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ramp_up_ms BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ramp_down_ms BIGINT DEFAULT 0;
`},
	{5, "add experiment_run phase columns", `
ALTER TABLE experiment_runs
    ADD COLUMN IF NOT EXISTS phase_rules        JSONB    NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS transition_cond    JSONB,
    ADD COLUMN IF NOT EXISTS current_phase      INTEGER  NOT NULL DEFAULT 0;
`},
	{6, "add experiment_run zeus_attack_id", `ALTER TABLE experiment_runs ADD COLUMN IF NOT EXISTS zeus_attack_id TEXT;`},
	{7, "add paused status to experiment_runs", `
ALTER TABLE experiment_runs DROP CONSTRAINT IF EXISTS experiment_runs_status_check;
ALTER TABLE experiment_runs ADD CONSTRAINT experiment_runs_status_check
    CHECK (status IN ('pending','running','paused','completed','failed'));
`},
	{8, "add workload_ids and zeus_attack_ids to experiment_runs", `
ALTER TABLE experiment_runs
    ADD COLUMN IF NOT EXISTS workload_ids    JSONB,
    ADD COLUMN IF NOT EXISTS zeus_attack_ids JSONB;
`},
	{9, "add hot-path indexes", `
CREATE INDEX IF NOT EXISTS idx_experiment_runs_baseline
    ON experiment_runs(experiment_id, run_type, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_experiment_runs_status
    ON experiment_runs(status)
    WHERE status IN ('running','paused');
CREATE INDEX IF NOT EXISTS idx_contribution_results_baseline_run
    ON contribution_results(baseline_run_id);
CREATE INDEX IF NOT EXISTS idx_contribution_results_isolation_run
    ON contribution_results(isolation_run_id);
CREATE INDEX IF NOT EXISTS idx_service_run_results_run
    ON service_run_results(experiment_run_id);
CREATE INDEX IF NOT EXISTS idx_workflow_run_results_run
    ON workflow_run_results(experiment_run_id);
CREATE INDEX IF NOT EXISTS idx_policy_rules_enabled
    ON policy_rules(id) WHERE enabled = true;
`},
	{10, "add experiment_run depends_on for declarative DAG", `
ALTER TABLE experiment_runs
    ADD COLUMN IF NOT EXISTS depends_on JSONB NOT NULL DEFAULT '[]';
CREATE INDEX IF NOT EXISTS idx_experiment_runs_depends_on
    ON experiment_runs USING GIN (depends_on);
`},
	{11, "add experiment_run persist_cache opt-in flag", `
ALTER TABLE experiment_runs
    ADD COLUMN IF NOT EXISTS persist_cache BOOLEAN NOT NULL DEFAULT FALSE;
`},
	{12, "add rule action_type and cachebox columns", `
ALTER TABLE rules
    ADD COLUMN IF NOT EXISTS action_type          TEXT NOT NULL DEFAULT 'fault_spec',
    ADD COLUMN IF NOT EXISTS cachebox_mode         TEXT,
    ADD COLUMN IF NOT EXISTS cachebox_key_strategy  TEXT;

UPDATE rules SET action_type = 'fault_composition' WHERE fault_composition_id IS NOT NULL;

DO $$
DECLARE
    con_name TEXT;
BEGIN
    SELECT conname INTO con_name
    FROM pg_constraint
    WHERE conrelid = 'rules'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%fault_spec_id IS NOT NULL AND fault_composition_id IS NULL%';
    IF con_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE rules DROP CONSTRAINT %I', con_name);
    END IF;
END $$;

ALTER TABLE rules ADD CONSTRAINT rules_action_check CHECK (
    CASE action_type
        WHEN 'fault_spec'        THEN fault_spec_id IS NOT NULL AND fault_composition_id IS NULL AND cachebox_mode IS NULL
        WHEN 'fault_composition' THEN fault_composition_id IS NOT NULL AND fault_spec_id IS NULL AND cachebox_mode IS NULL
        WHEN 'cachebox'          THEN cachebox_mode IS NOT NULL AND fault_spec_id IS NULL AND fault_composition_id IS NULL
    END
);

ALTER TABLE rules ALTER COLUMN fault_spec_id DROP NOT NULL;
ALTER TABLE rules ALTER COLUMN fault_composition_id DROP NOT NULL;
`},
	{13, "remove transitional workload models and slim attacks", `
-- Drop FK references before dropping tables.
ALTER TABLE attacks DROP CONSTRAINT IF EXISTS attacks_workload_id_fkey;
ALTER TABLE experiments DROP CONSTRAINT IF EXISTS experiments_primary_workload_id_fkey;

-- Slim attacks: remove transitional columns, add zeus_attack_id.
ALTER TABLE attacks
    DROP COLUMN IF EXISTS workload_id,
    DROP COLUMN IF EXISTS auto_rule_id,
    DROP COLUMN IF EXISTS role,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS started_at,
    ADD COLUMN IF NOT EXISTS zeus_attack_id TEXT;

-- Drop role/status CHECK constraints (auto-named).
DO $$
DECLARE
    con_name TEXT;
BEGIN
    FOR con_name IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'attacks'::regclass
          AND contype = 'c'
          AND (pg_get_constraintdef(oid) LIKE '%role%' OR pg_get_constraintdef(oid) LIKE '%status%')
    LOOP
        EXECUTE format('ALTER TABLE attacks DROP CONSTRAINT IF EXISTS %I', con_name);
    END LOOP;
END $$;

-- Rename experiment columns.
ALTER TABLE experiments RENAME COLUMN primary_workload_id TO primary_workflow_id;
ALTER TABLE experiment_runs RENAME COLUMN workload_ids TO workflow_ids;

-- Drop FK on primary_workflow_id (it pointed at workloads; now references zeus workflow IDs).
ALTER TABLE experiments DROP CONSTRAINT IF EXISTS experiments_primary_workload_id_fkey;

-- Now safe to drop tables.
DROP TABLE IF EXISTS workloads;
DROP TABLE IF EXISTS personas;
DROP TABLE IF EXISTS flows;
`},
	{14, "add experiment attack config columns", `
ALTER TABLE experiments
    ADD COLUMN IF NOT EXISTS target_url     TEXT,
    ADD COLUMN IF NOT EXISTS target_method  TEXT,
    ADD COLUMN IF NOT EXISTS rate           INT,
    ADD COLUMN IF NOT EXISTS duration_sec   INT;
`},
	{15, "rename network:loss fault_type → retransmit_delay", `
UPDATE fault_specs
   SET fault_type = 'retransmit_delay'
 WHERE category = 'network' AND fault_type = 'loss';
`},
	{16, "add fault_specs.host + network envelope columns", `
ALTER TABLE fault_specs
    ADD COLUMN IF NOT EXISTS host             TEXT;

-- The network envelope (only meaningful for category='network').
-- Stored as nullable columns rather than a single JSONB to make UI
-- form validation, indexing, and constraint enforcement first-class.
ALTER TABLE fault_specs
    ADD COLUMN IF NOT EXISTS network_target    TEXT,
    ADD COLUMN IF NOT EXISTS network_direction TEXT,
    ADD COLUMN IF NOT EXISTS network_scope     DOUBLE PRECISION;

-- Default existing rows: network → proxy; inline/resource → process
-- (process = inline-host for resource faults, just a label).
UPDATE fault_specs SET host = 'proxy'   WHERE category = 'network' AND host IS NULL;
UPDATE fault_specs SET host = 'process' WHERE category IN ('inline', 'resource') AND host IS NULL;

-- Enforce host vocab. Network envelope columns must be NULL on non-network.
ALTER TABLE fault_specs
    ADD CONSTRAINT fault_specs_host_check CHECK (
        host IN ('proxy', 'inline', 'process')
    );
ALTER TABLE fault_specs
    ADD CONSTRAINT fault_specs_envelope_check CHECK (
        category = 'network' OR (
            network_target IS NULL AND
            network_direction IS NULL AND
            network_scope IS NULL
        )
    );
ALTER TABLE fault_specs
    ADD CONSTRAINT fault_specs_direction_check CHECK (
        network_direction IS NULL OR network_direction IN ('upstream', 'downstream')
    );
`},
	{17, "add rules.start_policy", `
ALTER TABLE rules
    ADD COLUMN IF NOT EXISTS start_policy TEXT NOT NULL DEFAULT 'deduplicate_by_rule';
ALTER TABLE rules
    ADD CONSTRAINT rules_start_policy_check CHECK (
        start_policy IN ('deduplicate_by_rule', 'always_start')
    );
`},
	{18, "add fault_specs.description", `
ALTER TABLE fault_specs
    ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';
`},
	{19, "rename fault_specs.config → params (fix missing DDL after code rename caused /faults/specs 500)", `
ALTER TABLE fault_specs RENAME COLUMN config TO params;
`},
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

// initialSchema is the full DDL for manteion-go v1.
const initialSchema = `
-- ========== FAULT DOMAIN ==========

CREATE TABLE IF NOT EXISTS fault_specs (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    category     TEXT NOT NULL CHECK (category IN ('inline','network','resource')),
    fault_type   TEXT NOT NULL,
    config       JSONB NOT NULL,
    duration_ms  BIGINT,
    ramp_up_ms   BIGINT,
    ramp_down_ms BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fault_compositions (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    execution_mode TEXT NOT NULL CHECK (execution_mode IN ('parallel','sequential')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fault_composition_members (
    id                   BIGSERIAL PRIMARY KEY,
    composition_id       TEXT NOT NULL REFERENCES fault_compositions(id) ON DELETE CASCADE,
    position             INT NOT NULL,
    fault_spec_id        TEXT REFERENCES fault_specs(id),
    child_composition_id TEXT REFERENCES fault_compositions(id),
    direction            TEXT CHECK (direction IN ('upstream','downstream')),
    CHECK (
        (fault_spec_id IS NOT NULL AND child_composition_id IS NULL) OR
        (fault_spec_id IS NULL AND child_composition_id IS NOT NULL)
    ),
    UNIQUE (composition_id, position)
);

-- ========== RULE DOMAIN ==========

CREATE TABLE IF NOT EXISTS rules (
    id                   TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    service              TEXT NOT NULL,
    enabled              BOOLEAN NOT NULL DEFAULT true,
    priority             INT NOT NULL DEFAULT 0,
    injection_point      TEXT CHECK (injection_point IN ('ingress','egress','transient','custom')),
    match_labels         JSONB,
    fault_spec_id        TEXT REFERENCES fault_specs(id),
    fault_composition_id TEXT REFERENCES fault_compositions(id),
    mode                 TEXT NOT NULL CHECK (mode IN ('inline','background')),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (fault_spec_id IS NOT NULL AND fault_composition_id IS NULL) OR
        (fault_spec_id IS NULL AND fault_composition_id IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_rules_service ON rules(service);
CREATE INDEX IF NOT EXISTS idx_rules_enabled ON rules(enabled) WHERE enabled = true;

-- Rule store versioning: bumps on every rule mutation.
-- SDK polling reads this to decide 304 vs 200.
CREATE TABLE IF NOT EXISTS rule_version (
    id      INT PRIMARY KEY CHECK (id = 1),
    version BIGINT NOT NULL DEFAULT 0
);
INSERT INTO rule_version (id, version) VALUES (1, 0) ON CONFLICT DO NOTHING;

-- ========== SDK DOMAIN ==========

CREATE TABLE IF NOT EXISTS sdk_instances (
    id            TEXT PRIMARY KEY,
    service       TEXT NOT NULL,
    version       TEXT NOT NULL DEFAULT '',
    address       TEXT NOT NULL DEFAULT '',
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_poll_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sdk_instances_service ON sdk_instances(service);

-- ========== WORKLOAD DOMAIN ==========

CREATE TABLE IF NOT EXISTS flows (
    id                   TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    description          TEXT,
    targets              JSONB NOT NULL,
    estimated_rps_per_vu DOUBLE PRECISION,
    steps                JSONB NOT NULL,
    thresholds           JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS personas (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    description    TEXT,
    explore_prob   DOUBLE PRECISION NOT NULL DEFAULT 0,
    engage_prob    DOUBLE PRECISION NOT NULL DEFAULT 0,
    commit_prob    DOUBLE PRECISION NOT NULL DEFAULT 0,
    repeat_prob    DOUBLE PRECISION NOT NULL DEFAULT 0,
    think_time_min INT NOT NULL DEFAULT 0,
    think_time_max INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS workloads (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    flow_id       TEXT NOT NULL REFERENCES flows(id),
    persona_id    TEXT NOT NULL REFERENCES personas(id),
    vus           INT NOT NULL,
    rate          DOUBLE PRECISION NOT NULL DEFAULT 0,
    meta_trace_id TEXT,
    status        TEXT NOT NULL CHECK (status IN ('pending','running','completed','stopped','failed')),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS attacks (
    id                TEXT PRIMARY KEY,
    workload_id       TEXT REFERENCES workloads(id),
    experiment_run_id TEXT,
    policy_rule_id    TEXT,
    service           TEXT NOT NULL,
    role              TEXT NOT NULL CHECK (role IN ('primary','background')),
    target_url        TEXT NOT NULL,
    target_method     TEXT NOT NULL,
    target_headers    JSONB,
    rate              INT NOT NULL,
    duration_ms       BIGINT NOT NULL,
    dedup_bypass      TEXT,
    meta_trace_id     TEXT,
    status            TEXT NOT NULL CHECK (status IN ('pending','running','completed','stopped')),
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS attack_results (
    attack_id       TEXT PRIMARY KEY REFERENCES attacks(id) ON DELETE CASCADE,
    service         TEXT NOT NULL,
    total_requests  BIGINT NOT NULL,
    duration_ms     BIGINT NOT NULL,
    rate_actual     DOUBLE PRECISION NOT NULL,
    success_rate    DOUBLE PRECISION NOT NULL,
    status_codes    JSONB,
    latency_p50_us  BIGINT NOT NULL,
    latency_p90_us  BIGINT NOT NULL,
    latency_p95_us  BIGINT NOT NULL,
    latency_p99_us  BIGINT NOT NULL,
    latency_min_us  BIGINT NOT NULL,
    latency_max_us  BIGINT NOT NULL,
    bytes_in_total  BIGINT NOT NULL DEFAULT 0,
    bytes_out_total BIGINT NOT NULL DEFAULT 0,
    errors          JSONB,
    completed_at    TIMESTAMPTZ NOT NULL
);

-- ========== EXPERIMENT DOMAIN ==========

CREATE TABLE IF NOT EXISTS experiments (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    description         TEXT,
    primary_workload_id TEXT NOT NULL REFERENCES workloads(id),
    status              TEXT NOT NULL CHECK (status IN
        ('planned','running','completed','failed','cancelled')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS experiment_runs (
    id              TEXT PRIMARY KEY,
    experiment_id   TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    run_type        TEXT NOT NULL CHECK (run_type IN ('baseline','isolation','combination')),
    run_index       INT NOT NULL,
    frozen_services JSONB,
    meta_trace_id   TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending','running','completed','failed')),
    node_placement  JSONB,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Deferred FK from attacks to experiment_runs.
ALTER TABLE attacks
    ADD CONSTRAINT fk_attacks_experiment_run
    FOREIGN KEY (experiment_run_id) REFERENCES experiment_runs(id);

CREATE TABLE IF NOT EXISTS workflow_run_results (
    id                TEXT PRIMARY KEY,
    experiment_run_id TEXT NOT NULL REFERENCES experiment_runs(id) ON DELETE CASCADE,
    workflow          TEXT NOT NULL,
    latency_p50_us    BIGINT NOT NULL,
    latency_p95_us    BIGINT NOT NULL,
    latency_p99_us    BIGINT NOT NULL,
    latency_p999_us   BIGINT NOT NULL,
    request_count     BIGINT NOT NULL,
    error_rate        DOUBLE PRECISION NOT NULL DEFAULT 0,
    throughput_rps    DOUBLE PRECISION NOT NULL DEFAULT 0,
    raw_metrics       JSONB
);

CREATE TABLE IF NOT EXISTS service_run_results (
    id                TEXT PRIMARY KEY,
    experiment_run_id TEXT NOT NULL REFERENCES experiment_runs(id) ON DELETE CASCADE,
    service           TEXT NOT NULL,
    workflow          TEXT,
    latency_p50_us    BIGINT,
    latency_p95_us    BIGINT,
    latency_p99_us    BIGINT,
    cpu_millicores    BIGINT,
    memory_mb         BIGINT,
    cache_hit_rate    DOUBLE PRECISION,
    cache_exact_match DOUBLE PRECISION,
    cache_staleness   DOUBLE PRECISION,
    raw_metrics       JSONB
);

CREATE TABLE IF NOT EXISTS contribution_results (
    id                 TEXT PRIMARY KEY,
    experiment_id      TEXT NOT NULL REFERENCES experiments(id),
    service            TEXT NOT NULL,
    workflow           TEXT NOT NULL,
    cachebox_mode      TEXT NOT NULL CHECK (cachebox_mode IN ('replay','replay_with_delay')),
    baseline_run_id    TEXT NOT NULL REFERENCES experiment_runs(id),
    isolation_run_id   TEXT NOT NULL REFERENCES experiment_runs(id),
    delta_p50_us       BIGINT NOT NULL,
    delta_p95_us       BIGINT NOT NULL,
    delta_p99_us       BIGINT NOT NULL,
    interaction_effect DOUBLE PRECISION,
    combination_run_id TEXT REFERENCES experiment_runs(id)
);

-- ========== TRACE DOMAIN ==========

CREATE TABLE IF NOT EXISTS trace_anchors (
    id                TEXT PRIMARY KEY,
    experiment_run_id TEXT NOT NULL REFERENCES experiment_runs(id) ON DELETE CASCADE,
    meta_trace_id     TEXT NOT NULL,
    service           TEXT NOT NULL,
    backend           TEXT NOT NULL CHECK (backend IN ('jaeger','prometheus','tempo')),
    query_hint        JSONB,
    collected_at      TIMESTAMPTZ NOT NULL
);

-- ========== POLICY DOMAIN ==========

CREATE TABLE IF NOT EXISTS policy_rules (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT true,
    condition   JSONB NOT NULL,
    action      JSONB NOT NULL,
    cooldown_ns BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Deferred FK from attacks to policy_rules.
ALTER TABLE attacks
    ADD CONSTRAINT fk_attacks_policy_rule
    FOREIGN KEY (policy_rule_id) REFERENCES policy_rules(id);
`

# expctl — experiment driver for manteion

Replaces the curl/bash experiment flow. One YAML describes fault specs, rules,
workflows, and the phased experiment; `expctl apply` creates everything in the
**leak-safe order** (rules created disabled → attached via the experiment →
enabled only after attachment — the D9 ruling as code), `--start --watch`
runs it and narrates phase transitions, drains, and verdicts; `results` pulls
everything into an `experiments/<dir>/` with an ANALYSIS.md skeleton that
carries the validity gates from `docs/research/DECOMPOSITION-VALIDITY-2026-08.md`.

## Setup

Python 3.9+, PyYAML (`pip install pyyaml`). Nothing else.

Reach manteion (ClusterIP) via the port-forward convention:

```bash
# on vm1 (or leave one running):
(nohup kubectl port-forward svc/manteion 9090:8080 > /tmp/pf-manteion.log 2>&1 &)
# from the Mac, through an ssh tunnel to vm1's :9090, expctl's default
# base URL (http://localhost:9090) works unchanged.
```

## Commands

```bash
expctl.py preflight                       # readyz, instances, running-exp check, dataset TTLs
expctl.py apply -f examples/exp4-repro-120s.yaml --dry-run
expctl.py apply -f examples/exp4-repro-120s.yaml --start --watch
expctl.py watch exp-…                     # re-attach to a running experiment
expctl.py results exp-… -o experiments/2026-08-21-exp4
expctl.py abort exp-…
expctl.py experiments | instances | workflows | specs | datasets
expctl.py rules list | enable <id> | disable <id>
expctl.py dataset-upload <dataset-id> data.ndjson
```

Run from the **faults-lab root** so workflow `file:` paths resolve (they are
tried relative to the YAML's directory first, then the CWD).

`watch --grafana-url http://localhost:3000 [--grafana-auth admin:PASS]` posts
phase start/end annotations (tags `expctl`, `exp:<id>`, `phase:<name>`) so the
hypothesis dashboard shows phase windows.

## YAML schema (see examples/)

```yaml
fault_specs:            # POST /api/v1/faults/specs, reused by name if present
  - {name, category, fault_type, params: {...}, duration_ms}
rules:                  # created DISABLED; 'enabled: true' in YAML is ignored with a warning
  - {name, service, fault_spec: <spec name>, mode: inline|background, priority?, match?}
workflows:              # POST /api/v1/workflows, reused by name; base_url injected into the DSL
  - {name, file: path/to/flow.json, base_url}
experiment:
  name, hypothesis
  phases:               # order = position; nested create with attachments
    - name: p0-baseline
      persist_cache: true            # recording baseline (drain gate applies)
      frozen_services:               # cache-box freeze (isolation phases)
        - {service, mode: replay|replay_with_delay, key_strategy: canonical_v2, mutation_policy: deny}
      rules: [<rule names or raw ids>]
      workflows: [{workflow: <name or id>, vus, duration_sec, rate_rps?}]
```

## Behavior worth knowing

- **Idempotent-ish**: specs/rules/workflows are reused by exact name match, so a
  re-apply after a failed run does not duplicate them (the experiment itself is
  always created fresh).
- **Watch is the PAUSE-gate narrator**: a degraded drain or non-VALID verdict
  prints a loud warning — never override a degraded baseline; diagnose it
  (the 07-29 incident is why).
- `--no-enable` leaves created rules disabled (inspection before arming).
- Exit codes: 0 success, 1 API/preflight failure, 2 experiment ended failed/cancelled.

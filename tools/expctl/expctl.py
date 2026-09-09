#!/usr/bin/env python3
"""expctl — drive faults-lab experiments against manteion from the command line.

Replaces the ad-hoc curl/bash flow. Declarative YAML in, running experiment out,
with the rule-lifecycle ordering that is safe by construction (D9): fault specs →
rules created DISABLED → workflows registered → experiment created with phases
(rules attached, workflows attached) → rules enabled only after attachment.

Dependencies: Python 3.9+, PyYAML. Everything else is stdlib (urllib).

Usage:
  expctl.py [--base-url URL] preflight
  expctl.py apply -f experiment.yaml [--dry-run] [--start] [--watch] [--no-enable]
  expctl.py start EXP_ID [--watch]
  expctl.py watch EXP_ID [--interval 5] [--grafana-url URL [--grafana-auth U:P]]
  expctl.py results EXP_ID -o DIR
  expctl.py abort EXP_ID
  expctl.py experiments | instances | workflows | specs | datasets
  expctl.py rules [list | enable RULE_ID | disable RULE_ID]
  expctl.py dataset-upload DATASET_ID FILE.ndjson

Base URL: --base-url, else $EXPCTL_BASE_URL, else http://localhost:9090
(the kubectl port-forward convention on vm1, also correct through an ssh tunnel).
"""

import argparse
import datetime as dt
import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

try:
    import yaml
except ImportError:  # pragma: no cover
    sys.exit("expctl: PyYAML required — pip install pyyaml")

API = "/api/v1"


# ---------------------------------------------------------------- HTTP client
class Client:
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def req(self, method: str, path: str, body=None, raw_body: bytes = None,
            content_type: str = "application/json", ok=(200, 201, 202, 204)):
        url = self.base + path
        data = raw_body
        if body is not None:
            data = json.dumps(body).encode()
        r = urllib.request.Request(url, data=data, method=method,
                                   headers={"Content-Type": content_type,
                                            "Accept": "application/json"})
        try:
            with urllib.request.urlopen(r, timeout=self.timeout) as resp:
                txt = resp.read().decode() or "null"
                if resp.status not in ok:
                    raise ApiError(method, url, resp.status, txt)
                return json.loads(txt)
        except urllib.error.HTTPError as e:
            raise ApiError(method, url, e.code, e.read().decode()[:500]) from None
        except urllib.error.URLError as e:
            raise ApiError(method, url, 0, str(e.reason)) from None
        except (OSError, http.client.HTTPException) as e:
            # Covers RemoteDisconnected / connection resets that urllib lets
            # escape raw — a 20-minute watch over an ssh tunnel WILL see these.
            raise ApiError(method, url, 0, f"connection error: {e}") from None

    def get(self, path):
        return self.req("GET", path)

    def post(self, path, body=None, **kw):
        return self.req("POST", path, body=body, **kw)

    def put(self, path, body):
        return self.req("PUT", path, body=body)


class ApiError(Exception):
    def __init__(self, method, url, status, body):
        self.status = status
        super().__init__(f"{method} {url} -> {status or 'unreachable'}: {body}")


def items_of(payload, key=None):
    """Handle both bare-list and enveloped ({data:[..]} / {key:[..]}) responses."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ([key] if key else []) + ["data", "items", "datasets", "runs", "experiments"]:
            if k and isinstance(payload.get(k), list):
                return payload[k]
    return []


# ---------------------------------------------------------------- helpers
def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def warn(msg):
    print(f"\033[33mWARN\033[0m {msg}", flush=True)


def fail(msg, code=1):
    print(f"\033[31mFAIL\033[0m {msg}", flush=True)
    sys.exit(code)


def ok(msg):
    print(f"\033[32m OK \033[0m {msg}", flush=True)


def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


def read_json_file(path, cfg_dir):
    """Resolve relative to the YAML's directory first, then CWD."""
    for cand in (os.path.join(cfg_dir, path), path):
        if os.path.exists(cand):
            with open(cand) as f:
                return json.load(f)
    raise FileNotFoundError(path)


# ---------------------------------------------------------------- preflight
def cmd_preflight(c: Client, args):
    failures = 0
    try:
        c.get("/readyz")
        ok("manteion /readyz")
    except ApiError as e:
        fail(f"manteion unreachable: {e}")

    insts = c.get(f"{API}/sdk/instances") or []
    alive = [i for i in insts if i.get("status") == "alive"]
    portless = [i for i in alive if ":" not in i.get("address", "")]
    ok(f"sdk instances: {len(insts)} registered, {len(alive)} alive")
    for i in portless:
        warn(f"portless registration (fanout-unreachable): {i['service']} {i['address']}"
             " — do not target it with rules/freezes")

    running = [e for e in items_of(c.get(f"{API}/experiments"))
               if e.get("status") == "running"]
    if running:
        failures += 1
        print(f"\033[31mFAIL\033[0m experiment already running: "
              f"{running[0]['id']} ({running[0].get('name')}) — one at a time")
    else:
        ok("no running experiment")

    try:
        dsets = items_of(c.get(f"{API}/zeus/datasets"), "datasets")
        if not dsets:
            warn("zeus reachable but has NO datasets (in-memory store — did zeus restart?)")
        for d in dsets:
            created = d.get("created_at", "")
            ttl = d.get("ttl_s")
            note = ""
            if created and ttl:
                # Pre-3.11 fromisoformat rejects nanosecond fractions — trim to µs.
                iso = re.sub(r"\.(\d{6})\d+", r".\1", created.replace("Z", "+00:00"))
                exp_at = dt.datetime.fromisoformat(iso) + dt.timedelta(seconds=int(ttl))
                left = exp_at - dt.datetime.now(dt.timezone.utc)
                note = f" — expires in {left.days}d{left.seconds // 3600}h"
                if left.days < 2:
                    warn(f"dataset {d['id']} ({d.get('name')}) near expiry{note}")
                    continue
            ok(f"dataset {d['id']} ({d.get('name')}){note}")
    except ApiError as e:
        warn(f"zeus datasets check failed (proxy/zeus down?): {e}")

    if failures:
        sys.exit(1)
    print("preflight complete")


# ---------------------------------------------------------------- apply
def resolve_by_name(existing, name, kind):
    hits = [x for x in existing if x.get("name") == name]
    if len(hits) > 1:
        warn(f"{kind} name {name!r} matches {len(hits)} — using {hits[0]['id']}")
    return hits[0] if hits else None


def cmd_apply(c: Client, args):
    cfg = load_yaml(args.file)
    cfg_dir = os.path.dirname(os.path.abspath(args.file))
    dry = args.dry_run
    plan = []

    # --- fault specs (reuse by name) ---
    spec_ids = {}
    existing_specs = c.get(f"{API}/faults/specs") or []
    for s in cfg.get("fault_specs", []):
        hit = resolve_by_name(existing_specs, s["name"], "fault_spec")
        if hit:
            spec_ids[s["name"]] = hit["id"]
            plan.append(f"spec  {s['name']} = {hit['id']} (existing)")
            continue
        if dry:
            plan.append(f"spec  {s['name']} CREATE {s.get('category')}/{s.get('fault_type')}")
            spec_ids[s["name"]] = f"<new:{s['name']}>"
        else:
            created = c.post(f"{API}/faults/specs", s)
            spec_ids[s["name"]] = created["id"]
            plan.append(f"spec  {s['name']} = {created['id']} (created)")

    # --- rules: ALWAYS created disabled; enabled only after attachment (D9) ---
    rule_ids = {}
    managed_rules = []  # ids we created and must enable post-attach
    existing_rules = c.get(f"{API}/rules") or []
    for r in cfg.get("rules", []):
        if r.get("enabled"):
            warn(f"rule {r['name']}: 'enabled: true' in YAML ignored — expctl creates "
                 "disabled and enables after attachment (leak-safe ordering)")
        hit = resolve_by_name(existing_rules, r["name"], "rule")
        if hit:
            rule_ids[r["name"]] = hit["id"]
            managed_rules.append(hit["id"])
            plan.append(f"rule  {r['name']} = {hit['id']} (existing)")
            continue
        spec_ref = r.get("fault_spec")
        if spec_ref and spec_ref not in spec_ids:
            fail(f"rule {r['name']} references unknown fault_spec {spec_ref!r}")
        body = {
            "name": r["name"],
            "service": r["service"],
            "enabled": False,
            "priority": r.get("priority", 100),
            "match": r.get("match", {}),
            "mode": r.get("mode") or ("inline" if any(
                s.get("name") == spec_ref and s.get("category") == "inline"
                for s in cfg.get("fault_specs", [])) else "background"),
        }
        if spec_ref:
            body["action"] = {"type": "fault_spec", "fault_spec_id": spec_ids[spec_ref]}
        elif r.get("action"):
            body["action"] = r["action"]
        else:
            fail(f"rule {r['name']}: needs fault_spec or action")
        if dry:
            plan.append(f"rule  {r['name']} CREATE disabled on {r['service']} ({body['mode']})")
            rule_ids[r["name"]] = f"<new:{r['name']}>"
        else:
            created = c.post(f"{API}/rules", body)
            rule_ids[r["name"]] = created["id"]
            managed_rules.append(created["id"])
            plan.append(f"rule  {r['name']} = {created['id']} (created, disabled)")

    # --- workflows (reuse by name; inject base_url into the DSL doc) ---
    wf_ids = {}
    existing_wfs = items_of(c.get(f"{API}/workflows"))
    for w in cfg.get("workflows", []):
        doc = read_json_file(w["file"], cfg_dir)
        dsl = doc.get("dsl", doc)  # accept either the tracked file wrapper or a bare doc
        if w.get("base_url"):
            dsl["base_url"] = w["base_url"]
        if isinstance(w.get("think_scale"), (int, float)):
            dsl["think_scale"] = w["think_scale"]
        hit = resolve_by_name(existing_wfs, w["name"], "workflow")
        if hit:
            # Declarative apply: the YAML wins over the stored doc, so field
            # changes (base_url, think_scale, flow edits) actually land on
            # reuse instead of being silently skipped.
            wf_ids[w["name"]] = hit["id"]
            if dry:
                plan.append(f"wf    {w['name']} = {hit['id']} (existing, would update)")
            else:
                c.put(f"{API}/workflows/{hit['id']}", {"name": w["name"], "dsl": dsl})
                plan.append(f"wf    {w['name']} = {hit['id']} (updated)")
            continue
        if dry:
            plan.append(f"wf    {w['name']} CREATE from {w['file']}")
            wf_ids[w["name"]] = f"<new:{w['name']}>"
        else:
            created = c.post(f"{API}/workflows", {"name": w["name"], "dsl": dsl})
            wf_ids[w["name"]] = created["id"]
            plan.append(f"wf    {w['name']} = {created['id']} (created)")

    # --- experiment (nested phases; rules attached by id here) ---
    e = cfg.get("experiment")
    if not e:
        print("\n".join(plan))
        fail("config has no 'experiment' block")
    phases = []
    for idx, p in enumerate(e.get("phases", [])):
        ph = {
            "name": p["name"],
            "position": idx,
            "persist_cache": bool(p.get("persist_cache", False)),
            "frozen_services": p.get("frozen_services", []),
            "rule_ids": [],
            "workflows": [],
        }
        for ref in p.get("rules", []):
            ph["rule_ids"].append(rule_ids.get(ref, ref))  # name or raw id
        for ref in p.get("rule_ids", []):
            ph["rule_ids"].append(ref)
        for row in p.get("workflows", []):
            wf_ref = row.get("workflow") or row.get("workflow_id")
            # The API also accepts an optional per-row "dataset_id" (the zeus
            # dataset that workflow's k6 run reads; omitted = the server's
            # MANTEION_ZEUS_DATASET_ID fallback) and preflights it at create
            # and start (422 dataset_missing / dataset_expiring). Not mapped
            # from the YAML yet -- plans keep using the env fallback.
            ph["workflows"].append({
                "workflow_id": wf_ids.get(wf_ref, wf_ref),
                "vus": row.get("vus", 0),
                "rate_rps": row.get("rate_rps", 0),
                "duration_sec": row.get("duration_sec", 60),
            })
        phases.append(ph)

    plan.append(f"exp   {e.get('name')}: {len(phases)} phases "
                f"({sum(1 for p in phases if p['persist_cache'])} recording, "
                f"{sum(1 for p in phases if p['frozen_services'])} frozen)")
    print("\n".join(plan))
    if dry:
        print("\n--dry-run: nothing mutated")
        return

    exp = c.post(f"{API}/experiments", {
        "name": e["name"],
        "description": e.get("description", ""),
        "hypothesis": e.get("hypothesis", ""),
        "created_by": e.get("created_by", "expctl"),
        "phases": phases,
    })
    exp_id = exp["id"]
    ok(f"experiment {exp_id} created")

    # --- enable managed rules, now that they are attached (poll predicate
    # keeps them inert until their phase runs) ---
    if args.no_enable:
        warn(f"--no-enable: {len(managed_rules)} rules left disabled")
    else:
        for rid in managed_rules:
            rule = c.get(f"{API}/rules/{rid}")
            rule["enabled"] = True
            c.put(f"{API}/rules/{rid}", rule)
        if managed_rules:
            ok(f"enabled {len(managed_rules)} attached rules")

    print(f"\nexperiment id: {exp_id}")
    if args.start:
        cmd_start(c, argparse.Namespace(exp_id=exp_id, watch=args.watch,
                                        interval=5, grafana_url=args.grafana_url,
                                        grafana_auth=args.grafana_auth))
    else:
        print(f"start with: expctl.py start {exp_id} --watch")


# ---------------------------------------------------------------- start/watch
def cmd_start(c: Client, args):
    c.post(f"{API}/experiments/{args.exp_id}/start")
    ok(f"started {args.exp_id}")
    if getattr(args, "watch", False):
        cmd_watch(c, args)


def grafana_annotate(args, text, start_ms, end_ms=None, tags=None):
    if not getattr(args, "grafana_url", None):
        return
    body = {"time": start_ms, "text": text, "tags": tags or ["expctl"]}
    if end_ms:
        body["timeEnd"] = end_ms
    r = urllib.request.Request(args.grafana_url.rstrip("/") + "/api/annotations",
                               data=json.dumps(body).encode(), method="POST",
                               headers={"Content-Type": "application/json"})
    auth = getattr(args, "grafana_auth", None)
    if auth:
        import base64
        r.add_header("Authorization",
                     "Basic " + base64.b64encode(auth.encode()).decode())
    try:
        urllib.request.urlopen(r, timeout=10).read()
    except Exception as e:  # annotations are best-effort
        warn(f"grafana annotation failed: {e}")


def cmd_watch(c: Client, args):
    exp_id = args.exp_id
    interval = getattr(args, "interval", 5)
    seen = {}          # phase_id -> last status
    reported = set()   # phase_ids whose results were printed
    started_ms = {}    # phase_id -> epoch ms (for annotation spans)
    misses = 0         # consecutive failed polls (tunnel blips must not kill the watch)
    while True:
        try:
            exp = c.get(f"{API}/experiments/{exp_id}")
        except ApiError as e:
            misses += 1
            warn(f"watch poll failed ({misses}/30): {e}")
            if misses >= 30:
                fail("watch: 30 consecutive poll failures — giving up (experiment continues server-side)")
            time.sleep(min(interval * 2, 30))
            continue
        misses = 0
        status = exp.get("status")
        for ph in exp.get("phases") or []:
            pid, pstat, pname = ph["id"], ph.get("status"), ph.get("name")
            if seen.get(pid) != pstat:
                frozen = ",".join(fs["service"] for fs in ph.get("frozen_services") or [])
                extra = f" frozen[{frozen}]" if frozen else ""
                extra += " (recording)" if ph.get("persist_cache") else ""
                log(f"phase {ph.get('position')}:{pname} -> {pstat}{extra}")
                now_ms = int(time.time() * 1000)
                if pstat == "running":
                    started_ms[pid] = now_ms
                    grafana_annotate(args, f"{pname} start", now_ms,
                                     tags=["expctl", f"exp:{exp_id}", f"phase:{pname}"])
                if pstat in ("completed", "failed") and pid in started_ms:
                    grafana_annotate(args, pname, started_ms[pid], now_ms,
                                     tags=["expctl", f"exp:{exp_id}", f"phase:{pname}"])
                seen[pid] = pstat
            if pstat in ("completed", "failed") and pid not in reported:
                reported.add(pid)
                try:
                    res = c.get(f"{API}/experiments/{exp_id}/phases/{pid}/results")
                    summarize_phase_results(pname, res)
                except ApiError as e:
                    warn(f"results for {pname}: {e}")
                for reason in run_health(c, ph).values():
                    warn(f"  load health: {reason} — client-side k6 gate breached "
                         "(distinct from drain/verdict; see run reason)")
        if status in ("completed", "failed", "cancelled"):
            log(f"experiment {exp_id} {status}")
            if status != "completed":
                sys.exit(2)
            return
        time.sleep(interval)


def summarize_phase_results(pname, res):
    drain = res.get("drain")
    if drain:
        s = drain.get("status")
        line = f"  drain: {s}"
        if s != "clean":
            line += (f"  missing={drain.get('missing_instances')} "
                     f"shortfall={drain.get('shortfall_entries')}")
            warn(line + "  <- HARD STOP: do not trust this recording; do not override")
        else:
            ok(line)
    v = res.get("verdict")
    if v:
        line = (f"  verdict: {v.get('verdict')} reasons={v.get('reasons') or []} "
                f"collision_rate={v.get('collision_rate')}")
        (ok if v.get("verdict") == "VALID" else warn)(line)
    for sc in res.get("service_cache") or []:
        log(f"  cache[{sc.get('service')}]: hit_rate={sc.get('cache_hit_rate')} "
            f"requests={sc.get('request_count')} recorded={sc.get('recorded_entry_count')}")
    for wr in res.get("workflow_results") or []:
        log(f"  wf[{wr.get('workflow_id')}]: n={wr.get('request_count')} "
            f"err={wr.get('error_rate')} p50={us_ms(wr.get('latency_p50_us'))} "
            f"p95={us_ms(wr.get('latency_p95_us'))} p99={us_ms(wr.get('latency_p99_us'))}")


def run_health(c, ph):
    """{run_id: reason} for the phase's k6 runs that completed with a reason
    set — zeus stamps 'thresholds breached' when k6 exits 99 (client-side
    gates crossed; the run itself completed and its stats are whole)."""
    out = {}
    for wf in ph.get("workflows") or []:
        run_id = wf.get("zeus_run_id")
        if not run_id:
            continue
        try:
            rn = c.get(f"{API}/zeus/runs/{run_id}")
        except ApiError:
            continue
        if rn.get("status") == "completed" and rn.get("reason"):
            out[run_id] = rn["reason"]
    return out


def us_ms(v):
    return f"{v / 1000:.1f}ms" if isinstance(v, (int, float)) and v else "-"


def ns_ms(v):
    return f"{v / 1e6:.1f}ms" if isinstance(v, (int, float)) and v else "-"


# ---------------------------------------------------------------- results
GATES = [
    "G1 reproducibility band exists (repeat run) and claims stated vs band",
    "G2 offered-load integrity: dropped_iterations == 0 every phase; VU margin >= 2x",
    "G3 reference-phase symmetry: compared against non-recording phases OR record overhead bounded",
    "G4 downstream load-relief quantified before per-edge claims",
    "G5 condition ordering: ABAB minimum / randomized across repeats",
    "G6 boundaries: fault counters flat, RSS restored, drains clean, verdicts VALID, no pod restarts",
    "G7 tail sample sizes stated with every percentile",
    "G8 distributional delay replay before any contention/intrinsic number",
    "G9 per-phase node/steal telemetry screened",
    "G10 one ledger per number (zeus e2e | SDK fidelity | prom context), never mixed",
]


def cmd_results(c: Client, args):
    outdir = args.out
    os.makedirs(outdir, exist_ok=True)
    exp = c.get(f"{API}/experiments/{args.exp_id}")
    dump(outdir, "experiment.json", exp)
    rows = []
    for ph in exp.get("phases") or []:
        pid, pname, pos = ph["id"], ph.get("name"), ph.get("position")
        try:
            res = c.get(f"{API}/experiments/{args.exp_id}/phases/{pid}/results")
        except ApiError as e:
            warn(f"phase {pname}: results unavailable: {e}")
            continue
        dump(outdir, f"phase-{pos}-{pname}.json", res)
        phase_stats = []
        health = run_health(c, ph)  # run_id -> reason (k6 threshold breach)
        for run_id, reason in health.items():
            warn(f"phase {pname}: run {run_id[-8:]} load health: {reason} — "
                 "client-side k6 gate breached (stats are whole; judge with the reason in view)")
        for wf in ph.get("workflows") or []:
            run_id = wf.get("zeus_run_id")
            if run_id:
                try:
                    stats = c.get(f"{API}/zeus/runs/{run_id}/stats")
                    dump(outdir, f"phase-{pos}-{pname}-run-{run_id[-8:]}-stats.json", stats)
                    phase_stats.append(stats)
                except ApiError as e:
                    warn(f"zeus stats {run_id}: {e}")
        drain = (res.get("drain") or {}).get("status", "-")
        verdict = (res.get("verdict") or {}).get("verdict", "-")
        wrs = res.get("workflow_results") or []
        if wrs:  # attack-driven phases: harvested rows (no dropped counter)
            for wr in wrs:
                rows.append((pos, pname, wr.get("workflow_id", "?")[-6:],
                             wr.get("request_count"), wr.get("error_rate"),
                             us_ms(wr.get("latency_p50_us")), us_ms(wr.get("latency_p95_us")),
                             us_ms(wr.get("latency_p99_us")), "-", "-", drain, verdict))
        elif phase_stats:  # k6 run-driven phases: zeus per-run stats (ns latencies)
            for st in phase_stats:
                sent, okc = st.get("requests_sent") or 0, st.get("requests_ok") or 0
                err = f"{1 - okc / sent:.3f}" if sent else "-"
                dropped = st.get("requests_dropped", 0)
                if dropped:
                    warn(f"phase {pname}: {dropped} requests DROPPED (VU pool saturated) — "
                         "gate G2 FAIL: percentiles describe surviving load only")
                breach = health.get(st.get("run_id") or "", "ok")
                rows.append((pos, pname, (st.get("workflow_id") or "?")[-6:], okc, err,
                             ns_ms(st.get("latency_p50")), ns_ms(st.get("latency_p95")),
                             ns_ms(st.get("latency_p99")), dropped, breach, drain, verdict))
        else:
            rows.append((pos, pname, "-", "-", "-", "-", "-", "-", "-", "-", drain, verdict))
    dump(outdir, "instances.json", c.get(f"{API}/sdk/instances"))

    lines = [f"# {exp.get('name')} — results ({dt.date.today()})", "",
             f"Experiment `{exp.get('id')}`, status {exp.get('status')}. "
             f"Hypothesis: {exp.get('hypothesis') or '(none recorded)'}", "",
             "| # | phase | workflow | n | err | p50 | p95 | p99 | dropped | health | drain | verdict |",
             "|---|-------|----------|---|-----|-----|-----|-----|---------|--------|-------|---------|"]
    for r in rows:
        lines.append("| " + " | ".join(str(x) for x in r) + " |")
    lines += ["", "## Findings", "", "1. TODO", "",
              "## Validity (docs/research/DECOMPOSITION-VALIDITY-2026-08.md)", ""]
    lines += [f"- [ ] {g}" for g in GATES]
    lines += ["", "## Caveats", "", "- Single run per condition unless stated.", ""]
    with open(os.path.join(outdir, "ANALYSIS.md"), "w") as f:
        f.write("\n".join(lines))
    ok(f"wrote {outdir}/ (ANALYSIS.md skeleton + raw JSON)")


def dump(outdir, name, obj):
    with open(os.path.join(outdir, name), "w") as f:
        json.dump(obj, f, indent=1)


# ---------------------------------------------------------------- small cmds
def cmd_abort(c, args):
    c.post(f"{API}/experiments/{args.exp_id}/cancel")
    ok(f"cancelled {args.exp_id}")


def cmd_experiments(c, args):
    for e in items_of(c.get(f"{API}/experiments")):
        print(f"{e['id']}  {e.get('status'):10} {e.get('name')}")


def cmd_instances(c, args):
    for i in c.get(f"{API}/sdk/instances") or []:
        print(f"{i.get('service'):26} {i.get('address'):22} {i.get('status')}")


def cmd_workflows(c, args):
    for w in items_of(c.get(f"{API}/workflows")):
        print(f"{w['id']}  {w.get('name')}")


def cmd_specs(c, args):
    for s in c.get(f"{API}/faults/specs") or []:
        print(f"{s['id']}  {s.get('category')}/{s.get('fault_type'):10} {s.get('name')}")


def cmd_datasets(c, args):
    for d in items_of(c.get(f"{API}/zeus/datasets"), "datasets"):
        print(f"{d['id']}  {d.get('name')}  ttl_s={d.get('ttl_s')}  created={d.get('created_at')}")


def cmd_dataset_upload(c, args):
    with open(args.file, "rb") as f:
        body = f.read()
    res = c.post(f"{API}/zeus/datasets/{args.dataset_id}/upload", raw_body=body,
                 content_type="application/x-ndjson")
    ok(f"uploaded {len(body)} bytes: {json.dumps(res)[:200]}")


def cmd_rules(c, args):
    if args.action == "list":
        for r in c.get(f"{API}/rules") or []:
            flag = "ENABLED " if r.get("enabled") else "disabled"
            print(f"{r['id']}  {flag} {r.get('service'):24} {r.get('name')}")
        return
    rule = c.get(f"{API}/rules/{args.rule_id}")
    rule["enabled"] = args.action == "enable"
    c.put(f"{API}/rules/{args.rule_id}", rule)
    ok(f"{args.action}d {args.rule_id} ({rule.get('name')})")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(prog="expctl", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default=os.environ.get("EXPCTL_BASE_URL",
                                                         "http://localhost:9090"))
    ap.add_argument("--timeout", type=float, default=30.0)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("preflight")

    p = sub.add_parser("apply")
    p.add_argument("-f", "--file", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--start", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--no-enable", action="store_true",
                   help="leave created rules disabled")
    p.add_argument("--grafana-url", default=os.environ.get("EXPCTL_GRAFANA_URL"))
    p.add_argument("--grafana-auth", default=os.environ.get("EXPCTL_GRAFANA_AUTH"))

    p = sub.add_parser("start")
    p.add_argument("exp_id")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--interval", type=int, default=5)
    p.add_argument("--grafana-url", default=os.environ.get("EXPCTL_GRAFANA_URL"))
    p.add_argument("--grafana-auth", default=os.environ.get("EXPCTL_GRAFANA_AUTH"))

    p = sub.add_parser("watch")
    p.add_argument("exp_id")
    p.add_argument("--interval", type=int, default=5)
    p.add_argument("--grafana-url", default=os.environ.get("EXPCTL_GRAFANA_URL"))
    p.add_argument("--grafana-auth", default=os.environ.get("EXPCTL_GRAFANA_AUTH"))

    p = sub.add_parser("results")
    p.add_argument("exp_id")
    p.add_argument("-o", "--out", required=True)

    p = sub.add_parser("abort")
    p.add_argument("exp_id")

    for name in ("experiments", "instances", "workflows", "specs", "datasets"):
        sub.add_parser(name)

    p = sub.add_parser("rules")
    p.add_argument("action", choices=["list", "enable", "disable"])
    p.add_argument("rule_id", nargs="?")

    p = sub.add_parser("dataset-upload")
    p.add_argument("dataset_id")
    p.add_argument("file")

    args = ap.parse_args()
    c = Client(args.base_url, args.timeout)
    try:
        {
            "preflight": cmd_preflight, "apply": cmd_apply, "start": cmd_start,
            "watch": cmd_watch, "results": cmd_results, "abort": cmd_abort,
            "experiments": cmd_experiments, "instances": cmd_instances,
            "workflows": cmd_workflows, "specs": cmd_specs,
            "datasets": cmd_datasets, "dataset-upload": cmd_dataset_upload,
            "rules": cmd_rules,
        }[args.cmd](c, args)
    except ApiError as e:
        fail(str(e))
    except KeyboardInterrupt:
        print("\ninterrupted")
        sys.exit(130)


if __name__ == "__main__":
    main()

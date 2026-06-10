# Decision: no multitenancy (2026-06)

**Status:** accepted.

Manteion is a single-tenant research control plane for the microfaults
platform (metastability experiments on one service mesh). There is no
org/tenant/namespace column anywhere in the schema **by design**:

- The only scoping dimension is the free-form `service` label (rules,
  SDK instances, fault configs, cache files, freeze intents).
- Auth/authz is out of scope for the lab deployment.
- A second isolation dimension would tax every table, query, and API
  surface while serving no current user.

**Revisit if** the platform is ever hosted for multiple teams or runs
experiments against more than one mesh concurrently. Tenancy would then
enter as a `tenant_id` column on `rules`, `fault_specs`, `fault_configs`,
`experiments`, and `workflows`, plus scoped SDK registration tokens —
a schema-epoch-level change.

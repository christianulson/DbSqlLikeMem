# Global Evolution Plan (TDD-first)

## Executive summary (EN)
DbSqlLikeMem already has a strong multi-provider base (6 providers), broad test surface, and an explicit roadmap for parser/executor hardening. The main opportunity is to converge existing plans into one delivery system: **capability-driven development + strict TDD + release hardening gates**. The recommended path is to prioritize SQL Core parity, then query composition, then advanced SQL and DML mutations, while continuously reducing known gaps and preserving cross-provider behavior stability.

## Resumo executivo (PT-BR)
O DbSqlLikeMem já possui uma base sólida multi-provider (6 provedores), ampla superfície de testes e um roadmap explícito para hardening de parser/executor. A principal oportunidade é convergir os planos existentes em um único sistema de entrega: **desenvolvimento guiado por capabilities + TDD rigoroso + gates de hardening para release**. O caminho recomendado é priorizar paridade de SQL Core, depois composição de consultas, depois SQL avançado e mutações DML, reduzindo gaps conhecidos continuamente e preservando estabilidade de comportamento entre provedores.

---

## Implementation progress snapshot (%)

- **Parser/AST normalization track:** ~90% (paginação, quoting de alias complexos e semântica mínima de MERGE consolidadas; próximos itens focados em refinos finais por dialeto).
- **Runtime alignment track:** ~35% (base estável; pendências relevantes em `UPDATE/DELETE ... JOIN`, JSON avançado e padronização final de erros).
- **Cross-dialect regression track:** ~70% (expansão ampla por dialeto; suíte cruzada contínua ainda depende de execução sistemática em ambiente com `dotnet`).

## 1) Current-state assessment

### 1.1 Product and architecture signals
- The project is positioned as an in-memory SQL mock framework with provider-specific ADO.NET behavior and parser+executor support for DDL/DML.
- Core architecture already exposes dialect capability hooks in parser/runtime, reducing the need for parallel abstraction layers.
- Execution plan observability exists and can be used as a quality and performance baseline for evolution.

### 1.2 Delivery maturity signals
- Documentation set is rich (getting started, provider matrix, known gaps, hardening reports, release/publishing checklists).
- There is already a phased delivery strategy (P0..P10 artifacts and gap backlog generation scripts).
- Test strategy is intentionally conservative regarding duplication (safety over aggressive deduplication), coherent with current parser/executor evolution risk.

### 1.3 Main risks observed
- Risk of fragmentation between multiple plan documents if not managed under one operating cadence.
- Risk of provider drift (feature present in one dialect but undocumented or untested in another).
- Risk of implementing advanced features before consolidating SQL Core behavior parity and deterministic ordering/typing rules.

---

## 2) Global strategy (north star)

### 2.1 Strategic objective
Deliver a predictable and auditable SQL compatibility evolution pipeline across all providers, with **TDD as the default workflow**, minimizing regressions and shortening feedback cycles.

### 2.2 Guiding principles
1. **TDD always**: write/adjust failing tests first; then implement minimal code; finally refactor safely.
2. **Capability-first**: all provider differences isolated in dialect capability gates.
3. **Cross-provider contracts**: every new feature has at least one contract test pattern reused across providers.
4. **Hardening before merge**: enforce smoke + targeted + regression tests and documentation updates.
5. **Docs as product**: compatibility and limitations are updated in the same PR as code.

---

## 3) Global phased roadmap (integrated)

## Phase A — Alignment and baseline hardening (Weeks 1-2)
**Goal:** ensure roadmap/data/documentation coherence before new feature bursts.

- Reconcile provider/version matrix across README, provider docs and version metadata.
- Generate one prioritized gap backlog from compatibility and advanced gap tests.
- Define a single canonical status board (Now / Next / Later) sourced from backlog artifacts.
- Freeze acceptance criteria template for all SQL features.

**Exit criteria:** zero baseline documentation divergence; validated prioritized backlog published.

## Phase B — SQL Core parity (Weeks 3-6)
**Goal:** stabilize highest-frequency SQL behavior in all providers.

- WHERE precedence and parentheses determinism.
- SELECT expressions (arithmetic and CASE WHEN).
- Common scalar functions per dialect (COALESCE/IFNULL/ISNULL/NVL/CONCAT).
- ORDER BY alias/ordinal deterministic rules.

**Exit criteria:** all SQL Core compatibility tests green on supported providers + regression tests for all fixes.

## Phase C — Query composition (Weeks 7-10)
**Goal:** close critical gaps in reporting/API query composition.

- GROUP BY + HAVING with consistent aggregation semantics.
- UNION (+ ORDER BY final) and UNION in subselect scenarios.
- WITH/CTE simple flows with version/capability gating.

**Exit criteria:** composition scenarios green, including negative tests for unsupported/version-gated paths.

## Phase D — Advanced SQL and type semantics (Weeks 11-16)
**Goal:** increase analytical coverage without destabilizing fundamentals.

- Window functions evolution (ranking first, then lag/lead, then frame expansion).
- Correlated subquery in SELECT list.
- CAST/coercion and collation/case-sensitivity normalization by provider.
- Date operations by dialect.

**Exit criteria:** advanced tests green where supported; explicit unsupported behavior documented and tested.

## Phase E — DML advanced mutations and return payloads (Weeks 17-22)
**Goal:** strengthen realistic write paths.

- UPSERT family completion by dialect (ON DUPLICATE / ON CONFLICT / MERGE subset).
- UPDATE/DELETE with subquery/JOIN according to dialect contracts.
- RETURNING/OUTPUT/RETURNING INTO minimal viable support per provider.

**Exit criteria:** mutation test suites stable with provider-specific assertions and known limitations tracked.

## Phase F — Performance and release industrialization (continuous)
**Goal:** keep confidence high while throughput increases.

- AST cache and executor hot-path profiling.
- Periodic performance baselines with plan metrics history.
- NuGet/VSIX/VSCode release checklist automation and gate enforcement.

**Exit criteria:** no release without hardening checklist completion and smoke matrix evidence.

---

## 4) TDD operating model (mandatory workflow)

For each feature slice:
1. **Red:** add failing tests in provider-specific suites + at least one shared contract test pattern.
2. **Green:** implement the smallest parser/executor/dialect capability change.
3. **Refactor:** remove incidental complexity, keep behavior unchanged, preserve capability boundaries.
4. **Harden:** add one regression test per fixed bug and one negative test for non-supported/version-limited behavior.
5. **Document:** update provider compatibility docs and known gaps in same PR.

Definition of Done (DoD) per slice:
- All targeted tests green.
- No new cross-provider regressions in smoke set.
- Docs updated.
- Limitations explicit.

---

## 5) Suggested governance and cadence

- **Weekly cadence:**
  - 1 planning checkpoint (backlog reprioritization by impact x effort).
  - 1 quality checkpoint (flaky/regression/perf review).
- **Biweekly release candidate cadence:**
  - branch hardening window with mandatory smoke matrix.
- **Ownership model:**
  - Core parser/executor owner.
  - Provider contract owners (MySQL, SQL Server, Oracle, Npgsql, SQLite, DB2).
  - Documentation and release owner.

KPIs:
- Gap burn-down rate by provider.
- Regression count per release candidate.
- Lead time from failing test to merged fix.
- Percentage of features delivered with full docs+tests in same PR.

---

## 6) Top 10 recommended next actions

1. Publish this integrated plan as canonical reference in `docs/README.md`.
2. Execute P0 baseline reconciliation immediately.
3. Regenerate the technical backlog from current Gap/Advanced tests.
4. Establish a shared feature acceptance template (positive + negative + version gate tests).
5. Prioritize SQL Core parity items with highest cross-provider incidence.
6. Add mandatory PR checklist enforcing TDD evidence and docs update.
7. Create a compact smoke matrix workflow for all providers on every core parser change.
8. Track unsupported behavior explicitly with tests (not only docs).
9. Reserve one hardening sprint every 2-3 feature sprints.
10. Reassess roadmap percentages monthly using objective pass/fail metrics.

---

## 7) Final recommendation

The project is in a strong position to accelerate feature delivery **without sacrificing confidence**, provided evolution is run as a single integrated program (not isolated documents), with strict TDD discipline and hardening gates as merge/release invariants.

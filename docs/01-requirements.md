# 01 — Requirements

| | |
|---|---|
| **Status** | Draft |
| **Owner** | Hossam Hosny |
| **Last updated** | 2026-11-20 |
| **Related docs** | [02-high-level.md](02-high-level.md), [03-failure-modes.md](03-failure-modes.md), [adr/](adr/) (planned) |


## 1. Purpose and scope

<!-- One paragraph: what this document defines and which system boundary it covers. -->

This document covers the requirements decision for the project including system boundaries, limitations, expectations and contracts.

## 2. Problem statement

<!-- What hurts today without this pipeline? Who feels it? Why do existing tools fall short for LLM/agent traces? -->
LLM Agents are everywhere and their usage are starting to grow to scales in which they would need telemetry to be able to reliably debug a conversation gone wrong or where it failed. This system helps developers and users debug their agents (where they failed, how much tokens they used in a particular conversation and what was the usage about)

## 3. Goals and non-goals

### 3.1 Goals

- **G1:** A tenant developer can open one agent run and see every step (LLM calls, tool calls, retrieval steps) with prompt, completion, tool input/output, model, tokens, latency, cost and errors, within 2 minutes of the run happening.
- **G2:** Tenants can see token usage and cost per tenant, per model and per day.
- **G3:** Tenants can find traces by session ID, user ID, time range, model and error status.
- **G4:** Tenants can attach eval scores and user feedback to a trace or span after it was ingested.
- **G5:** One hot tenant cannot cause other tenants to lose acknowledged data or to miss their freshness target (see 8.2).
- **G6:** Tenants bring their existing instrumentation: ingest is OTLP-native and follows the OpenTelemetry GenAI semantic conventions. No proprietary SDK is required.

### 3.2 Non-goals

<!-- Things a reader might reasonably expect but that are deliberately excluded. -->

- **NG1:** A proprietary client SDK. Tenants use OpenTelemetry SDKs and GenAI instrumentation libraries; the platform only defines the wire contract.
- **NG2:** Server-side PII detection or redaction. Payloads are stored as received; redaction is the tenant's responsibility before export (see NFR-SEC-003).
- **NG3:** Guaranteed capture of error or outlier traces for Normal-tier traffic above the daily quota. Sampling above quota is head-based and does not know the outcome of a run.
- **NG4:** Storing full prompts larger than the 1 MB span limit (e.g. full 1M-token contexts) in v1. Oversized fields are truncated and flagged.
- **NG5:** Durability across a full regional outage. Acknowledged data survives single-node and single-AZ failures only.
- **NG6:** Validated operation beyond 5,000 tenants. Components are expected to scale horizontally, but nothing beyond the design point is load-tested.
- **NG7:** Retaining raw traces beyond 14 days.
- **NG8:** Running evaluations. The platform stores and displays eval scores; it does not execute evaluators (e.g. LLM-as-judge).
- **NG9:** OTLP metrics and logs signals. v1 ingests traces only.

## 4. Users and personas

| Persona | Description | Primary needs |
|---|---|---|
| Tenant developer | An engineer on a team building an LLM/agent application, debugging behaviour in development or production | Find a specific run by session, user or trace ID; see the full step tree with payloads, tokens, latency and errors; attach eval scores and feedback |
| Tenant admin | The person who owns the team's account, keys and spend | Manage API keys; see quota usage, dropped/sampled counts and cost per model; get warned before hitting the quota; change tier |
| Platform operator | The engineer running the pipeline | Per-tenant usage and throttling visibility; SLO dashboards for freshness, ingest and query; ability to identify and contain a hot tenant |

## 5. Glossary

| Term | Definition |
|---|---|
| Tenant | A team or organisation account. The unit of isolation, quota, authentication and billing. |
| Trace / span | A trace is one end-to-end agent run, identified by a 16-byte trace ID. A span is one operation inside it (LLM call, tool call, retrieval step) with start/end time, parent span ID and attributes. |
| GenAI semantic conventions | OpenTelemetry's standard attribute names for generative-AI operations, e.g. `gen_ai.operation.name`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`. |
| Hot tenant | A tenant whose traffic exceeds its rate limit or daily quota, or whose legitimate (Pro) volume is large enough to skew shared resources. |
| Session | A tenant-supplied identifier (`session.id`) grouping the traces of one conversation. |
| User ID | A tenant-supplied, opaque identifier (`user.id`) for the end user of the tenant's application. |
| Quota | The bytes a tenant may send per UTC day with full retention guaranteed. |
| Rate limit | The maximum sustained bytes per second a tenant may send. Excess is rejected with HTTP 429 / gRPC `RESOURCE_EXHAUSTED`. |
| Consistent sampling | Keeping or dropping a span based only on its trace ID, so every span of a trace gets the same decision on every gateway node. |
| Acknowledged span | A span for which the gateway returned success. From this point the platform owns its durability. |
| Freshness | Time from a span being acknowledged to it being visible in query results. |

## 6. Assumptions and constraints

### 6.1 Assumptions

- **A1:** We assume Tenants count to not exceed 5K tenants
- **A2:** Average usage is 20% of quota
- **A3:** Peak factor is 4x of Average use
- **A4:** About 5% of tenants (250) are on the Pro tier. *(Validate: product/market assumption.)*
- **A5:** The design workload is agent loops whose agents compact their context. A typical run has n = 10 steps, each one LLM call plus one tool call, with system prompt S = 6,000, user input U = 3,000, completion C = 2,000 and tool result T = 11,000 tokens. This gives ≈ 805,000 stored tokens, 21 spans and ≈ 3.23 MB per trace. *(Validate: measure against the instrumented sample app.)*
- **A6:** Text averages ~4 bytes per token; span overhead (IDs, timestamps, attributes) is ~0.5 KB per span.
- **A7:** Tenants use a mix of models, from small open models to frontier models. Compaction keeps a single prompt at or below ~128k tokens (~500 KB) in the large majority of runs.
- **A8:** Tenants instrument with OpenTelemetry SDKs and GenAI instrumentation libraries, and apply masking (in the SDK or an OTel Collector) before export when they need it.

### 6.2 Constraints

<!-- Hard limits: protocols (OTLP gRPC/HTTP), OpenTelemetry GenAI conventions version, budget, deployment target, compliance. -->

- **C1:** Quota per day per tenant is 5GB (Normal tier) with a 5 MB/s rate limit. Pro tier: 50 GB/day with a 50 MB/s rate limit.
- **C2:** Ingest protocol is OTLP over gRPC and over HTTP (protobuf).
- **C3:** The GenAI semantic conventions are not yet stable. The platform pins a specific semconv version and documents which attributes it reads.
- **C4:** Single-region deployment on AWS.
- **C5:** The thin working slice must run on one machine (Docker Compose) for demos and load tests.

## 7. Functional requirements

<!-- One row per requirement. Acceptance criteria should be testable. -->

### 7.1 Ingestion

| ID | Requirement | Priority | Rationale | Acceptance criteria |
|---|---|---|---|---|
| FR-ING-001 | Accept OTLP trace exports over gRPC and HTTP/protobuf, authenticated by a per-tenant API key | Must | Tenants keep their existing OTel instrumentation (G6) | An unmodified OTel SDK exporter configured with endpoint + key delivers spans that appear in query results; a missing or invalid key returns 401 |
| FR-ING-002 | Reject requests larger than 4 MB; truncate prompt/completion/tool fields so no stored span exceeds 1 MB, set `truncated=true` on the span and count truncations per tenant | Must | Bounds gateway memory (in-flight memory ≤ concurrent requests × 4 MB). 4 MB matches the common gRPC default, so exporters already batch under it | A 5 MB request returns 413 / `RESOURCE_EXHAUSTED`; a span with a 2 MB prompt is stored at ≤ 1 MB with `truncated=true` |
| FR-ING-003 | Enforce the per-tenant rate limit (C1); reject excess with HTTP 429 + `Retry-After` (gRPC `RESOURCE_EXHAUSTED` with retry info) | Must | Caps what any one tenant can push into the gateway | A tenant sending 10× its rate limit has its excess rejected with 429, and its accepted rate stays within the limit (± burst allowance) |
| FR-ING-004 | Once a Normal-tier tenant exceeds its daily quota, keep 1 in 10 traces for the rest of the UTC day, chosen consistently by trace ID; drop the rest at the gateway, before the durable queue | Must | Over-quota traffic never reaches shared queue or storage (G5); a trace-ID decision is stateless and consistent across nodes | After quota is exceeded, ~10% (±1 pp over ≥ 10k traces) of traces are stored, and every stored trace has all of its spans |
| FR-ING-005 | Report spans dropped by over-quota sampling in the OTLP partial-success response (`rejected_spans`, `error_message`) instead of failing the request | Must | Tells clients data was dropped without triggering exporter retries | A sampled-out export returns success with `rejected_spans` equal to the dropped count; the exporter does not retry |
| FR-ING-006 | Return success only after the spans are durably written to the replicated queue | Must | Makes "acknowledged" mean "owned by the platform" (NFR-AVL-002) | Killing a gateway node immediately after a success response loses no acknowledged spans |
| FR-ING-007 | Provide an API to attach eval scores and user feedback (name, value, optional comment) to a trace or span, at any time within the trace's retention | Must | G4 | A score attached 7 days after ingest is returned with the trace and is usable as a search filter |

### 7.2 Processing and enrichment

| ID | Requirement | Priority | Rationale | Acceptance criteria |
|---|---|---|---|---|
| FR-PRC-001 | Compute cost per span from model and token counts, using a versioned model price table | Must | G2; most instrumentation emits tokens, not cost | For a span with known model and token counts, computed cost matches the price table |
| FR-PRC-002 | Include all received spans in token and cost aggregates, including spans dropped by over-quota sampling | Should | Keeps cost reports accurate (NFR-CST-001) without storing dropped payloads | With sampling active, aggregated tokens for a tenant match the sent total within NFR-CST-001's bound |
| FR-PRC-003 | Assemble traces correctly when spans arrive out of order (child before parent) or late | Must | Spans of one run arrive in different requests and at different nodes | A trace whose spans are delivered in reverse order, with the root 10 minutes late, shows the correct tree |
| FR-PRC-004 | Deduplicate spans delivered more than once (same trace ID + span ID) | Must | Exporters retry; delivery is at-least-once | A span sent 3 times appears once in queries and is counted once in cost |
| FR-PRC-005 | Tag stored traces with the sampling rate applied to them | Must | Lets the UI say "this trace was sampled at 10%" and lets aggregates be scaled | Traces stored while over quota carry a sampling-rate attribute of 0.1 |

### 7.3 Storage and retention

| ID | Requirement | Priority | Rationale | Acceptance criteria |
|---|---|---|---|---|
| FR-STO-001 | Data is retained for 14 days and deleted afterwards | Must | Bounds storage (≈ 101 TB raw, see §9); expiry also covers most erasure needs | No span older than 14 days (+ 24 h deletion lag) is returned by any query or remains in storage |
| FR-STO-002 | Keep token and cost aggregates longer than raw traces (duration: Q2) | Should | Monthly cost reporting needs more than 14 days | Cost per tenant/model is queryable for periods older than 14 days |
| FR-STO-003 | Store payloads as received, encrypted at rest | Must | Tenant-side redaction (NG2); payloads must be treated as sensitive | Stored payloads are byte-identical to what was sent (apart from FR-ING-002 truncation); storage encryption is verified |

### 7.4 Query and retrieval

| ID | Requirement | Priority | Rationale | Acceptance criteria |
|---|---|---|---|---|
| FR-QRY-001 | Fetch one trace by trace ID, with its full span tree, payloads, tokens, latency, cost, errors, scores and feedback | Must | G1 | A known trace ID returns every stored span in the correct tree |
| FR-QRY-002 | Search traces by session ID, user ID, time range, model, error status and score | Must | G3 | Each filter alone and combined returns exactly the matching traces in the tenant's 14-day window |
| FR-QRY-003 | Return token and cost aggregates per tenant, per model and per day | Must | G2 | Aggregates for a test tenant match the sum of its spans |
| FR-QRY-004 | Show sampling and truncation state on traces and in usage views | Should | Tenants must not mistake sampled data for complete data | A sampled trace and a truncated span are visibly marked |

### 7.5 Multi-tenancy and isolation

| ID | Requirement | Priority | Rationale | Acceptance criteria |
|---|---|---|---|---|
| FR-TEN-001 | Tenant identity for every read and write is derived from the authenticated credential, never from the request body or attributes | Must | Prevents cross-tenant writes and reads | A request with tenant A's key that claims tenant B in its payload is stored and queried as tenant A only |
| FR-TEN-002 | Enforce quotas and rate limits per tenant according to its tier | Must | G5 | Normal and Pro tenants are throttled at their own limits (C1) |
| FR-TEN-003 | Expose per-tenant usage: bytes and spans received, accepted, rejected (429), sampled out and truncated | Must | Tenants and operators can see why data is missing | Usage counters for a test tenant match its sent traffic |

### 7.6 Tenant administration and configuration

| ID | Requirement | Priority | Rationale | Acceptance criteria |
|---|---|---|---|---|
| FR-ADM-001 | Create, rotate and revoke API keys; a revoked key stops working within 2 minutes | Must | Basic credential hygiene | A revoked key gets 401 within 2 minutes |
| FR-ADM-002 | Notify the tenant admin (email and/or webhook) at 80% and at 100% of the daily quota | Must | Warn before the limit so dropping data is never a surprise | Crossing 80% and 100% each triggers exactly one notification per day |
| FR-ADM-003 | Change a tenant's tier; new limits apply within 5 minutes without a redeploy | Should | Upgrades must take effect quickly | After an upgrade to Pro, the tenant is throttled at Pro limits within 5 minutes |

## 8. Non-functional requirements

### 8.1 Scale and performance targets

| ID | Metric | Target | Measured where / how | Notes |
|---|---|---|---|---|
| NFR-PERF-001 | Sustained ingest rate (spans/s, all tenants) | ≈ 550 spans/s average (≈ 84 MB/s); ≥ 2,200 spans/s (≈ 336 MB/s) at peak | Gateway accepted-span counters; load test with synthetic tenants | Derived in §9 from A1–A6 |
| NFR-PERF-002 | Peak ingest rate per tenant (spans/s) | Normal: 5 MB/s (≈ 33 spans/s at average span size); Pro: 50 MB/s (≈ 330 spans/s) | Gateway per-tenant counters | Limits are in bytes; span count depends on span size (see Q6) |
| NFR-PERF-003 | Max span payload size (KB) | 1,024 KB per span; 4 MB per request | Gateway request validation | Prompt/completion bodies; see FR-ING-002 |
| NFR-PERF-004 | Ingest acknowledgement latency (p99, ms) | ≤ 500 ms | Gateway, from request received to response sent | Includes the durable queue write (FR-ING-006) |
| NFR-PERF-005 | End-to-end freshness: ingest to queryable (p99, s) | ≤ 120 s | Synthetic canary tenant writes a span every 10 s and polls until it is queryable | Under a hot tenant, see NFR-ISO-001 |
| NFR-PERF-006 | Query latency (p95, s) | Trace by ID ≤ 2 s; search over 14 days ≤ 5 s; cost aggregates ≤ 3 s | Query API server-side timings | Per tenant, at design-point data volume |
| NFR-PERF-007 | Number of tenants | 5,000 | Load test with 5,000 synthetic tenants at A2/A3 usage | Design point; see NG6 |

### 8.2 Tenant isolation (noisy neighbor)

<!-- Define what "one hot tenant can't degrade the rest" means in measurable terms:
     e.g. latency/freshness bounds for other tenants while one tenant exceeds its quota by N×. -->

| ID | Requirement | Target | Acceptance criteria |
|---|---|---|---|
| NFR-ISO-001 | While one Normal-tier tenant offers 10× its rate limit (50 MB/s) and is over its daily quota, other tenants are unaffected | Other tenants: freshness p99 ≤ 5 min; ingest ack latency p99 ≤ 500 ms; zero acknowledged spans lost | Load test: 5,000 tenants at design load plus one tenant at 10× rate limit for 1 hour; measure all three for the other tenants |
| NFR-ISO-002 | A Pro tenant sending at its full 50 MB/s does not degrade other tenants | Same targets as NFR-ISO-001 | Load test with one Pro tenant at 50 MB/s for 1 hour (hot-partition case) |
| NFR-ISO-003 | When total load exceeds platform capacity, over-quota and burst traffic is shed first, then capacity is shared fairly across tenants | Tenants within their quota and rate limit keep ≥ their fair share of capacity | Load test at 1.5× platform capacity: within-quota tenants see no loss of acknowledged data, and rejections go to over-quota tenants first |
| NFR-ISO-004 | Quota enforcement accuracy | A tenant overshoots its daily quota by ≤ 1% before sampling starts (counter staleness ≤ 10 s; 5 MB/s × 10 s = 50 MB = 1% of 5 GB) | Measure bytes stored at full rate past the quota boundary |

### 8.3 Availability and durability

| ID | Requirement | Target | Notes |
|---|---|---|---|
| NFR-AVL-001 | Ingest availability | 99.9% monthly (successful or correctly throttled responses) | Query availability: 99.5% monthly |
| NFR-AVL-002 | Data durability once acknowledged | Zero acknowledged spans lost under single-node or single-AZ failure | Regional outage is NG5; unacknowledged spans are the client's to retry |
| NFR-AVL-003 | RPO / RTO | RPO 0 for acknowledged spans (single-AZ failure); RTO 15 min for ingest, 1 h for query | During recovery or backlog replay, freshness may degrade to p99 ≤ 1 h |

### 8.4 Security and privacy

<!-- Consider: tenant authN/authZ, cross-tenant data access, encryption, PII in prompts/completions, redaction, data residency. -->

| ID | Requirement | Priority | Notes |
|---|---|---|---|
| NFR-SEC-001 | Every ingest and query request is authenticated by a tenant API key, and scoped server-side to that tenant | Must | See FR-TEN-001; no tenant-facing path can read another tenant's data |
| NFR-SEC-002 | TLS 1.2+ in transit; encryption at rest for all stored data | Must | |
| NFR-SEC-003 | The platform does not inspect or redact payloads; docs tell tenants to mask PII in their SDK or OTel Collector before export | Must | NG2 |
| NFR-SEC-004 | User IDs are treated as opaque, tenant-supplied strings; docs tell tenants to send a hashed or pseudonymous ID, never an email | Must | Search by user still works (FR-QRY-002) |
| NFR-SEC-005 | Erasure is satisfied by 14-day expiry (FR-STO-001) | Should | Whether an explicit delete-by-user API is needed is Q7 |

### 8.5 Cost

| ID | Requirement | Target | Notes |
|---|---|---|---|
| NFR-CST-001 | Accuracy of token and cost reports per tenant and model | Within ±5% of true usage, including periods of over-quota sampling | FR-PRC-002 |
| NFR-CST-002 | Platform infrastructure cost per ingested GB | Q8 | |

### 8.6 Operability and self-observability

| ID | Requirement | Priority | Notes |
|---|---|---|---|
| NFR-OPS-001 | Per-tenant metrics: bytes/spans received, accepted, rejected, sampled out, truncated; quota usage | Must | Basis for FR-TEN-003 and for spotting hot tenants |
| NFR-OPS-002 | Pipeline lag (queue consumer lag) and freshness SLI from the canary tenant, with SLO burn-rate alerts | Must | Measures NFR-PERF-005 and NFR-ISO-001 |
| NFR-OPS-003 | An operator can lower a tenant's rate limit or force sampling at runtime, without a redeploy | Should | Manual containment of a hot tenant |

## 9. Capacity estimates

<!-- Back-of-envelope math that justifies the targets in 8.1. Show the formula and the inputs. -->

| Quantity | Formula | Value |
|---|---|---|
| Bytes per trace | (n(S+U) + (C+T)·n(n−1)/2 + nC + nT) × 4 B + (2n+1) × 0.5 KB | (90,000 + 585,000 + 20,000 + 110,000) × 4 B + 21 × 0.5 KB ≈ **3.23 MB** |
| Share of trace bytes that is repeated history | 1 − (S + U + n(C+T)) / total tokens | 1 − 139,000 / 805,000 ≈ **83%** |
| Raw bytes per day (Normal) | 4,750 tenants × 5 GB × 20% | 4.75 TB |
| Raw bytes per day (Pro) | 250 tenants × 50 GB × 20% | 2.5 TB |
| Raw bytes per day (total) | Normal + Pro | **≈ 7.25 TB/day** |
| Average ingest rate | 7.25 TB ÷ 86,400 s | **≈ 84 MB/s** |
| Peak ingest rate | average × 4 | **≈ 336 MB/s** |
| Traces per day | 7.25 TB ÷ 3.23 MB | ≈ 2.24M |
| Spans per day | traces × 21 | **≈ 47M** |
| Spans per second | 47M ÷ 86,400; × 4 for peak | ≈ 545 average, ≈ 2,200 peak |
| Storage at full retention | 7.25 TB × 14 days | **≈ 101 TB** (before compression and replication) |
| Gateway worst case (every tenant at its rate limit) | 4,750 × 5 MB/s + 250 × 50 MB/s | ≈ 36 GB/s, ~108× planned peak (oversubscription ratio; see Q5) |
| Gateway in-flight memory bound | concurrent requests × 4 MB | Depends on gateway concurrency limit |

## 10. Out of scope

- Billing and payments (the platform produces usage and cost data; it does not invoice).
- Tenant sign-up and account management UI beyond API key and tier management.
- An evaluation engine, prompt management or a playground.
- Client-side instrumentation libraries (tenants use OpenTelemetry's).

## 11. Open questions

| # | Question | Owner | Status | Resolution |
|---|---|---|---|---|
| Q1 | What are p99 run length and the compaction threshold? These set the p99 trace size. | Hossam | Open | |
| Q2 | How long are token/cost aggregates retained (FR-STO-002)? | Hossam | Open | |
| Q3 | What happens when a Pro tenant exceeds its daily quota: the same sampling as Normal, or billed overage? | Hossam | Open | |
| Q4 | Quota day boundary: UTC for everyone, or the tenant's time zone? How do we avoid a synchronised reset at midnight UTC? | Hossam | Open | |
| Q5 | How many tenants at full rate limit at the same time must the gateway absorb (worst case ≈ 36 GB/s)? This sets gateway sizing. | Hossam | Open | |
| Q6 | Do we need a per-tenant span-count limit in addition to bytes, to stop floods of tiny spans? | Hossam | Open | |
| Q7 | Is 14-day expiry enough for erasure requests, or do we need a delete-by-user-ID API? | Hossam | Open | |
| Q8 | What is the infrastructure cost target per ingested GB? | Hossam | Open | |
| Q9 | Traces that straddle the quota threshold: keep them whole, or accept partial traces? | Hossam | Open | |
| Q10 | How late can a span arrive and still join its trace (FR-PRC-003)? Up to the 14-day retention, or a shorter window? | Hossam | Open | |
| Q11 | Do we store repeated history once (by content hash or delta) to cut the ~83% duplicate bytes? Does quota then count unique or raw bytes? | Hossam | Open | To be decided in the large-payload ADR |

## 12. Change history

| Date | Author | Change |
|---|---|---|
| 2026-11-20 | Hossam Hosny | Initial skeleton |
| 2026-11-20 | Hossam Hosny | Filled in goals, non-goals, personas, glossary, requirements, NFRs, capacity estimates and open questions |
| 2026-11-20 | Hossam Hosny | Changed Freshness target to 2 minutes instead of 1. This is to help reduce the number of files that will be stored in the Storage engine. |
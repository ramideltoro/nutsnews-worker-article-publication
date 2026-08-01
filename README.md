# nutsnews-worker-article-publication

Deployable worker-uplift publication service shell for NutsNews.

## Responsibility

Consume publication-readiness jobs, keep public visibility behind backend-owned policy and deployment controls, and produce shadow comparison output until protected cutover explicitly enables production writes.

This service establishes the publication runtime, health/status/metrics surface, non-root container, strict TypeScript tooling, exact contracts/runtime dependencies, injectable dependency boundaries, backend public-feed snapshot compatibility, and local verification around least-privilege access.

## Runtime Surface

- Consumes the contracted `publication` route and accepts only publication-stage payloads.
- Provides injectable inbox, readiness policy, database transaction, snapshot publisher, feature flag, broker outbox, broker transport, clock, and work-handler boundaries.
- Evaluates readiness against the backend-captured publication policy from `worker-uplift-api-admin-compatibility-contract`.
- Compares shadow public-feed snapshot output against the current backend `public.public_feed_snapshot` contract without creating a second authoritative snapshot schema.
- Gates readiness on an active `publication` main-queue consumer, broker lifecycle, dependency probes, database write scope, and publication write-mode status.
- Emits bounded structured events and Prometheus metrics when RabbitMQ cancels the consumer, drops its channel, or restores consumption.
- Exposes `/live`, `/ready`, `/startup`, `/metrics`, `/config-schema`, and `/status`.
- Keeps `shadow_comparison` as the hard default. Production writes require production dependency mode, configured backend/database/broker/API presence, and the protected confirmation value from backend-owned deployment.
- Contains no feed fetching, page fetching, AI generation, translation generation, general persistence, direct production snapshot SQL, Cloudflare KV writes, or legacy ingestion logic.

## Observability Contract

The publication service is the terminal-success SLO producer for the worker-uplift pipeline. Its `/metrics` endpoint exports:

- `nutsnews_worker_uplift_stage_events_total{environment,service="publication",outcome}` for the bounded outcomes `success`, `duplicate`, `invalid`, `retry`, and `dlq`;
- `nutsnews_worker_uplift_stage_latency_seconds`, a fixed-bucket Prometheus histogram with boundaries `0.01`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `10`, `30`, `60`, `120`, and `300` seconds plus `+Inf`;
- `nutsnews_worker_expected_active{environment,service="publication"}`, which is `0` in the default shadow-comparison mode and becomes `1` only with the protected production write-mode cutover;
- distinct `nutsnews_worker_health_probe` liveness, startup, and readiness series, plus the runtime-owned active-consumer signal for the contracted publication queue.

Each delivery attempt produces exactly one completing lifecycle outcome. `success` and `duplicate` are the SLO good events. The terminal denominator is `success|duplicate|invalid|failure|dlq`; `retry` is intentionally excluded because it is not terminal. Publication handler terminal failures are routed to the DLQ and therefore recorded as `dlq`. No message, article, correlation, trace, or idempotency identifier is used as a Prometheus label.

Content/feed freshness is intentionally not inferred in this process. The backend host's durable content exporter owns feed-age telemetry.

The health series are one-hot and present before the first scrape: liveness initializes healthy, startup and readiness initialize fail-closed, startup follows the service lifecycle, and readiness changes only from a real readiness evaluation or a known consumer shutdown. Telemetry, log, metric, and telemetry-flush failures are best effort and cannot change acknowledgement, idempotency, retry, DLQ, or protected publication-write behavior. Duration-less dependency events remain available in structured logs but are not forwarded into legacy duration summaries, and startup does not create a fabricated zero-millisecond dependency observation.

## Policy-Driven Gate

The captured backend policy version is `2026-07-23.worker-uplift-api-admin-compatibility-contract.v1`, based on the #68/#140 evidence. Its hold-for-translations default requires `fr`, `ja`, `de-CH`, `de`, and `el` summaries before an article can be ready.

Each publication delivery records the evaluated policy version, article version, final aggregate version, readiness status, reasons, required/available/missing languages, and shadow output. Stale policy, mismatched required-language payloads, superseded content, stale aggregate versions, invalid canonical identity, invalid enrichment policy, missing accepted approval, missing persisted source summary, and blocking processing state are rejected explicitly.

The local fixtures cover both hold-until-complete and approved non-blocking backlog policies. Replays with the same idempotency key return the recorded decision without duplicate output; out-of-order older aggregate versions are rejected without republishing.

## Public-Feed Snapshot Compatibility

The captured backend snapshot contract is `worker-uplift-api-admin-compatibility-contract` version `1`. The service treats `public.public_feed_snapshot`, `load-public-feed-snapshot-rows`, and the public reader operations `load-public-feed-snapshot` and `load-home-feed-snapshot` as the compatibility target.

Shadow output records sanitized stable identity hashes, rank/order by `snapshot_rank asc`, pagination, category filtering, publication/image eligibility, timestamps, localized summary availability, backend snapshot comparison, and legacy KV fallback comparison status. Shadow mode never refreshes the live materialized state and never writes Cloudflare KV.

Production mode can request only scoped backend Worker API operations: `uplift-publish-articles-batch`, and `uplift-refresh-public-feed-snapshot` when a snapshot refresh is required. Partial refresh failures are retryable through the same idempotency key; replay does not duplicate the publish command.

## Shadow Safety

The default service mode records a readiness evaluation and publishes shadow-comparison output only. Even a production-capable config cannot publish live snapshots through the baseline local publisher unless backend-owned runtime wiring explicitly enables cutover, production writes, and single-writer gates.

When production mode is enabled by protected backend-owned runtime, the service calls only scoped backend commands with `backend_postgres_primary`. It does not perform direct SQL against public visibility or snapshot tables.

The `/status` and `/config-schema` endpoints expose readiness policy, write mode, dependency presence booleans, role/identity names, and compatibility versions without retaining or returning database URLs, RabbitMQ URLs, backend API URLs, API tokens, or confirmation values.

## Configuration

| Variable | Default | Production | Sensitive |
| --- | --- | --- | --- |
| `NUTSNEWS_PUBLICATION_BUILD_REVISION` | `development` | required lowercase 40-character Git SHA | no |
| `NUTSNEWS_PUBLICATION_DATABASE_URL` | unset | required | yes |
| `NUTSNEWS_PUBLICATION_RABBITMQ_URL` | unset | required | yes |
| `NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL` | unset | required | yes |
| `NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN` | unset | required | yes |
| `NUTSNEWS_PUBLICATION_BACKEND_API_COMPATIBILITY_VERSION` | `worker-api-v1` | optional | no |
| `NUTSNEWS_PUBLICATION_SHADOW_SCHEMA_VERSION` | `worker-uplift-shadow-v1` | optional | no |
| `NUTSNEWS_PUBLICATION_DATABASE_ROLE` | `nutsnews_worker_publication` | optional | no |
| `NUTSNEWS_PUBLICATION_BACKEND_API_IDENTITY` | `worker-uplift-publication` | optional | no |
| `NUTSNEWS_PUBLICATION_POLICY_ID` | `worker-uplift-api-admin-compatibility-contract` | optional | no |
| `NUTSNEWS_PUBLICATION_FEATURE_FLAG` | `worker-uplift-publication-shadow` | optional | no |
| `NUTSNEWS_PUBLICATION_WRITE_MODE` | `shadow_comparison` | protected | no |
| `NUTSNEWS_PUBLICATION_PRODUCTION_WRITE_CONFIRMATION` | unset | required for production writes | no |
| `NUTSNEWS_PUBLICATION_CONCURRENCY` | `1` | optional | no |
| `NUTSNEWS_PUBLICATION_PREFETCH` | `2` | optional | no |

## Local Verification

```sh
npm ci
npm run ci
NODE_AUTH_TOKEN=<github-packages-token> npm run container:build
```

`npm run ci` runs lint, typecheck, unit tests, integration tests, build, SBOM generation, and a production dependency audit.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-publication:${GITHUB_SHA}`. This repository is deployable only through backend-owned infrastructure.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI. Workflows use least-privilege permissions and request `packages: write` only for publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.

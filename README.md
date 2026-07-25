# nutsnews-worker-article-publication

Deployable worker-uplift publication service shell for NutsNews.

## Responsibility

Consume publication-readiness jobs, keep public visibility behind backend-owned policy and deployment controls, and produce shadow comparison output until protected cutover explicitly enables production writes.

This bootstrap establishes the publication service runtime, health/status/metrics surface, non-root container, strict TypeScript tooling, exact contracts/runtime dependencies, injectable dependency boundaries, and local verification around least-privilege access. The policy-driven gate and backend public-feed snapshot compatibility are implemented in later publication issues.

## Runtime Surface

- Consumes the contracted `publication` route and accepts only publication-stage payloads.
- Provides injectable inbox, readiness policy, database transaction, snapshot publisher, feature flag, broker outbox, broker transport, clock, and work-handler boundaries.
- Gates readiness on broker lifecycle, dependency probes, database write scope, and publication write-mode status.
- Exposes `/live`, `/ready`, `/startup`, `/metrics`, `/config-schema`, and `/status`.
- Keeps `shadow_comparison` as the hard default. Production writes require production dependency mode, configured backend/database/broker/API presence, and the protected confirmation value from backend-owned deployment.
- Contains no feed fetching, page fetching, AI generation, translation generation, general persistence, direct production snapshot SQL, Cloudflare KV writes, or legacy ingestion logic.

## Shadow Safety

The local service handler records a readiness evaluation and publishes shadow-comparison output only. Even a production-capable config cannot publish live snapshots through the baseline local publisher unless backend-owned runtime wiring explicitly enables the production publisher implementation.

The `/status` and `/config-schema` endpoints expose readiness policy, write mode, dependency presence booleans, role/identity names, and compatibility versions without retaining or returning database URLs, RabbitMQ URLs, backend API URLs, API tokens, or confirmation values.

## Configuration

| Variable | Default | Production | Sensitive |
| --- | --- | --- | --- |
| `NUTSNEWS_PUBLICATION_DATABASE_URL` | unset | required | yes |
| `NUTSNEWS_PUBLICATION_RABBITMQ_URL` | unset | required | yes |
| `NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL` | unset | required | yes |
| `NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN` | unset | required | yes |
| `NUTSNEWS_PUBLICATION_BACKEND_API_COMPATIBILITY_VERSION` | `worker-api-v1` | optional | no |
| `NUTSNEWS_PUBLICATION_SHADOW_SCHEMA_VERSION` | `worker-uplift-shadow-v1` | optional | no |
| `NUTSNEWS_PUBLICATION_DATABASE_ROLE` | `nutsnews_worker_publication` | optional | no |
| `NUTSNEWS_PUBLICATION_BACKEND_API_IDENTITY` | `worker-uplift-publication` | optional | no |
| `NUTSNEWS_PUBLICATION_POLICY_ID` | `backend-publication-policy-v1` | optional | no |
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

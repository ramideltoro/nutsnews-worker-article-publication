import http from "node:http";
import { timingSafeEqual } from "node:crypto";
import type { AddressInfo } from "node:net";

import {
  runtimeHealthEndpointResponse,
  type PrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  PUBLICATION_CONFIG_SCHEMA,
  type PublicationConfig
} from "./config.js";
import {
  PUBLICATION_RECONCILIATION_PATH,
  type PublicationReconciliationRequest,
  type PublicationReconciler
} from "./reconciliation.js";
import type { PublicationService } from "./service.js";

export interface PublicationHttpServerOptions {
  readonly config: PublicationConfig;
  readonly service: PublicationService;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
  readonly reconciler?: PublicationReconciler;
  readonly reconciliationToken?: string;
}

export interface PublicationHttpServer {
  readonly server: http.Server;
  listen(): Promise<http.Server>;
  close(): Promise<void>;
  url(path?: string): string;
}

export function createPublicationHttpServer(options: PublicationHttpServerOptions): PublicationHttpServer {
  const server = http.createServer((request, response) => {
    void routeRequest(options, request, response);
  });

  return {
    server,
    listen: () => new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.config.http.port, options.config.http.host, () => {
        server.off("error", reject);
        resolve(server);
      });
    }),
    close: () => new Promise((resolve, reject) => {
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }

        resolve();
      });
    }),
    url: (path = "/") => {
      const address = server.address();

      if (!isAddressInfo(address)) {
        throw new Error("Publication HTTP server is not listening on a TCP address.");
      }

      return `http://127.0.0.1:${String(address.port)}${path}`;
    }
  };
}

async function routeRequest(
  options: PublicationHttpServerOptions,
  request: http.IncomingMessage,
  response: http.ServerResponse
): Promise<void> {
  const url = new URL(request.url ?? "/", "http://localhost");

  if (request.method === "POST" && url.pathname === PUBLICATION_RECONCILIATION_PATH) {
    await handleReconciliationRequest(options, request, response);
    return;
  }

  if (request.method !== "GET") {
    writeJson(response, 405, {
      status: "method-not-allowed"
    });
    return;
  }

  switch (url.pathname) {
    case "/live":
    case "/healthz":
      writeHealth(response, await options.service.health.liveness());
      return;
    case "/startup":
    case "/startupz":
      writeHealth(response, await options.service.health.startup());
      return;
    case "/ready":
    case "/readyz":
      writeHealth(response, await options.service.health.readiness());
      return;
    case "/metrics":
      writeText(response, 200, options.metrics?.collect() ?? "", "text/plain; version=0.0.4; charset=utf-8");
      return;
    case "/config-schema":
      writeJson(response, 200, {
        service: options.config.serviceName,
        version: options.config.serviceVersion,
        variables: PUBLICATION_CONFIG_SCHEMA
      });
      return;
    case "/status":
      writeJson(response, 200, publicStatus(options.config));
      return;
    default:
      writeJson(response, 404, {
        status: "not-found"
      });
  }
}

async function handleReconciliationRequest(
  options: PublicationHttpServerOptions,
  request: http.IncomingMessage,
  response: http.ServerResponse
): Promise<void> {
  if (options.reconciler === undefined || options.reconciliationToken === undefined) {
    writeJson(response, 503, {
      service: "publication",
      status: "not_configured",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        "publication reconciliation endpoint is not configured"
      ]
    });
    return;
  }

  if (!authorized(request.headers.authorization, options.reconciliationToken)) {
    writeJson(response, 401, {
      service: "publication",
      status: "unauthorized",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        "valid bearer token required"
      ]
    });
    return;
  }

  let body: PublicationReconciliationRequest;

  try {
    body = await readJsonBody(request);
  } catch (error: unknown) {
    writeJson(response, 400, {
      service: "publication",
      status: "failed_closed",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        error instanceof Error ? error.message : "invalid reconciliation request body"
      ]
    });
    return;
  }

  const report = await options.reconciler.reconcile(body);
  const statusCode = report.status === "applied" || report.status === "dry_run"
    ? 200
    : report.status === "kill_switch_active"
      ? 423
      : 409;

  writeJson(response, statusCode, report);
}

function authorized(header: string | undefined, expectedToken: string): boolean {
  if (!header?.startsWith("Bearer ")) {
    return false;
  }

  const provided = Buffer.from(header.slice("Bearer ".length));
  const expected = Buffer.from(expectedToken);

  return provided.length === expected.length && timingSafeEqual(provided, expected);
}

function readJsonBody(request: http.IncomingMessage): Promise<PublicationReconciliationRequest> {
  const chunks: Buffer[] = [];
  let totalBytes = 0;
  const maxBytes = 16_384;

  return new Promise((resolve, reject) => {
    request.on("data", (chunk: Buffer) => {
      totalBytes += chunk.byteLength;

      if (totalBytes > maxBytes) {
        reject(new Error("reconciliation request body is too large"));
        request.destroy();
        return;
      }

      chunks.push(chunk);
    });
    request.on("error", reject);
    request.on("end", () => {
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;

        if (!isRecord(parsed)) {
          reject(new Error("reconciliation request body must be a JSON object"));
          return;
        }

        resolve(parsed as unknown as PublicationReconciliationRequest);
      } catch {
        reject(new Error("reconciliation request body must be valid JSON"));
      }
    });
  });
}

function publicStatus(config: PublicationConfig): Readonly<Record<string, unknown>> {
  return {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    dependencyMode: config.dependencyMode,
    writeMode: config.writeMode,
    readinessPolicyId: config.readiness.policyId,
    featureFlag: config.readiness.featureFlag,
    productionWriteConfirmationPresent: config.security.productionWriteConfirmationPresent,
    dependencies: config.dependencies,
    compatibility: config.compatibility,
    security: {
      databaseRole: config.security.databaseRole,
      backendApiIdentity: config.security.backendApiIdentity
    }
  };
}

function writeHealth(
  response: http.ServerResponse,
  report: Awaited<ReturnType<PublicationService["health"]["liveness"]>>
): void {
  const endpointResponse = runtimeHealthEndpointResponse(report);
  writeJson(response, endpointResponse.statusCode, endpointResponse.body, endpointResponse.headers);
}

function writeJson(
  response: http.ServerResponse,
  statusCode: number,
  body: unknown,
  headers: Readonly<Record<string, string>> = {}
): void {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    ...headers
  });
  response.end(`${JSON.stringify(body)}\n`);
}

function writeText(
  response: http.ServerResponse,
  statusCode: number,
  body: string,
  contentType: string
): void {
  response.writeHead(statusCode, {
    "content-type": contentType,
    "cache-control": "no-store"
  });
  response.end(body);
}

function isAddressInfo(address: string | AddressInfo | null): address is AddressInfo {
  return typeof address === "object" && address !== null;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

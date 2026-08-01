import {
  createServer,
  type AddressInfo,
  type Server
} from "node:net";

import {
  describe,
  expect,
  it
} from "vitest";

import { loadPublicationConfig } from "../src/config.js";
import { createPublicationApplication } from "../src/index.js";
import {
  LocalPublicationBrokerTransport,
  createLocalPublicationDependencies
} from "../src/test-doubles.js";

describe("createPublicationApplication", () => {
  it("serves health and metrics while broker initialization is pending, then cleans up a rejected init", async () => {
    const port = await reserveTcpPort();
    const config = loadPublicationConfig({
      HOSTNAME: "publication-pending-init-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_PUBLICATION_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_PUBLICATION_HTTP_PORT: String(port),
      NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
    });
    const broker = new PendingPublicationBrokerTransport();
    const localDependencies = createLocalPublicationDependencies(config);
    let dependencyCloseCalls = 0;
    const dependencies = {
      ...localDependencies,
      brokerTransport: broker,
      close: () => {
        dependencyCloseCalls += 1;
        return Promise.resolve();
      }
    };
    const application = createPublicationApplication(config, {
      dependencies
    });
    const sigintListenersBefore = process.listenerCount("SIGINT");
    const sigtermListenersBefore = process.listenerCount("SIGTERM");
    const start = application.start();
    const initializationError = new Error("simulated broker initialization failure");

    await broker.connectStarted;

    try {
      expect(process.listenerCount("SIGINT")).toBe(sigintListenersBefore + 1);
      expect(process.listenerCount("SIGTERM")).toBe(sigtermListenersBefore + 1);

      const baseUrl = `http://127.0.0.1:${String(port)}`;
      const [live, startup, ready, metrics] = await Promise.all([
        fetch(`${baseUrl}/live`),
        fetch(`${baseUrl}/startup`),
        fetch(`${baseUrl}/ready`),
        fetch(`${baseUrl}/metrics`)
      ]);

      expect(live.status).toBe(200);
      expect(startup.status).toBe(503);
      expect(ready.status).toBe(503);
      expect(metrics.status).toBe(200);
      expect(await metrics.text()).toContain("nutsnews_worker_expected_active");
    } finally {
      broker.rejectConnect(initializationError);
    }

    await expect(start).rejects.toBe(initializationError);
    expect(broker.closeCalls).toBe(1);
    expect(dependencyCloseCalls).toBe(1);
    expect(process.listenerCount("SIGINT")).toBe(sigintListenersBefore);
    expect(process.listenerCount("SIGTERM")).toBe(sigtermListenersBefore);

    await expect(expectPortCanBeRebound(port)).resolves.toBeUndefined();
  });
});

class PendingPublicationBrokerTransport extends LocalPublicationBrokerTransport {
  readonly connectStarted: Promise<void>;
  closeCalls = 0;
  private resolveConnectStarted: (() => void) | undefined;
  private rejectPendingConnect: ((error: Error) => void) | undefined;

  constructor() {
    super();
    this.connectStarted = new Promise((resolve) => {
      this.resolveConnectStarted = resolve;
    });
  }

  override connect(): Promise<void> {
    this.resolveConnectStarted?.();

    return new Promise((_resolve, reject) => {
      this.rejectPendingConnect = reject;
    });
  }

  rejectConnect(error: Error): void {
    if (this.rejectPendingConnect === undefined) {
      throw new Error("publication broker connection has not started");
    }

    this.rejectPendingConnect(error);
  }

  override async close(): Promise<void> {
    this.closeCalls += 1;
    await super.close();
  }
}

async function reserveTcpPort(): Promise<number> {
  const server = createServer();

  await listen(server, 0);
  const address = server.address();

  if (!isAddressInfo(address)) {
    await close(server);
    throw new Error("test TCP server did not bind an address");
  }

  const port = address.port;
  await close(server);

  return port;
}

async function expectPortCanBeRebound(port: number): Promise<void> {
  const server = createServer();

  await listen(server, port);
  await close(server);
}

function listen(server: Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error !== undefined) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

function isAddressInfo(value: string | AddressInfo | null): value is AddressInfo {
  return typeof value === "object" && value !== null;
}

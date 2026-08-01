import { readFileSync } from "node:fs";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import { getRuntimePackageMetadata } from "@ramideltoro/nutsnews-worker-runtime";
import { describe, expect, it } from "vitest";

import {
  SUPPORTED_CONTRACTS_PACKAGE_VERSION,
  SUPPORTED_RUNTIME_PACKAGE_VERSION
} from "../src/index.js";

describe("package compatibility", () => {
  it("accepts the exact installed contracts and runtime releases", () => {
    expect(getContractPackageMetadata().packageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(getRuntimePackageMetadata().packageVersion).toBe(SUPPORTED_RUNTIME_PACKAGE_VERSION);
    expect(getRuntimePackageMetadata().contractsPackageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(SUPPORTED_CONTRACTS_PACKAGE_VERSION).toBe("1.0.0");
    expect(SUPPORTED_RUNTIME_PACKAGE_VERSION).toBe("1.0.0");
  });

  it("resolves one immutable GitHub Packages copy of each exact 1.0.0 dependency", () => {
    const packageJson = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as {
      readonly dependencies?: Readonly<Record<string, string>>;
      readonly overrides?: unknown;
    };
    const packageLock = JSON.parse(readFileSync(new URL("../package-lock.json", import.meta.url), "utf8")) as {
      readonly packages: Readonly<Record<string, {
        readonly integrity?: string;
        readonly resolved?: string;
        readonly version?: string;
      }>>;
    };
    const contracts = packageLock.packages["node_modules/@ramideltoro/nutsnews-worker-contracts"];
    const runtime = packageLock.packages["node_modules/@ramideltoro/nutsnews-worker-runtime"];

    expect(packageJson.dependencies?.["@ramideltoro/nutsnews-worker-contracts"]).toBe("1.0.0");
    expect(packageJson.dependencies?.["@ramideltoro/nutsnews-worker-runtime"]).toBe("1.0.0");
    expect(packageJson.overrides).toBeUndefined();
    expect(contracts?.version).toBe("1.0.0");
    expect(contracts?.resolved).toMatch(/^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-contracts\/1\.0\.0\/[a-f0-9]+$/u);
    expect(contracts?.integrity).toMatch(/^sha512-[A-Za-z0-9+/]+={0,2}$/u);
    expect(runtime?.version).toBe("1.0.0");
    expect(runtime?.resolved).toMatch(/^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-runtime\/1\.0\.0\/[a-f0-9]+$/u);
    expect(runtime?.integrity).toMatch(/^sha512-[A-Za-z0-9+/]+={0,2}$/u);
    expect(packageLock.packages["node_modules/@ramideltoro/nutsnews-worker-runtime/node_modules/@ramideltoro/nutsnews-worker-contracts"]).toBeUndefined();
  });
});

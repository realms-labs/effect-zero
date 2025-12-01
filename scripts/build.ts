import path from "node:path";
import { $ } from "bun";
import fg from "fast-glob";
import fs from "fs-extra";

const outDir = "out";
const tsOutDir = "dist";

await fs.rm(outDir, { recursive: true, force: true });
await fs.rm(tsOutDir, { recursive: true, force: true });
await $`bun run test`;
await $`bun tsc`;

// This is required to have correct paths in declaration source maps
await fs.move(tsOutDir, path.join(outDir, tsOutDir));

const packageJson = await Bun.file("package.json").json();
const version = packageJson.version as string;
const filePaths = await fg(["src/**/*.ts", "!src/internal"]);
const exports: Record<string, { import: { types: string; default: string } }> = {};
for await (const filePath of filePaths) {
  const relPath = path.relative("src", filePath);
  const importPath = relPath.replace(/\.ts$/, "");
  const basePath = path.join(tsOutDir, importPath);
  exports[`./${importPath}`] = { import: { types: `./${basePath}.d.ts`, default: `./${basePath}.js` } };
}
packageJson.exports = exports;
delete packageJson.patchedDependencies;

await Promise.all([
  Bun.write(path.join(outDir, "package.json"), JSON.stringify(packageJson, null, 2)),
  fs.copyFile("README.md", path.join(outDir, "README.md")),
  fs.copyFile("LICENSE", path.join(outDir, "LICENSE")),
  fs.copy("src", path.join(outDir, "src")),
]);

await $`npm pack . --pack-destination ..`.cwd(outDir);

await fs.remove("package.tgz");
await fs.rename(`effect-zero-${version}.tgz`, `package.tgz`);

await $`bun attw package.tgz --profile esm-only`;

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const extensionRoot = path.resolve(__dirname, '..');
const bridgeProject = path.resolve(extensionRoot, '..', 'DbSqlLikeMem.VsCodeMetadataBridge', 'DbSqlLikeMem.VsCodeMetadataBridge.csproj');
const outputDir = path.resolve(extensionRoot, 'out', 'metadata-bridge');

if (!fs.existsSync(bridgeProject)) {
  console.error(`Bridge project not found: ${bridgeProject}`);
  process.exit(1);
}

fs.mkdirSync(outputDir, { recursive: true });

const result = spawnSync('dotnet', ['publish', bridgeProject, '-c', 'Release', '-o', outputDir], {
  stdio: 'inherit',
  cwd: extensionRoot
});

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

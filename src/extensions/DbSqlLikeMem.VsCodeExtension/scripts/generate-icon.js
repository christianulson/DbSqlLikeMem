const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const sourcePath = path.join(root, 'resources', 'icon.png.base64');
const targetPath = path.join(root, 'resources', 'icon.png');

if (!fs.existsSync(sourcePath)) {
  console.error(`Icon source not found: ${sourcePath}`);
  process.exit(1);
}

const base64 = fs.readFileSync(sourcePath, 'utf8').replace(/\s+/g, '');
const bytes = Buffer.from(base64, 'base64');

if (bytes.length === 0) {
  console.error('Decoded icon is empty.');
  process.exit(1);
}

fs.writeFileSync(targetPath, bytes);
console.log(`Generated ${targetPath} (${bytes.length} bytes).`);

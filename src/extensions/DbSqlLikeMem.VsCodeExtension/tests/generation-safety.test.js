const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { ensureUniqueGenerationTargets, resolveGeneratedFilePath, sanitizeClassName } = require('../out/generation-support.js');

test('different objects cannot silently overwrite the same resolved target', () => {
  const root = path.resolve('generated');
  assert.throws(() => ensureUniqueGenerationTargets([
    path.join(root, 'OrdersModel.cs'),
    path.join(root, '.', 'OrdersModel.cs')
  ]), /Multiple objects target the same file/);
});

test('template filename cannot escape the output directory or use invalid filename characters', () => {
  for (const file of ['../Other.cs', '..\\Other.cs', '/Other.cs', 'C:\\Other.cs', 'bad:name.cs', 'trailing.']) {
    assert.throws(() => resolveGeneratedFilePath(process.cwd(), 'Models', file), /Invalid generated file name/);
  }
});

test('absolute output folder is respected while the filename remains inside it', () => {
  const output = path.resolve('Models');
  assert.equal(resolveGeneratedFilePath(path.resolve('elsewhere'), output, 'Orders.cs'), path.join(output, 'Orders.cs'));
});

test('class identifiers starting with a digit receive a valid prefix', () => {
  assert.equal(sanitizeClassName('123Orders'), '_123Orders');
  assert.equal(sanitizeClassName(''), '_');
});

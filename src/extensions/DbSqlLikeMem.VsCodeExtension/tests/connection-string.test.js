const test = require('node:test');
const assert = require('node:assert/strict');
const {
  parseSqlServerConnectionString,
  splitConnectionStringPairs,
  unquoteConnectionStringValue
} = require('../out/connection-string.js');

test('parseSqlServerConnectionString reads standard keys', () => {
  const parsed = parseSqlServerConnectionString('Server=localhost;Database=ERP;User Id=sa;Password=secret;');

  assert.equal(parsed.server, 'localhost');
  assert.equal(parsed.database, 'ERP');
  assert.equal(parsed.userId, 'sa');
  assert.equal(parsed.password, 'secret');
});

test('parseSqlServerConnectionString accepts aliases and ignores casing', () => {
  const parsed = parseSqlServerConnectionString('Data Source=tcp:db,1433;Initial Catalog=Sales;UID=app;PWD=pass');

  assert.equal(parsed.server, 'tcp:db,1433');
  assert.equal(parsed.database, 'Sales');
  assert.equal(parsed.userId, 'app');
  assert.equal(parsed.password, 'pass');
});

test('parseSqlServerConnectionString keeps semicolons inside quotes', () => {
  const parsed = parseSqlServerConnectionString('Server=localhost;Password="a;b";Database=ERP;');

  assert.equal(parsed.password, 'a;b');
  assert.equal(parsed.database, 'ERP');
});

test('parseSqlServerConnectionString keeps semicolons inside single quotes', () => {
  const parsed = parseSqlServerConnectionString("Server=localhost;Password='x;y';Database=ERP;");

  assert.equal(parsed.password, 'x;y');
  assert.equal(parsed.database, 'ERP');
});

test('parseSqlServerConnectionString keeps semicolons inside braces', () => {
  const parsed = parseSqlServerConnectionString('Server=localhost;Password={p;w};Database=ERP;');

  assert.equal(parsed.password, '{p;w}');
  assert.equal(parsed.database, 'ERP');
});

test('parseSqlServerConnectionString ignores entries without a key', () => {
  const parsed = parseSqlServerConnectionString('=orphan;Server=localhost;');

  assert.equal(parsed.server, 'localhost');
});

test('splitConnectionStringPairs returns trimmed non-empty entries', () => {
  const pairs = splitConnectionStringPairs(' A=1 ; ;B=2; ');

  assert.deepEqual(pairs, [' A=1 ', 'B=2']);
});

test('unquoteConnectionStringValue removes matching quotes only', () => {
  assert.equal(unquoteConnectionStringValue('"value"'), 'value');
  assert.equal(unquoteConnectionStringValue("'value'"), 'value');
  assert.equal(unquoteConnectionStringValue('"value'), '"value');
  assert.equal(unquoteConnectionStringValue('value'), 'value');
});

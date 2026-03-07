const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const {
  buildTemplateClassFilePath,
  buildTestClassFilePath,
  buildTestClassName,
  generateTestClassTemplate,
  sanitizeClassName
} = require('../out/generation-support.js');

test('buildTestClassName sanitizes object names using the configured suffix', () => {
  const className = buildTestClassName(
    { schema: 'dbo', name: 'vw.active-customers', objectType: 'View' },
    { targetFolder: 'src/Generated', fileSuffix: 'ViewTests' }
  );

  assert.equal(className, 'vw_active_customersViewTests');
});

test('generateTestClassTemplate emits skipped scaffold without TODO markers', () => {
  const content = generateTestClassTemplate(
    'OrdersTableTests',
    { schema: 'dbo', name: 'orders', objectType: 'Table' },
    'Company.Project.Tests'
  );

  assert.match(content, /namespace Company\.Project\.Tests/);
  assert.match(content, /Fact\(Skip = "Implement generated scenario for dbo\.orders\."\)/);
  assert.match(content, /Should_validate_dbo_orders_Table/);
  assert.match(content, /\/\/ Arrange/);
  assert.doesNotMatch(content, /TODO/i);
});

test('buildTemplateClassFilePath uses deterministic model and repository targets', () => {
  assert.equal(
    buildTemplateClassFilePath(
      'c:\\workspace',
      { schema: 'dbo', name: 'orders', objectType: 'Table' },
      'model',
      'src/Models'
    ),
    path.join('c:\\workspace', 'src/Models', 'ordersModel.cs')
  );

  assert.equal(
    buildTemplateClassFilePath(
      'c:\\workspace',
      { schema: 'dbo', name: 'orders', objectType: 'Table' },
      'repository',
      'src/Repositories'
    ),
    path.join('c:\\workspace', 'src/Repositories', 'ordersRepository.cs')
  );
});

test('buildTestClassFilePath uses mapping folder and sanitized class name', () => {
  const filePath = buildTestClassFilePath(
    'c:\\workspace',
    { schema: 'dbo', name: 'sp.refresh-cache', objectType: 'Procedure' },
    { targetFolder: 'tests/Procedures', fileSuffix: 'ProcedureTests', namespace: 'Company.Project.Tests' }
  );

  assert.equal(
    filePath,
    path.join('c:\\workspace', 'tests/Procedures', 'sp_refresh_cacheProcedureTests.cs')
  );
});

test('sanitizeClassName replaces unsupported characters with underscores', () => {
  assert.equal(sanitizeClassName('vw.active-customers.cs'), 'vw_active_customers_cs');
});

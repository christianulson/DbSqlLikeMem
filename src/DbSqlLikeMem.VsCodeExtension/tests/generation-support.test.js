const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const {
  buildTemplateClassFilePath,
  buildTestClassFilePath,
  buildTestClassName,
  evaluateGenerationConsistency,
  generateTestClassTemplate,
  resolveTemplateFileName,
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
      'src/Models',
      { databaseType: 'SqlServer', databaseName: 'ERP' }
    ),
    path.join('c:\\workspace', 'src/Models', 'OrdersModel.cs')
  );

  assert.equal(
    buildTemplateClassFilePath(
      'c:\\workspace',
      { schema: 'dbo', name: 'orders', objectType: 'Table' },
      'repository',
      'src/Repositories',
      { databaseType: 'SqlServer', databaseName: 'ERP' }
    ),
    path.join('c:\\workspace', 'src/Repositories', 'OrdersRepository.cs')
  );
});

test('resolveTemplateFileName expands placeholders for template-based generation', () => {
  const fileName = resolveTemplateFileName(
    { schema: 'sales', name: 'monthly-report', objectType: 'View' },
    'repository',
    { databaseType: 'PostgreSql', databaseName: 'Billing' },
    '{DatabaseType}_{DatabaseName}_{Schema}_{NamePascal}_{Type}_{Namespace}.g.cs',
    'Company.Project.Generated'
  );

  assert.equal(
    fileName,
    'PostgreSql_Billing_sales_MonthlyReport_View_Company.Project.Generated.g.cs'
  );
});

test('evaluateGenerationConsistency marks partial and lists the missing artifacts', () => {
  const result = evaluateGenerationConsistency(false, true, false);

  assert.equal(result.status, 'partial');
  assert.deepEqual(result.missingArtifacts, ['test', 'repository']);
});

test('evaluateGenerationConsistency marks missing when the full trio is absent', () => {
  const result = evaluateGenerationConsistency(false, false, false);

  assert.equal(result.status, 'missing');
  assert.deepEqual(result.missingArtifacts, ['test', 'model', 'repository']);
});

test('evaluateGenerationConsistency marks ok when the full trio is present', () => {
  const result = evaluateGenerationConsistency(true, true, true);

  assert.equal(result.status, 'ok');
  assert.deepEqual(result.missingArtifacts, []);
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

test('buildTestClassName supports sequence objects in the same deterministic path flow', () => {
  const className = buildTestClassName(
    { schema: 'dbo', name: 'order-seq', objectType: 'Sequence' },
    { targetFolder: 'tests/Sequences', fileSuffix: 'SequenceTests' }
  );

  assert.equal(className, 'order_seqSequenceTests');
});

test('sanitizeClassName replaces unsupported characters with underscores', () => {
  assert.equal(sanitizeClassName('vw.active-customers.cs'), 'vw_active_customers_cs');
});

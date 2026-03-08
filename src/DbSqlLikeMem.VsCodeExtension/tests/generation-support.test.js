const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const {
  buildTemplateClassFilePath,
  buildTestClassFilePath,
  buildTestClassName,
  evaluateGenerationConsistency,
  generateTestClassTemplate,
  isGeneratedArtifactMetadataAligned,
  isGeneratedArtifactStructureAligned,
  parseGeneratedArtifactMetadata,
  renderTemplateContent,
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
  assert.match(content, /\/\/ DBSqlLikeMem:Schema=dbo/);
  assert.match(content, /\/\/ DBSqlLikeMem:Object=orders/);
  assert.match(content, /\/\/ DBSqlLikeMem:Type=Table/);
  assert.match(content, /\/\/ DBSqlLikeMem:Columns=/);
  assert.match(content, /\/\/ DBSqlLikeMem:ForeignKeys=/);
  assert.match(content, /Fact\(Skip = "Implement generated scenario for dbo\.orders\."\)/);
  assert.match(content, /Should_validate_dbo_orders_Table/);
  assert.match(content, /\/\/ Arrange/);
  assert.doesNotMatch(content, /TODO/i);
});

test('renderTemplateContent prepends standardized metadata header for template-based artifacts', () => {
  const content = renderTemplateContent(
    '// {{Schema}}.{{ObjectName}}\\npublic class {{ClassName}}\\n{\\n}\\n',
    'OrdersModel',
    { schema: 'dbo', name: 'orders', objectType: 'Table' },
    { databaseType: 'SqlServer', databaseName: 'ERP' },
    'Company.Project.Generated'
  );

  assert.match(content, /\/\/ DBSqlLikeMem:Schema=dbo/);
  assert.match(content, /\/\/ DBSqlLikeMem:Object=orders/);
  assert.match(content, /\/\/ DBSqlLikeMem:Type=Table/);
  assert.match(content, /public class OrdersModel/);
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

test('parseGeneratedArtifactMetadata reads the standardized snapshot header', () => {
  const metadata = parseGeneratedArtifactMetadata(
    '// DBSqlLikeMem:Schema=sales\n// DBSqlLikeMem:Object=monthly_report\n// DBSqlLikeMem:Type=View\npublic class MonthlyReportView {}\n'
  );

  assert.deepEqual(metadata, {
    schema: 'sales',
    objectName: 'monthly_report',
    objectType: 'View',
    columns: undefined,
    foreignKeys: undefined,
    startValue: undefined,
    incrementBy: undefined,
    currentValue: undefined
  });
});

test('isGeneratedArtifactMetadataAligned detects drift against the selected object', () => {
  const content = '// DBSqlLikeMem:Schema=dbo\n// DBSqlLikeMem:Object=orders_archive\n// DBSqlLikeMem:Type=Table\npublic class OrdersModel {}\n';

  assert.equal(
    isGeneratedArtifactMetadataAligned(content, { schema: 'dbo', name: 'orders', objectType: 'Table' }),
    false
  );
});

test('renderTemplateContent stores deterministic structural snapshot when object metadata exists', () => {
  const content = renderTemplateContent(
    'public class {{ClassName}}\\n{\\n}\\n',
    'OrdersModel',
    {
      schema: 'dbo',
      name: 'orders',
      objectType: 'Table',
      columns: [
        { name: 'Name', dataType: 'nvarchar', isNullable: true, ordinalPosition: 2 },
        { name: 'Id', dataType: 'int', isNullable: false, ordinalPosition: 1 }
      ],
      foreignKeys: [
        { name: 'FK_Orders_Customers', referencedSchema: 'dbo', referencedTable: 'Customers' }
      ]
    },
    { databaseType: 'SqlServer', databaseName: 'ERP' }
  );

  assert.match(content, /\/\/ DBSqlLikeMem:Columns=Id\|int\|1\|0;Name\|nvarchar\|2\|1/);
  assert.match(content, /\/\/ DBSqlLikeMem:ForeignKeys=FK_Orders_Customers\|dbo\|Customers/);
});

test('isGeneratedArtifactStructureAligned detects structural drift in stored metadata', () => {
  const content = [
    '// DBSqlLikeMem:Schema=dbo',
    '// DBSqlLikeMem:Object=orders',
    '// DBSqlLikeMem:Type=Table',
    '// DBSqlLikeMem:Columns=Id|int|1|0;Name|nvarchar|2|1',
    '// DBSqlLikeMem:ForeignKeys=FK_Orders_Customers|dbo|Customers',
    'public class OrdersModel {}'
  ].join('\n');

  assert.equal(
    isGeneratedArtifactStructureAligned(content, {
      schema: 'dbo',
      name: 'orders',
      objectType: 'Table',
      columns: [
        { name: 'Id', dataType: 'int', isNullable: false, ordinalPosition: 1 },
        { name: 'Status', dataType: 'nvarchar', isNullable: true, ordinalPosition: 2 }
      ],
      foreignKeys: [
        { name: 'FK_Orders_Customers', referencedSchema: 'dbo', referencedTable: 'Customers' }
      ]
    }),
    false
  );
});

test('renderTemplateContent stores deterministic sequence snapshot when sequence metadata exists', () => {
  const content = renderTemplateContent(
    'public class {{ClassName}}\\n{\\n}\\n',
    'OrderSeqRepository',
    {
      schema: 'dbo',
      name: 'order_seq',
      objectType: 'Sequence',
      sequenceMetadata: {
        startValue: '1000',
        incrementBy: '5',
        currentValue: '1015'
      }
    },
    { databaseType: 'SqlServer', databaseName: 'ERP' }
  );

  assert.match(content, /\/\/ DBSqlLikeMem:StartValue=1000/);
  assert.match(content, /\/\/ DBSqlLikeMem:IncrementBy=5/);
  assert.match(content, /\/\/ DBSqlLikeMem:CurrentValue=1015/);
});

test('isGeneratedArtifactStructureAligned detects sequence metadata drift', () => {
  const content = [
    '// DBSqlLikeMem:Schema=dbo',
    '// DBSqlLikeMem:Object=order_seq',
    '// DBSqlLikeMem:Type=Sequence',
    '// DBSqlLikeMem:Columns=',
    '// DBSqlLikeMem:ForeignKeys=',
    '// DBSqlLikeMem:StartValue=1000',
    '// DBSqlLikeMem:IncrementBy=5',
    '// DBSqlLikeMem:CurrentValue=1015',
    'public class OrderSeqRepository {}'
  ].join('\n');

  assert.equal(
    isGeneratedArtifactStructureAligned(content, {
      schema: 'dbo',
      name: 'order_seq',
      objectType: 'Sequence',
      sequenceMetadata: {
        startValue: '1000',
        incrementBy: '10',
        currentValue: '1020'
      }
    }),
    false
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

import * as path from 'node:path';

export type SupportedDatabaseObjectType = 'Table' | 'View' | 'Procedure' | 'Sequence';
export type TemplateGenerationKind = 'model' | 'repository';
export type GenerationCheckStatus = 'ok' | 'partial' | 'missing' | 'drift';
export type GeneratedArtifactKind = 'test' | 'model' | 'repository';

export interface GenerationObjectReference {
  schema: string;
  name: string;
  objectType: SupportedDatabaseObjectType;
  columns?: ReadonlyArray<GenerationColumnReference>;
  foreignKeys?: ReadonlyArray<GenerationForeignKeyReference>;
  sequenceMetadata?: GenerationSequenceMetadata;
}

export interface GenerationColumnReference {
  name: string;
  dataType: string;
  isNullable: boolean;
  ordinalPosition: number;
}

export interface GenerationForeignKeyReference {
  name: string;
  referencedSchema: string;
  referencedTable: string;
}

export interface GenerationSequenceMetadata {
  startValue?: string;
  incrementBy?: string;
  currentValue?: string;
}

export interface TestObjectMappingReference {
  targetFolder: string;
  fileSuffix: string;
  namespace?: string;
}

export interface TemplateConnectionReference {
  databaseType: string;
  databaseName: string;
}

export interface GenerationConsistencyResult {
  status: GenerationCheckStatus;
  missingArtifacts: GeneratedArtifactKind[];
}

export interface GeneratedArtifactMetadata {
  schema: string;
  objectName: string;
  objectType: SupportedDatabaseObjectType;
  columns?: string;
  foreignKeys?: string;
  startValue?: string;
  incrementBy?: string;
  currentValue?: string;
}

export function sanitizeClassName(value: string): string {
  return value.replace(/[^a-zA-Z0-9_]/g, '_');
}

export function buildTestClassName(
  objectRef: GenerationObjectReference,
  objectMapping: TestObjectMappingReference
): string {
  return sanitizeClassName(objectRef.name + objectMapping.fileSuffix);
}

export function buildTestClassFilePath(
  workspaceFolder: string,
  objectRef: GenerationObjectReference,
  objectMapping: TestObjectMappingReference
): string {
  const className = buildTestClassName(objectRef, objectMapping);
  return path.join(workspaceFolder, objectMapping.targetFolder, `${className}.cs`);
}

export function buildTemplateClassFilePath(
  workspaceFolder: string,
  objectRef: GenerationObjectReference,
  kind: TemplateGenerationKind,
  targetFolder: string,
  connection: TemplateConnectionReference,
  fileNamePattern?: string,
  namespace?: string
): string {
  const fileName = resolveTemplateFileName(objectRef, kind, connection, fileNamePattern, namespace);
  return path.join(workspaceFolder, targetFolder, fileName);
}

export function evaluateGenerationConsistency(
  testFound: boolean,
  modelFound: boolean,
  repositoryFound: boolean
): GenerationConsistencyResult {
  const missingArtifacts: GeneratedArtifactKind[] = [];

  if (!testFound) {
    missingArtifacts.push('test');
  }

  if (!modelFound) {
    missingArtifacts.push('model');
  }

  if (!repositoryFound) {
    missingArtifacts.push('repository');
  }

  return {
    status: missingArtifacts.length === 0 ? 'ok' : (missingArtifacts.length === 3 ? 'missing' : 'partial'),
    missingArtifacts
  };
}

export function resolveTemplateFileName(
  objectRef: GenerationObjectReference,
  kind: TemplateGenerationKind,
  connection: TemplateConnectionReference,
  fileNamePattern?: string,
  namespace?: string
): string {
  const kindSuffix = kind === 'model' ? 'Model' : 'Repository';
  const safePattern = fileNamePattern?.trim() || `{NamePascal}${kindSuffix}.cs`;
  return replaceIgnoreCase(
    replaceIgnoreCase(
      replaceIgnoreCase(
        replaceIgnoreCase(
          replaceIgnoreCase(
            replaceIgnoreCase(
              replaceIgnoreCase(safePattern, '{NamePascal}', toPascalCase(objectRef.name)),
              '{Name}',
              objectRef.name
            ),
            '{Type}',
            objectRef.objectType
          ),
          '{Schema}',
          objectRef.schema
        ),
        '{DatabaseType}',
        connection.databaseType
      ),
      '{DatabaseName}',
      connection.databaseName
    ),
    '{Namespace}',
    namespace ?? ''
  );
}

export function generateTestClassTemplate(
  className: string,
  objectRef: GenerationObjectReference,
  namespace?: string
): string {
  const methodName = sanitizeClassName(
    `Should_validate_${objectRef.schema}_${objectRef.name}_${objectRef.objectType}`);
  const skipReason = escapeCSharpString(`Implement generated scenario for ${objectRef.schema}.${objectRef.name}.`);
  const body = `// Auto-generated test class by DbSqlLikeMem VS Code extension\n` +
    `// Source: ${objectRef.objectType} ${objectRef.schema}.${objectRef.name}\n` +
    `using Xunit;\n\n` +
    `public class ${className}\n` +
    `{\n` +
    `    [Fact(Skip = "${skipReason}")]\n` +
    `    public void ${methodName}()\n` +
    `    {\n` +
    `        // Arrange\n` +
    `        // Act\n` +
    `        // Assert\n` +
    `    }\n` +
    `}\n`;

  const content = !namespace?.trim()
    ? body
    : `namespace ${namespace.trim()}\n{\n${indentMultiline(body, 1)}}\n`;

  return prependGeneratedArtifactMetadata(content, objectRef);
}

export function renderTemplateContent(
  template: string,
  className: string,
  objectRef: GenerationObjectReference,
  connection: TemplateConnectionReference,
  namespace?: string
): string {
  const rendered = replaceIgnoreCase(
    replaceIgnoreCase(
      replaceIgnoreCase(
        replaceIgnoreCase(
          replaceIgnoreCase(
            replaceIgnoreCase(
              replaceIgnoreCase(template, '{{ClassName}}', className),
              '{{ObjectName}}',
              objectRef.name
            ),
            '{{Schema}}',
            objectRef.schema
          ),
          '{{ObjectType}}',
          objectRef.objectType
        ),
        '{{DatabaseType}}',
        connection.databaseType
      ),
      '{{DatabaseName}}',
      connection.databaseName
    ),
    '{{Namespace}}',
    namespace ?? ''
  );

  return prependGeneratedArtifactMetadata(rendered, objectRef);
}

export function prependGeneratedArtifactMetadata(
  content: string,
  objectRef: GenerationObjectReference
): string {
  if (content.includes('// DBSqlLikeMem:Schema=')) {
    return content;
  }

  return buildGeneratedArtifactMetadataHeader(objectRef) + content;
}

export function parseGeneratedArtifactMetadata(content: string): GeneratedArtifactMetadata | undefined {
  const metadata = new Map<string, string>();
  for (const line of content.split(/\r?\n/)) {
    if (!line.startsWith('// DBSqlLikeMem:')) {
      continue;
    }

    const payload = line.slice('// DBSqlLikeMem:'.length);
    const separatorIndex = payload.indexOf('=');
    if (separatorIndex <= 0) {
      continue;
    }

    const key = payload.slice(0, separatorIndex).trim();
    const value = payload.slice(separatorIndex + 1).trim();
    if (key && value) {
      metadata.set(key, value);
    }
  }

  const schema = metadata.get('Schema');
  const objectName = metadata.get('Object');
  const objectType = metadata.get('Type');
  if (!schema || !objectName || !isSupportedDatabaseObjectType(objectType)) {
    return undefined;
  }

  return {
    schema,
    objectName,
    objectType,
    columns: metadata.get('Columns'),
    foreignKeys: metadata.get('ForeignKeys'),
    startValue: metadata.get('StartValue'),
    incrementBy: metadata.get('IncrementBy'),
    currentValue: metadata.get('CurrentValue')
  };
}

export function isGeneratedArtifactMetadataAligned(
  content: string,
  objectRef: GenerationObjectReference
): boolean {
  const metadata = parseGeneratedArtifactMetadata(content);
  return !!metadata
    && metadata.schema === objectRef.schema
    && metadata.objectName === objectRef.name
    && metadata.objectType === objectRef.objectType;
}

export function isGeneratedArtifactStructureAligned(
  content: string,
  objectRef: GenerationObjectReference
): boolean {
  const metadata = parseGeneratedArtifactMetadata(content);
  if (!metadata) {
    return false;
  }

  if (!isGeneratedArtifactMetadataAligned(content, objectRef)) {
    return false;
  }

  return (metadata.columns ?? '') === serializeColumns(objectRef.columns)
    && (metadata.foreignKeys ?? '') === serializeForeignKeys(objectRef.foreignKeys)
    && (metadata.startValue ?? '') === (objectRef.sequenceMetadata?.startValue ?? '')
    && (metadata.incrementBy ?? '') === (objectRef.sequenceMetadata?.incrementBy ?? '')
    && (metadata.currentValue ?? '') === (objectRef.sequenceMetadata?.currentValue ?? '');
}

export function indentMultiline(value: string, level: number): string {
  const indent = '    '.repeat(level);
  return value
    .split('\n')
    .map((line) => line ? `${indent}${line}` : line)
    .join('\n');
}

function toPascalCase(value: string): string {
  return value
    .split(/[^a-zA-Z0-9]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}

function replaceIgnoreCase(value: string, oldValue: string, newValue: string): string {
  let current = value;
  let index = current.toLowerCase().indexOf(oldValue.toLowerCase());

  while (index >= 0) {
    current = current.slice(0, index) + newValue + current.slice(index + oldValue.length);
    index = current.toLowerCase().indexOf(oldValue.toLowerCase(), index + newValue.length);
  }

  return current;
}

function escapeCSharpString(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

function buildGeneratedArtifactMetadataHeader(objectRef: GenerationObjectReference): string {
  return `// DBSqlLikeMem:Schema=${objectRef.schema}\n`
    + `// DBSqlLikeMem:Object=${objectRef.name}\n`
    + `// DBSqlLikeMem:Type=${objectRef.objectType}\n`
    + `// DBSqlLikeMem:Columns=${serializeColumns(objectRef.columns)}\n`
    + `// DBSqlLikeMem:ForeignKeys=${serializeForeignKeys(objectRef.foreignKeys)}\n`
    + `// DBSqlLikeMem:StartValue=${objectRef.sequenceMetadata?.startValue ?? ''}\n`
    + `// DBSqlLikeMem:IncrementBy=${objectRef.sequenceMetadata?.incrementBy ?? ''}\n`
    + `// DBSqlLikeMem:CurrentValue=${objectRef.sequenceMetadata?.currentValue ?? ''}\n`;
}

function isSupportedDatabaseObjectType(value: string | undefined): value is SupportedDatabaseObjectType {
  return value === 'Table' || value === 'View' || value === 'Procedure' || value === 'Sequence';
}

function serializeColumns(columns: ReadonlyArray<GenerationColumnReference> | undefined): string {
  if (!columns || columns.length === 0) {
    return '';
  }

  return [...columns]
    .sort((left, right) => left.ordinalPosition - right.ordinalPosition || left.name.localeCompare(right.name))
    .map((column) => `${column.name}|${column.dataType}|${column.ordinalPosition}|${column.isNullable ? '1' : '0'}`)
    .join(';');
}

function serializeForeignKeys(foreignKeys: ReadonlyArray<GenerationForeignKeyReference> | undefined): string {
  if (!foreignKeys || foreignKeys.length === 0) {
    return '';
  }

  return [...foreignKeys]
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((foreignKey) => `${foreignKey.name}|${foreignKey.referencedSchema}|${foreignKey.referencedTable}`)
    .join(';');
}

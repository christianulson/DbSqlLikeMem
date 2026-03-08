import * as path from 'node:path';

export type SupportedDatabaseObjectType = 'Table' | 'View' | 'Procedure' | 'Sequence';
export type TemplateGenerationKind = 'model' | 'repository';
export type GenerationCheckStatus = 'ok' | 'partial' | 'missing';
export type GeneratedArtifactKind = 'test' | 'model' | 'repository';

export interface GenerationObjectReference {
  schema: string;
  name: string;
  objectType: SupportedDatabaseObjectType;
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

  if (!namespace?.trim()) {
    return body;
  }

  return `namespace ${namespace.trim()}\n{\n${indentMultiline(body, 1)}}\n`;
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

import * as path from 'node:path';

export type SupportedDatabaseObjectType = 'Table' | 'View' | 'Procedure';
export type TemplateGenerationKind = 'model' | 'repository';

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
  targetFolder: string
): string {
  const suffix = kind === 'model' ? 'Model' : 'Repository';
  const className = sanitizeClassName(`${objectRef.name}${suffix}`);
  return path.join(workspaceFolder, targetFolder, `${className}.cs`);
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

function escapeCSharpString(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

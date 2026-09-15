import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { randomBytes } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import * as vscode from 'vscode';
import {
  buildTemplateClassFilePath,
  buildTestClassFilePath,
  buildTestClassName,
  evaluateGenerationConsistency,
  generateTestClassTemplate,
  SUPPORTED_DATABASE_OBJECT_TYPES,
  isGeneratedArtifactMetadataAligned,
  isGeneratedArtifactStructureAligned,
  renderTemplateContent,
  ensureUniqueGenerationTargets,
  sanitizeClassName
} from './generation-support';
import {
  buildTemplateBaselineProfileSummary,
  getMappingBaselineDefaults,
  getTemplateBaselineProfile,
  getTemplateBaselineProfiles,
  parseTemplateReviewMetadata,
  resolveTemplateSettingsDefaults
} from './template-baselines';
import { findUnsupportedTemplateTokens } from './template-token-catalog';
import { persistConnectionState, restoreConnectionSecrets, withoutConnectionSecrets } from './state-storage';
import { parseSqlServerConnectionString } from './connection-string';
import { createManagerViewState, readMappingNamespace, serializeWebviewData } from './manager-state';

type DatabaseObjectType = 'Table' | 'View' | 'Procedure' | 'Function' | 'Sequence';
type FilterMode = 'Equals' | 'Like';

interface ConnectionDefinition {
  id: string;
  name: string;
  databaseType: string;
  connectionString: string;
  databaseName: string;
}

interface ObjectTypeMapping {
  objectType: DatabaseObjectType;
  targetFolder: string;
  fileSuffix: string;
  namespace?: string;
}

interface ConnectionMappingConfiguration {
  connectionId: string;
  mappings: ObjectTypeMapping[];
}

interface DatabaseObjectReference {
  schema: string;
  name: string;
  objectType: DatabaseObjectType;
  columns?: DatabaseColumnReference[];
  foreignKeys?: ForeignKeyReference[];
  sequenceMetadata?: SequenceMetadataReference;
  requiredIn?: string;
  optionalIn?: string;
  outParams?: string;
  returnParam?: string;
  parameters?: string;
  returnTypeSql?: string;
  bodySql?: string;
}

interface DatabaseColumnReference {
  name: string;
  dataType: string;
  isNullable: boolean;
  ordinalPosition: number;
}

interface ForeignKeyReference {
  name: string;
  referencedSchema: string;
  referencedTable: string;
}

interface SequenceMetadataReference {
  startValue?: string;
  incrementBy?: string;
  currentValue?: string;
}

interface ExtensionState {
  connections: ConnectionDefinition[];
  mappingConfigurations: ConnectionMappingConfiguration[];
  templateSettings: TemplateSettings;
  generationCheckByObjectKey: Record<string, GenerationCheckStatus>;
  generationCheckDetailsByObjectKey: Record<string, string>;
  filterText: string;
  filterMode: FilterMode;
}

type GenerationKind = 'model' | 'repository';
type GenerationCheckStatus = 'ok' | 'partial' | 'missing' | 'drift';

interface TemplateSettings {
  modelTemplatePath: string;
  repositoryTemplatePath: string;
  modelTargetFolder: string;
  repositoryTargetFolder: string;
  modelFileNamePattern: string;
  repositoryFileNamePattern: string;
}

interface ConnectionInput {
  name: string;
  databaseType: string;
  databaseName: string;
  connectionString: string;
}

const SUPPORTED_DATABASE_TYPES = [
  'SqlServer',
  'SqlAzure',
  'AzureSql',
  'PostgreSql',
  'Oracle',
  'MySql',
  'MariaDb',
  'Sqlite',
  'Db2',
  'Firebird'
] as const;

const DEFAULT_DATABASE_TYPE = SUPPORTED_DATABASE_TYPES[0];
const SQL_SERVER_FAMILY_DATABASE_TYPES = new Set<string>(['sqlserver', 'sqlazure', 'azuresql']);
const DATABASE_TYPE_ALIAS_MAP = new Map<string, string>([
  ['sqlserver', 'SqlServer'],
  ['mssql', 'SqlServer'],
  ['sqlazure', 'SqlAzure'],
  ['azuresql', 'AzureSql'],
  ['postgresql', 'PostgreSql'],
  ['postgres', 'PostgreSql'],
  ['pg', 'PostgreSql'],
  ['pgsql', 'PostgreSql'],
  ['oracle', 'Oracle'],
  ['mysql', 'MySql'],
  ['mariadb', 'MariaDb'],
  ['sqlite', 'Sqlite'],
  ['sqlite3', 'Sqlite'],
  ['db2', 'Db2'],
  ['db2luw', 'Db2'],
  ['firebird', 'Firebird'],
  ['firebirdsql', 'Firebird']
]);
interface GenerationPlan {
  objectRef: DatabaseObjectReference;
  targetFile: string;
  content: string;
  namespace?: string;
}
interface TreeNode {
  id: string;
  label: string;
  kind: 'dbType' | 'database' | 'objectType' | 'object' | 'section' | 'column' | 'foreignKey';
  objectType?: DatabaseObjectType;
  children?: TreeNode[];
  connectionId?: string;
  objectRef?: DatabaseObjectReference;
  generationStatus?: GenerationCheckStatus;
  generationStatusDetail?: string;
}

const DEFAULT_STATE: ExtensionState = {
  connections: [],
  mappingConfigurations: [],
  templateSettings: {
    modelTemplatePath: '',
    repositoryTemplatePath: '',
    modelTargetFolder: 'src/Models',
    repositoryTargetFolder: 'src/Repositories',
    modelFileNamePattern: '{NamePascal}Model.cs',
    repositoryFileNamePattern: '{NamePascal}Repository.cs'
  },
  generationCheckByObjectKey: {},
  generationCheckDetailsByObjectKey: {},
  filterText: '',
  filterMode: 'Like'
};

function createDefaultMappings(folder: string, fileSuffix: string, namespace?: string): ObjectTypeMapping[] {
  return SUPPORTED_DATABASE_OBJECT_TYPES.map((objectType) => ({
    objectType,
    targetFolder: folder,
    fileSuffix,
    namespace: namespace || undefined
  }));
}

function isSupportedDatabaseObjectType(value: string): value is DatabaseObjectType {
  return SUPPORTED_DATABASE_OBJECT_TYPES.includes(value as DatabaseObjectType);
}

class DbNodeItem extends vscode.TreeItem {
  constructor(public readonly node: TreeNode) {
    super(node.label, node.children?.length ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.None);
    this.id = node.id;
    if (node.generationStatusDetail) {
      this.tooltip = node.generationStatusDetail;
    }
    if (node.kind === 'database' && node.generationStatus === 'missing') {
      this.iconPath = new vscode.ThemeIcon('error');
      this.description = vscode.l10n.t('Connection unavailable');
    }

    if (node.kind === 'object') {
      this.contextValue = 'db-object';
      this.description = node.objectType;
      const status = (node as TreeNode & { generationStatus?: GenerationCheckStatus }).generationStatus;
      if (status === 'ok') {
        this.iconPath = new vscode.ThemeIcon('pass-filled');
      } else if (status === 'drift') {
        this.iconPath = new vscode.ThemeIcon('diff');
      } else if (status === 'partial') {
        this.iconPath = new vscode.ThemeIcon('warning');
      } else if (status === 'missing') {
        this.iconPath = new vscode.ThemeIcon('error');
      }

      if (node.generationStatusDetail) {
        this.tooltip = node.generationStatusDetail;
      }
    } else if (node.kind === 'section') {
      this.iconPath = new vscode.ThemeIcon('list-unordered');
    } else if (node.kind === 'column') {
      this.iconPath = new vscode.ThemeIcon('symbol-field');
    } else if (node.kind === 'foreignKey') {
      this.iconPath = new vscode.ThemeIcon('link');
    } else {
      this.contextValue = `db-${node.kind}`;
    }
  }
}

class ConnectionTreeDataProvider implements vscode.TreeDataProvider<DbNodeItem> {
  private readonly eventEmitter = new vscode.EventEmitter<DbNodeItem | undefined>();
  readonly onDidChangeTreeData = this.eventEmitter.event;

  private tree: TreeNode[] = [];

  public setTree(tree: TreeNode[]): void {
    this.tree = tree;
    this.eventEmitter.fire(undefined);
  }

  getTreeItem(element: DbNodeItem): vscode.TreeItem {
    return element;
  }

  getChildren(element?: DbNodeItem): vscode.ProviderResult<DbNodeItem[]> {
    const nodes = element ? element.node.children ?? [] : this.tree;
    return nodes.map((node) => new DbNodeItem(node));
  }
}

interface DatabaseMetadataProvider {
  getObjects(connection: ConnectionDefinition): Promise<DatabaseObjectReference[]>;
  testConnection(connection: ConnectionDefinition): Promise<{ success: boolean; message?: string }>;
  clearConnectionWarnings(connectionId: string): void;
}

interface BridgeResponseEnvelope {
  success: boolean;
  message?: string;
  objects?: DatabaseObjectReference[];
}

class BridgeMetadataProvider implements DatabaseMetadataProvider {
  private readonly legacyProvider = new SqlMetadataProvider();
  private readonly warnedConnectionIds = new Set<string>();

  constructor(private readonly bridgeDllPath: string) {
  }

  public async getObjects(connection: ConnectionDefinition): Promise<DatabaseObjectReference[]> {
    const bridgeResult = await this.runBridgeCommand(connection, 'list-objects');
    if (bridgeResult?.success && Array.isArray(bridgeResult.objects)) {
      return bridgeResult.objects;
    }

    if (!bridgeResult && isSqlServerFamilyDatabaseType(connection.databaseType)) {
      return this.legacyProvider.getObjects(connection);
    }

    if (bridgeResult?.message) {
      this.warnConnectionFailure(connection, bridgeResult.message);
    } else {
      this.warnConnectionFailure(connection, vscode.l10n.t('Metadata bridge helper not found at {0}.', this.bridgeDllPath));
    }

    throw new Error(bridgeResult?.message ?? vscode.l10n.t('Metadata bridge helper not found at {0}.', this.bridgeDllPath));
  }

  public async testConnection(connection: ConnectionDefinition): Promise<{ success: boolean; message?: string }> {
    const bridgeResult = await this.runBridgeCommand(connection, 'test-connection');
    if (bridgeResult?.success) {
      return { success: true };
    }

    if (!bridgeResult && isSqlServerFamilyDatabaseType(connection.databaseType)) {
      return this.legacyProvider.testConnection(connection);
    }

    return {
      success: false,
      message: bridgeResult?.message ?? vscode.l10n.t('Metadata bridge helper not found at {0}.', this.bridgeDllPath)
    };
  }

  private async runBridgeCommand(connection: ConnectionDefinition, operation: 'list-objects' | 'test-connection'): Promise<BridgeResponseEnvelope | null> {
    if (!(await this.isBridgeAvailable())) {
      return null;
    }

    const args = [
      this.bridgeDllPath,
      '--operation',
      operation,
      '--database-type',
      connection.databaseType,
      '--database-name',
      connection.databaseName,
      '--connection-string-stdin',
      '--display-name',
      connection.name
    ];

    try {
      const stdout = await new Promise<string>((resolve, reject) => {
        const child = execFile('dotnet', args, { maxBuffer: 10 * 1024 * 1024, timeout: 60000 }, (error, output) => {
          if (error) {
            reject(Object.assign(error, { stdout: output }));
          } else {
            resolve(output);
          }
        });
        child.stdin?.on('error', reject);
        child.stdin?.end(connection.connectionString);
      });
      return this.parseBridgeResponse(stdout);
    } catch (error) {
      const stdout = error instanceof Error && 'stdout' in error ? String((error as { stdout?: string }).stdout ?? '') : '';
      const parsed = this.parseBridgeResponse(stdout);
      if (parsed) {
        return parsed;
      }

      const message = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        message
      };
    }
  }

  private parseBridgeResponse(stdout: string): BridgeResponseEnvelope | null {
    const trimmed = stdout.trim();
    if (!trimmed) {
      return null;
    }

    try {
      return JSON.parse(trimmed) as BridgeResponseEnvelope;
    } catch {
      return null;
    }
  }

  private async isBridgeAvailable(): Promise<boolean> {
    try {
      await fs.access(this.bridgeDllPath);
      return true;
    } catch {
      return false;
    }
  }

  private warnConnectionFailure(connection: ConnectionDefinition, message: string): void {
    if (this.warnedConnectionIds.has(connection.id)) {
      return;
    }

    this.warnedConnectionIds.add(connection.id);
    vscode.window.showWarningMessage(vscode.l10n.t('Failed to connect to {0}: {1}', connection.name, message));
  }

  public clearConnectionWarnings(connectionId: string): void {
    this.warnedConnectionIds.delete(connectionId);
  }
}

class SqlMetadataProvider implements DatabaseMetadataProvider {
  private warnedConnectionIds = new Set<string>();

  public async getObjects(connection: ConnectionDefinition): Promise<DatabaseObjectReference[]> {
    if (!isSqlServerFamilyDatabaseType(connection.databaseType)) {
      return [];
    }

    const parsed = parseSqlServerConnectionString(connection.connectionString);
    if (!parsed.server) {
      throw new Error(vscode.l10n.t('Connection string without server (Server/Data Source).'));
    }

    const query = "SET NOCOUNT ON; SELECT TABLE_SCHEMA AS [schema], TABLE_NAME AS [name], CASE TABLE_TYPE WHEN 'BASE TABLE' THEN 'Table' ELSE 'View' END AS [objectType] FROM INFORMATION_SCHEMA.TABLES UNION ALL SELECT ROUTINE_SCHEMA AS [schema], ROUTINE_NAME AS [name], CASE ROUTINE_TYPE WHEN 'FUNCTION' THEN 'Function' ELSE 'Procedure' END AS [objectType] FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE IN ('PROCEDURE', 'FUNCTION') UNION ALL SELECT SCHEMA_NAME(schema_id) AS [schema], name AS [name], 'Sequence' AS [objectType] FROM sys.sequences ORDER BY [schema], [name];";
    const columnQuery = "SET NOCOUNT ON; SELECT TABLE_SCHEMA AS [schema], TABLE_NAME AS [name], COLUMN_NAME AS [columnName], DATA_TYPE AS [dataType], IS_NULLABLE AS [isNullable], ORDINAL_POSITION AS [ordinalPosition] FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;";
    const foreignKeyQuery = "SET NOCOUNT ON; SELECT sch.name AS [schema], t.name AS [name], fk.name AS [fkName], refSch.name AS [referencedSchema], refTable.name AS [referencedTable] FROM sys.foreign_keys fk JOIN sys.tables t ON fk.parent_object_id = t.object_id JOIN sys.schemas sch ON t.schema_id = sch.schema_id JOIN sys.tables refTable ON fk.referenced_object_id = refTable.object_id JOIN sys.schemas refSch ON refTable.schema_id = refSch.schema_id ORDER BY sch.name, t.name, fk.name;";
    const sequenceQuery = "SET NOCOUNT ON; SELECT SCHEMA_NAME(schema_id) AS [schema], name AS [name], CAST(start_value AS nvarchar(50)) AS [startValue], CAST(increment AS nvarchar(50)) AS [incrementBy], CAST(current_value AS nvarchar(50)) AS [currentValue] FROM sys.sequences ORDER BY [schema], [name];";
    const routineMetadataQuery = "SET NOCOUNT ON; SELECT SCHEMA_NAME(o.schema_id) AS [schema], o.name AS [name], CASE WHEN o.type IN ('FN', 'IF', 'TF', 'FS', 'FT') THEN 'Function' ELSE 'Procedure' END AS [objectType], COALESCE(OBJECT_DEFINITION(o.object_id), '') AS [bodySql], COALESCE(CASE WHEN p0.parameter_id = 0 THEN t.name ELSE '' END, '') AS [returnTypeSql] FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id LEFT JOIN sys.parameters p0 ON p0.object_id = o.object_id AND p0.parameter_id = 0 LEFT JOIN sys.types t ON t.user_type_id = p0.user_type_id WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'FS', 'FT') ORDER BY SCHEMA_NAME(o.schema_id), o.name;";
    const routineParametersQuery = "SET NOCOUNT ON; SELECT SCHEMA_NAME(o.schema_id) AS [schema], o.name AS [name], CASE WHEN p.parameter_id = 0 THEN 'RETURN' WHEN p.is_output = 1 THEN 'OUT' ELSE 'IN' END AS [parameterMode], CASE WHEN p.parameter_id = 0 THEN 'return' ELSE REPLACE(REPLACE(p.name, '@', ''), ' ', '') END AS [parameterName], COALESCE(t.name, '') AS [dataType], p.parameter_id AS [ordinal], CASE WHEN p.has_default_value = 1 THEN COALESCE(CONVERT(nvarchar(4000), p.default_value), '') ELSE '' END AS [defaultValue], CASE WHEN p.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS [isNullable], p.max_length AS [charMaxLen], p.precision AS [numPrecision], p.scale AS [numScale], '0' AS [isVariadic], '0' AS [isOrderByClause], '0' AS [isFrameClause] FROM sys.parameters p JOIN sys.objects o ON o.object_id = p.object_id JOIN sys.schemas s ON s.schema_id = o.schema_id LEFT JOIN sys.types t ON t.user_type_id = p.user_type_id WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'FS', 'FT') ORDER BY SCHEMA_NAME(o.schema_id), o.name, p.parameter_id;";

    try {
      const { stdout } = await this.executeSqlCmd(parsed, query);
      const objects: DatabaseObjectReference[] = stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line && line.includes('|'))
        .map((line) => line.split('|').map((x) => x.trim()))
        .filter((parts) => parts.length >= 3)
        .filter((parts): parts is [string, string, DatabaseObjectType] => isSupportedDatabaseObjectType(parts[2]))
        .map((parts) => ({ schema: parts[0], name: parts[1], objectType: parts[2] }))
        .filter((x) => x.schema && x.name);

      const byObjectKey = new Map<string, DatabaseObjectReference>();
      for (const objectRef of objects) {
        byObjectKey.set(this.buildObjectKey(objectRef.schema, objectRef.name), objectRef);
      }

      const { stdout: columnsStdout } = await this.executeSqlCmd(parsed, columnQuery);
      for (const parts of this.parsePipeRows(columnsStdout, 6)) {
        const owner = byObjectKey.get(this.buildObjectKey(parts[0], parts[1]));
        if (!owner) {
          continue;
        }

        owner.columns ??= [];
        owner.columns.push({
          name: parts[2],
          dataType: parts[3],
          isNullable: parts[4].toUpperCase() === 'YES',
          ordinalPosition: Number.parseInt(parts[5], 10) || 0
        });
      }

      const { stdout: fkStdout } = await this.executeSqlCmd(parsed, foreignKeyQuery);
      for (const parts of this.parsePipeRows(fkStdout, 5)) {
        const owner = byObjectKey.get(this.buildObjectKey(parts[0], parts[1]));
        if (!owner || owner.objectType !== 'Table') {
          continue;
        }

        owner.foreignKeys ??= [];
        owner.foreignKeys.push({
          name: parts[2],
          referencedSchema: parts[3],
          referencedTable: parts[4]
        });
      }

      const { stdout: sequenceStdout } = await this.executeSqlCmd(parsed, sequenceQuery);
      for (const parts of this.parsePipeRows(sequenceStdout, 5)) {
        const owner = byObjectKey.get(this.buildObjectKey(parts[0], parts[1]));
        if (!owner || owner.objectType !== 'Sequence') {
          continue;
        }

        owner.sequenceMetadata = {
          startValue: parts[2] || undefined,
          incrementBy: parts[3] || undefined,
          currentValue: parts[4] || undefined
        };
      }

      const { stdout: routineMetadataStdout } = await this.executeSqlCmd(parsed, routineMetadataQuery);
      const routineMetadataByObjectKey = new Map<string, { bodySql?: string; returnTypeSql?: string }>();
      for (const parts of this.parsePipeRows(routineMetadataStdout, 5)) {
        const owner = byObjectKey.get(this.buildObjectKey(parts[0], parts[1]));
        if (!owner || (owner.objectType !== 'Procedure' && owner.objectType !== 'Function')) {
          continue;
        }

        routineMetadataByObjectKey.set(this.buildObjectKey(parts[0], parts[1]), {
          bodySql: parts[3] || undefined,
          returnTypeSql: parts[4] || undefined
        });
      }

      const { stdout: routineParametersStdout } = await this.executeSqlCmd(parsed, routineParametersQuery);
      const routineParametersByObjectKey = new Map<string, string[][]>();
      for (const parts of this.parsePipeRows(routineParametersStdout, 14)) {
        const owner = byObjectKey.get(this.buildObjectKey(parts[0], parts[1]));
        if (!owner || (owner.objectType !== 'Procedure' && owner.objectType !== 'Function')) {
          continue;
        }

        const key = this.buildObjectKey(parts[0], parts[1]);
        const bucket = routineParametersByObjectKey.get(key) ?? [];
        bucket.push(parts);
        routineParametersByObjectKey.set(key, bucket);
      }

      for (const objectRef of objects) {
        if (objectRef.objectType === 'Procedure') {
          const routineKey = this.buildObjectKey(objectRef.schema, objectRef.name);
          const params = routineParametersByObjectKey.get(routineKey) ?? [];
          const metadata = routineMetadataByObjectKey.get(routineKey);
          const serialized = this.serializeSqlServerProcedureRoutine(params, metadata?.returnTypeSql);
          objectRef.requiredIn = serialized.requiredIn;
          objectRef.optionalIn = serialized.optionalIn;
          objectRef.outParams = serialized.outParams;
          objectRef.returnParam = serialized.returnParam;
        } else if (objectRef.objectType === 'Function') {
          const routineKey = this.buildObjectKey(objectRef.schema, objectRef.name);
          const params = routineParametersByObjectKey.get(routineKey) ?? [];
          const metadata = routineMetadataByObjectKey.get(routineKey);
          const serialized = this.serializeSqlServerFunctionRoutine(params, metadata?.returnTypeSql, metadata?.bodySql);
          objectRef.parameters = serialized.parameters;
          objectRef.returnTypeSql = serialized.returnTypeSql;
          objectRef.bodySql = serialized.bodySql;
        }
      }

      for (const objectRef of objects) {
        objectRef.columns?.sort((a: DatabaseColumnReference, b: DatabaseColumnReference) => a.ordinalPosition - b.ordinalPosition);
      }

      return objects;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(vscode.l10n.t('Failed to connect to {0}: {1}', connection.name, message));
    }
  }

  private parsePipeRows(stdout: string, minParts: number): string[][] {
    return stdout
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line && line.includes('|'))
      .map((line) => line.split('|').map((x) => x.trim()))
      .filter((parts) => parts.length >= minParts);
  }

  private buildObjectKey(schema: string, name: string): string {
    return `${schema.toLowerCase()}|${name.toLowerCase()}`;
  }

  private serializeSqlServerProcedureRoutine(rows: string[][], returnTypeSql?: string): { requiredIn: string; optionalIn: string; outParams: string; returnParam: string } {
    const requiredIn: string[] = [];
    const optionalIn: string[] = [];
    const outParams: string[] = [];
    let returnParam = '';

    for (const parts of rows.sort((left, right) => Number(left[5] ?? 0) - Number(right[5] ?? 0))) {
      const mode = (parts[2] || 'IN').toUpperCase();
      const name = parts[3] || 'Parameter';
      const dbType = this.mapSqlServerDbType(parts[4] || '', parts[8], parts[9]);
      const value = parts[6] || '';
      const required = value.length === 0;
      const serialized = `${name}|${dbType}|${required ? '1' : '0'}|${value}`;

      if (mode === 'RETURN') {
        returnParam = serialized;
        continue;
      }

      if (mode.includes('OUT')) {
        outParams.push(serialized);
        continue;
      }

      if (required) {
        requiredIn.push(serialized);
      } else {
        optionalIn.push(serialized);
      }
    }

    if (!returnParam && returnTypeSql) {
      returnParam = `return|${this.mapSqlServerDbType(returnTypeSql)}|0|`;
    }

    return {
      requiredIn: requiredIn.join(';'),
      optionalIn: optionalIn.join(';'),
      outParams: outParams.join(';'),
      returnParam
    };
  }

  private serializeSqlServerFunctionRoutine(rows: string[][], returnTypeSql?: string, bodySql?: string): { parameters: string; returnTypeSql: string; bodySql: string } {
    const parameters = rows
      .filter((parts) => (parts[2] || 'IN').toUpperCase() !== 'RETURN')
      .sort((left, right) => Number(left[5] ?? 0) - Number(right[5] ?? 0))
      .map((parts) => {
        const name = parts[3] || 'Parameter';
        const typeSql = parts[4] || '';
        const required = (parts[6] || '').length === 0 ? '1' : '0';
        const isVariadic = '0';
        const isOrderByClause = '0';
        const isFrameClause = '0';
        const value = parts[6] || '';
        return `${name}|${typeSql}|${required}|${isVariadic}|${isOrderByClause}|${isFrameClause}|${value}`;
      });

    return {
      parameters: parameters.join(';'),
      returnTypeSql: returnTypeSql || '',
      bodySql: bodySql || ''
    };
  }

  private mapSqlServerDbType(dataType: string, charMaxLen?: string, numPrecision?: string): string {
    const type = dataType.trim().toLowerCase();
    const precision = Number.parseInt(numPrecision ?? '', 10);
    const length = Number.parseInt(charMaxLen ?? '', 10);

    switch (type) {
      case 'bigint':
        return 'Int64';
      case 'int':
        return 'Int32';
      case 'smallint':
        return 'Int16';
      case 'tinyint':
        return precision === 1 || length === 1 ? 'Boolean' : 'Byte';
      case 'bit':
        return 'Boolean';
      case 'decimal':
      case 'numeric':
      case 'money':
      case 'smallmoney':
        return 'Decimal';
      case 'float':
        return 'Double';
      case 'real':
        return 'Single';
      case 'date':
      case 'datetime':
      case 'datetime2':
      case 'smalldatetime':
      case 'datetimeoffset':
        return 'DateTime';
      case 'time':
        return 'Time';
      case 'uniqueidentifier':
        return 'Guid';
      case 'binary':
      case 'varbinary':
      case 'image':
        return 'Binary';
      case 'xml':
      case 'nchar':
      case 'nvarchar':
      case 'varchar':
      case 'char':
      case 'text':
      case 'ntext':
      case 'sysname':
        return 'String';
      default:
        return 'String';
    }
  }

  public async testConnection(connection: ConnectionDefinition): Promise<{ success: boolean; message?: string }> {
    if (!isSqlServerFamilyDatabaseType(connection.databaseType)) {
      return { success: false, message: vscode.l10n.t('This fallback supports SQL Server connections only.') };
    }

    const parsed = parseSqlServerConnectionString(connection.connectionString);
    if (!parsed.server) {
      return { success: false, message: vscode.l10n.t('Connection string without server (Server/Data Source).') };
    }

    try {
      await this.executeSqlCmd(parsed, 'SET NOCOUNT ON; SELECT 1;', 15000);
      return { success: true };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return { success: false, message };
    }
  }

  private async executeSqlCmd(
    parsed: { server?: string; database?: string; userId?: string; password?: string },
    query: string,
    timeout = 30000
  ): Promise<{ stdout: string; stderr: string }> {
    const args = ['-b', '-S', parsed.server ?? '', '-W', '-s', '|', '-Q', query, '-h', '-1'];

    if (parsed.database) {
      args.push('-d', parsed.database);
    }

    if (parsed.userId && parsed.password) {
      args.push('-U', parsed.userId);
    } else {
      args.push('-E');
    }

    const execFileAsync = promisify(execFile);
    return execFileAsync('sqlcmd', args, {
      maxBuffer: 10 * 1024 * 1024, timeout,
      env: { ...process.env, SQLCMDPASSWORD: parsed.password ?? '' }
    });
  }

  private warnConnectionFailure(connection: ConnectionDefinition, message: string): void {
    if (this.warnedConnectionIds.has(connection.id)) {
      return;
    }

    this.warnedConnectionIds.add(connection.id);
    vscode.window.showWarningMessage(vscode.l10n.t('Failed to connect to {0}: {1}', connection.name, message));
  }

  public clearConnectionWarnings(connectionId: string): void {
    this.warnedConnectionIds.delete(connectionId);
  }
}



async function validateAndNotifyConnection(metadataProvider: DatabaseMetadataProvider, connection: ConnectionDefinition, notifySuccess = true): Promise<boolean> {
  const result = await metadataProvider.testConnection(connection);
  if (result.success) {
    if (notifySuccess) {
      vscode.window.showInformationMessage(vscode.l10n.t('Connection {0} validated successfully.', connection.name));
    }
    return true;
  }

  const choice = await vscode.window.showWarningMessage(
    vscode.l10n.t('Failed to validate connection {0}: {1}', connection.name, result.message ?? vscode.l10n.t('Unknown error')),
    { modal: true },
    vscode.l10n.t('Save anyway'));
  return choice === vscode.l10n.t('Save anyway');
}
export async function activate(context: vscode.ExtensionContext): Promise<void> {
  const metadataProvider = new BridgeMetadataProvider(
    path.join(context.extensionPath, 'out', 'metadata-bridge', 'DbSqlLikeMem.VsCodeMetadataBridge.dll'));
  const treeProvider = new ConnectionTreeDataProvider();

  let state = await loadStateSafely(context);

  const refreshTree = async (): Promise<void> => {
    const tree = await buildTree(
      state.connections,
      state.filterText,
      state.filterMode,
      metadataProvider,
      state.generationCheckByObjectKey,
      state.generationCheckDetailsByObjectKey);
    treeProvider.setTree(tree);
  };

  context.subscriptions.push(vscode.window.registerTreeDataProvider('dbSqlLikeMem.connections', treeProvider));

  let managerPanel: vscode.WebviewPanel | undefined;
  let selectedMappingConnectionId = state.connections[0]?.id ?? '';
  const notifyManager = async (message: unknown): Promise<void> => {
    if (managerPanel) {
      try {
        await managerPanel.webview.postMessage(message);
      } catch {
        // The panel can close while a database operation is completing.
      }
    }
  };

  let commandRunning = false;
  const runSafely = async (action: () => Promise<unknown>): Promise<void> => {
    if (commandRunning) {
      await vscode.window.showInformationMessage(vscode.l10n.t('An operation is already in progress. Please wait.'));
      return;
    }
    commandRunning = true;
    await notifyManager({ type: 'busy', busy: true });
    try {
      await vscode.window.withProgress({ location: vscode.ProgressLocation.Window, title: 'DbSqlLikeMem' }, action);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await notifyManager({ type: 'feedback', kind: 'error', message });
      if (!managerPanel?.visible) {
        await vscode.window.showErrorMessage(message);
      }
    } finally {
      await notifyManager({ type: 'state', state: createManagerViewState(state) });
      await notifyManager({ type: 'busy', busy: false });
      commandRunning = false;
    }
  };
  const registerCommand = (name: string, action: (item?: DbNodeItem) => Promise<unknown>): vscode.Disposable =>
    vscode.commands.registerCommand(name, (item?: DbNodeItem) => runSafely(() => action(item)));

  context.subscriptions.push(
    vscode.commands.registerCommand('dbSqlLikeMem.openManager', async () => {
      if (managerPanel) {
        managerPanel.reveal();
        return;
      }
      const panel = vscode.window.createWebviewPanel(
        'dbSqlLikeMem.manager',
        vscode.l10n.t('DbSqlLikeMem Manager'),
        vscode.ViewColumn.Active,
        { enableScripts: true, retainContextWhenHidden: true, localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'resources')] }
      );
      context.subscriptions.push(panel);
      managerPanel = panel;
      panel.onDidDispose(() => { if (managerPanel === panel) { managerPanel = undefined; } });

      const render = (): void => {
        panel.webview.html = getManagerHtml(panel.webview, state, selectedMappingConnectionId, context.extensionUri);
      };

      panel.webview.onDidReceiveMessage(async (message: unknown) => {
        if (message && typeof message === 'object' && 'type' in message && message.type === 'ready') {
          await notifyManager({ type: 'state', state: createManagerViewState(state) });
          await notifyManager({ type: 'busy', busy: commandRunning });
          return;
        }
        await runSafely(async () => {
          if (!message || typeof message !== 'object' || !("type" in message)) {
            return;
          }

          const payload = message as Record<string, unknown>;

          if (payload.type === 'saveConnection') {
            const existingId = String(payload.connectionId ?? '').trim();
            const name = String(payload.name ?? '').trim();
            const databaseType = normalizeDatabaseType(String(payload.databaseType ?? '').trim());
            const databaseName = String(payload.databaseName ?? '').trim();
            const existingConnection = state.connections.find(connection => connection.id === existingId);
            if (existingId && !existingConnection) {
              throw new Error(vscode.l10n.t('Connection no longer exists. Reopen the manager.'));
            }
            const connectionString = String(payload.connectionString ?? '').trim() || existingConnection?.connectionString || '';

            if (!name || !databaseType || !databaseName || !connectionString) {
              throw new Error(vscode.l10n.t('Fill in all connection fields.'));
            }

            const draftConnection: ConnectionDefinition = {
              id: existingId || `${databaseType}-${databaseName}-${Date.now()}`,
              name,
              databaseType,
              databaseName,
              connectionString
            };

            if (!await validateAndNotifyConnection(metadataProvider, draftConnection, false)) {
              return;
            }

            if (existingId) {
              const index = state.connections.findIndex((x) => x.id === existingId);
              if (index >= 0) {
                state.connections[index] = {
                  id: existingId,
                  name,
                  databaseType,
                  databaseName,
                  connectionString
                };
              }
            } else {
              const connectionId = draftConnection.id;
              state.connections.push(draftConnection);

              if (!state.mappingConfigurations.some((x) => x.connectionId === connectionId)) {
                state.mappingConfigurations.push({
                  connectionId,
                  mappings: createDefaultMappings('src/Generated', 'Factory')
                });
              }
            }

            await saveState(context, state);
            selectedMappingConnectionId = existingId || draftConnection.id;
            metadataProvider.clearConnectionWarnings(draftConnection.id);
            await refreshTree();
            await notifyManager({ type: 'saved', requestId: payload.requestId, kind: 'connection', connectionId: draftConnection.id,
              message: existingId ? vscode.l10n.t('Connection {0} updated.', name) : vscode.l10n.t('Connection {0} saved.', name) });
            return;
          }

          if (payload.type === 'removeConnection') {
            const connectionId = String(payload.connectionId ?? '');
            const connection = state.connections.find(candidate => candidate.id === connectionId);
            if (!connection) {
              return;
            }
            const confirmed = await vscode.window.showWarningMessage(
              vscode.l10n.t('Remove connection {0}?', connection.name), { modal: true }, vscode.l10n.t('Remove'));
            if (confirmed !== vscode.l10n.t('Remove')) {
              return;
            }

            state.connections = state.connections.filter((x) => x.id !== connectionId);
            state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connectionId);
            await saveState(context, state);
            selectedMappingConnectionId = state.connections[0]?.id ?? '';
            await refreshTree();
            await notifyManager({ type: 'saved', requestId: payload.requestId, kind: 'remove', connectionId, message: vscode.l10n.t('Connection removed.') });
            return;
          }

          if (payload.type === 'saveMapping') {
            const connectionId = String(payload.connectionId ?? '').trim();
            const tableFolder = String(payload.tableFolder ?? '').trim();
            const tableSuffix = String(payload.tableSuffix ?? '').trim();
            const viewFolder = String(payload.viewFolder ?? '').trim();
            const viewSuffix = String(payload.viewSuffix ?? '').trim();
            const procedureFolder = String(payload.procedureFolder ?? '').trim();
            const procedureSuffix = String(payload.procedureSuffix ?? '').trim();
            const functionFolder = String(payload.functionFolder ?? '').trim();
            const functionSuffix = String(payload.functionSuffix ?? '').trim();
            const sequenceFolder = String(payload.sequenceFolder ?? '').trim();
            const sequenceSuffix = String(payload.sequenceSuffix ?? '').trim();

            if (!state.connections.some(connection => connection.id === connectionId) || !tableFolder || !tableSuffix || !viewFolder || !viewSuffix || !procedureFolder || !procedureSuffix || !functionFolder || !functionSuffix || !sequenceFolder || !sequenceSuffix) {
              throw new Error(vscode.l10n.t('Fill in connection, folders and mapping suffixes.'));
            }

            const mapping: ConnectionMappingConfiguration = {
              connectionId,
              mappings: [
                { objectType: 'Table', targetFolder: tableFolder, fileSuffix: tableSuffix, namespace: readMappingNamespace(payload, 'Table') },
                { objectType: 'View', targetFolder: viewFolder, fileSuffix: viewSuffix, namespace: readMappingNamespace(payload, 'View') },
                { objectType: 'Procedure', targetFolder: procedureFolder, fileSuffix: procedureSuffix, namespace: readMappingNamespace(payload, 'Procedure') },
                { objectType: 'Function', targetFolder: functionFolder, fileSuffix: functionSuffix, namespace: readMappingNamespace(payload, 'Function') },
                { objectType: 'Sequence', targetFolder: sequenceFolder, fileSuffix: sequenceSuffix, namespace: readMappingNamespace(payload, 'Sequence') }
              ]
            };

            state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connectionId);
            state.mappingConfigurations.push(mapping);
            await saveState(context, state);
            selectedMappingConnectionId = connectionId;
            await notifyManager({ type: 'saved', requestId: payload.requestId, kind: 'mapping', connectionId, message: vscode.l10n.t('Mappings saved.') });
          }
        });
      });

      render();
    }),
    registerCommand('dbSqlLikeMem.refresh', refreshTree),
    registerCommand('dbSqlLikeMem.removeConnection', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item);
      if (!connection) {
        return;
      }

      const confirmed = await vscode.window.showWarningMessage(
        vscode.l10n.t('Remove connection {0}?', connection.name),
        { modal: true },
        vscode.l10n.t('Remove')
      );

      if (confirmed !== vscode.l10n.t('Remove')) {
        return;
      }

      state.connections = state.connections.filter((x) => x.id !== connection.id);
      state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connection.id);
      await saveState(context, state);
      await refreshTree();
      vscode.window.showInformationMessage(vscode.l10n.t('Connection {0} removed.', connection.name));
    }),
    registerCommand('dbSqlLikeMem.editConnection', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item);
      if (!connection) {
        return;
      }

      const updatedConnection = await promptConnectionInput(connection);
      if (!updatedConnection) {
        return;
      }

      const updatedDraft: ConnectionDefinition = {
        id: connection.id,
        ...updatedConnection
      };

      if (!await validateAndNotifyConnection(metadataProvider, updatedDraft)) {
        return;
      }

      state.connections = state.connections.map((x) => x.id === connection.id
        ? updatedDraft
        : x);

      await saveState(context, state);
      metadataProvider.clearConnectionWarnings(connection.id);
      await refreshTree();
      vscode.window.showInformationMessage(vscode.l10n.t('Connection {0} updated.', updatedConnection.name));
    }),
    registerCommand('dbSqlLikeMem.addConnection', async () => {
      const newConnection = await promptConnectionInput();
      if (!newConnection) {
        return;
      }

      const normalizedDatabaseType = normalizeDatabaseType(newConnection.databaseType);
      const connectionId = `${normalizedDatabaseType}-${newConnection.databaseName}-${Date.now()}`;
      const draftConnection: ConnectionDefinition = {
        id: connectionId,
        ...newConnection,
        databaseType: normalizedDatabaseType
      };

      if (!await validateAndNotifyConnection(metadataProvider, draftConnection)) {
        return;
      }

      state.connections.push(draftConnection);

      if (!state.mappingConfigurations.some((x) => x.connectionId === connectionId)) {
        state.mappingConfigurations.push({
          connectionId,
          mappings: createDefaultMappings('src/Generated', 'Factory')
        });
      }

      await saveState(context, state);
      metadataProvider.clearConnectionWarnings(connectionId);
      await refreshTree();
    }),
    registerCommand('dbSqlLikeMem.setFilter', async () => {
      const text = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Filter (empty to clear)'),
        value: state.filterText
      });

      if (text === undefined) {
        return;
      }

      const mode = await vscode.window.showQuickPick(['Like', 'Equals'], {
        placeHolder: vscode.l10n.t('Filter mode')
      });

      if (!mode) {
        return;
      }

      state.filterText = text;
      state.filterMode = mode as FilterMode;
      await saveState(context, state);
      await refreshTree();
    }),
    registerCommand('dbSqlLikeMem.configureMappings', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item) ?? await pickConnection(state.connections);
      if (!connection) {
        return;
      }

      const reviewMetadata = await readTemplateReviewMetadataFromWorkspace();
      const current = state.mappingConfigurations.find((x) => x.connectionId === connection.id);
      const currentByType = new Map((current?.mappings ?? []).map((x) => [x.objectType, x]));
      const baselineSelection = await vscode.window.showQuickPick(
        [
          ...getTemplateBaselineProfiles().map((profile) => ({
            label: profile.label,
            description: vscode.l10n.t('Recommended test mapping defaults for {0}.', profile.label),
            detail: buildTemplateBaselineProfileSummary(profile, reviewMetadata),
            profileId: profile.id
          })),
          {
            label: vscode.l10n.t('Keep current/custom values'),
            description: vscode.l10n.t('Preserves the current mapping folders and suffixes as the prompt baseline.'),
            profileId: 'custom'
          }
        ],
        {
          placeHolder: vscode.l10n.t('Choose a baseline profile for the mapping prompts')
        }
      );
      if (!baselineSelection) {
        return;
      }

      const selectedMappingBaseline = baselineSelection.profileId === 'custom'
        ? undefined
        : getMappingBaselineDefaults(baselineSelection.profileId);

      const tableFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Folder for Table'),
        value: selectedMappingBaseline?.Table.targetFolder ?? currentByType.get('Table')?.targetFolder ?? 'src/Models/Tables'
      });
      if (!tableFolder) {
        return;
      }

      const tableSuffix = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Class suffix for Table'),
        value: selectedMappingBaseline?.Table.fileSuffix ?? currentByType.get('Table')?.fileSuffix ?? 'TableTests'
      });
      if (!tableSuffix) {
        return;
      }

      const viewFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Folder for View'),
        value: selectedMappingBaseline?.View.targetFolder ?? currentByType.get('View')?.targetFolder ?? 'src/Models/Views'
      });
      if (!viewFolder) {
        return;
      }

      const viewSuffix = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Class suffix for View'),
        value: selectedMappingBaseline?.View.fileSuffix ?? currentByType.get('View')?.fileSuffix ?? 'ViewTests'
      });
      if (!viewSuffix) {
        return;
      }

      const procedureFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Folder for Procedure'),
        value: selectedMappingBaseline?.Procedure.targetFolder ?? currentByType.get('Procedure')?.targetFolder ?? 'src/Models/Procedures'
      });
      if (!procedureFolder) {
        return;
      }

      const procedureSuffix = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Class suffix for Procedure'),
        value: selectedMappingBaseline?.Procedure.fileSuffix ?? currentByType.get('Procedure')?.fileSuffix ?? 'ProcedureTests'
      });
      if (!procedureSuffix) {
        return;
      }

      const functionFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Folder for Function'),
        value: selectedMappingBaseline?.Function.targetFolder ?? currentByType.get('Function')?.targetFolder ?? 'src/Models/Functions'
      });
      if (!functionFolder) {
        return;
      }

      const functionSuffix = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Class suffix for Function'),
        value: selectedMappingBaseline?.Function.fileSuffix ?? currentByType.get('Function')?.fileSuffix ?? 'FunctionTests'
      });
      if (!functionSuffix) {
        return;
      }

      const sequenceFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Folder for Sequence'),
        value: selectedMappingBaseline?.Sequence.targetFolder ?? currentByType.get('Sequence')?.targetFolder ?? 'src/Models/Sequences'
      });
      if (!sequenceFolder) {
        return;
      }

      const sequenceSuffix = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Class suffix for Sequence'),
        value: selectedMappingBaseline?.Sequence.fileSuffix ?? currentByType.get('Sequence')?.fileSuffix ?? 'SequenceTests'
      });
      if (!sequenceSuffix) {
        return;
      }

      const namespace = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Namespace (optional)'),
        value: currentByType.get('Table')?.namespace ?? currentByType.get('View')?.namespace ?? currentByType.get('Procedure')?.namespace ?? currentByType.get('Function')?.namespace ?? currentByType.get('Sequence')?.namespace ?? ''
      });
      if (namespace === undefined) {
        return;
      }

      const mapping: ConnectionMappingConfiguration = {
        connectionId: connection.id,
        mappings: [
          { objectType: 'Table', targetFolder: tableFolder, fileSuffix: tableSuffix, namespace: namespace || undefined },
          { objectType: 'View', targetFolder: viewFolder, fileSuffix: viewSuffix, namespace: namespace || undefined },
          { objectType: 'Procedure', targetFolder: procedureFolder, fileSuffix: procedureSuffix, namespace: namespace || undefined },
          { objectType: 'Function', targetFolder: functionFolder, fileSuffix: functionSuffix, namespace: namespace || undefined },
          { objectType: 'Sequence', targetFolder: sequenceFolder, fileSuffix: sequenceSuffix, namespace: namespace || undefined }
        ]
      };

      state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connection.id);
      state.mappingConfigurations.push(mapping);
      await saveState(context, state);
      vscode.window.showInformationMessage(vscode.l10n.t('Mappings saved for {0}.', connection.name));
    }),
    registerCommand('dbSqlLikeMem.configureTemplates', async () => {
      const defaultSettings = resolveTemplateSettingsDefaults(state.templateSettings);
      const reviewMetadata = await readTemplateReviewMetadataFromWorkspace();
      const baselineSelection = await vscode.window.showQuickPick(
        [
          ...getTemplateBaselineProfiles().map((profile) => ({
            label: profile.label,
            description: profile.description,
            detail: buildTemplateBaselineProfileSummary(profile, reviewMetadata),
            profileId: profile.id
          })),
          {
            label: vscode.l10n.t('Keep current/custom values'),
            description: vscode.l10n.t('Preserves the current template paths and folders as the prompt baseline.'),
            profileId: 'custom'
          }
        ],
        {
          placeHolder: vscode.l10n.t('Choose a baseline profile for the template prompts')
        }
      );
      if (!baselineSelection) {
        return;
      }

      const selectedBaseline = baselineSelection.profileId === 'custom'
        ? undefined
        : getTemplateBaselineProfile(baselineSelection.profileId);
      const promptDefaults = selectedBaseline ?? defaultSettings;

      const modelTemplatePath = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Model template (workspace-relative path)'),
        value: promptDefaults.modelTemplatePath
      });
      if (modelTemplatePath === undefined) {
        return;
      }

      const repositoryTemplatePath = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Repository template (workspace-relative path)'),
        value: promptDefaults.repositoryTemplatePath
      });
      if (repositoryTemplatePath === undefined) {
        return;
      }

      const modelTargetFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Target folder for model classes'),
        value: promptDefaults.modelTargetFolder
      });
      if (!modelTargetFolder) {
        return;
      }

      const repositoryTargetFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Target folder for repository classes'),
        value: promptDefaults.repositoryTargetFolder
      });
      if (!repositoryTargetFolder) {
        return;
      }

      const modelFileNamePattern = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('File name pattern for model classes'),
        value: promptDefaults.modelFileNamePattern
      });
      if (!modelFileNamePattern) {
        return;
      }

      const repositoryFileNamePattern = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('File name pattern for repository classes'),
        value: promptDefaults.repositoryFileNamePattern
      });
      if (!repositoryFileNamePattern) {
        return;
      }

      const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
      if (workspaceFolder) {
        for (const templatePath of [modelTemplatePath.trim(), repositoryTemplatePath.trim()]) {
          if (!templatePath) {
            continue;
          }

          const templateFullPath = path.join(workspaceFolder, templatePath);
          try {
            const templateText = await fs.readFile(templateFullPath, 'utf8');
            const unsupportedTokens = findUnsupportedTemplateTokens(templateText);
            if (unsupportedTokens.length > 0) {
              vscode.window.showWarningMessage(
                vscode.l10n.t('Template {0} uses unsupported tokens: {1}.', templatePath, unsupportedTokens.join(', '))
              );
              return;
            }
          } catch {
            // Keep current behavior: allow saving paths that do not exist yet.
          }
        }
      }

      state.templateSettings = {
        modelTemplatePath: modelTemplatePath.trim(),
        repositoryTemplatePath: repositoryTemplatePath.trim(),
        modelTargetFolder: modelTargetFolder.trim(),
        repositoryTargetFolder: repositoryTargetFolder.trim(),
        modelFileNamePattern: modelFileNamePattern.trim(),
        repositoryFileNamePattern: repositoryFileNamePattern.trim()
      };
      await saveState(context, state);
      vscode.window.showInformationMessage(vscode.l10n.t('Generation templates configured.'));
    }),
    registerCommand('dbSqlLikeMem.generateClasses', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item);
      if (!connection) {
        return;
      }

      const mapping = state.mappingConfigurations.find((x) => x.connectionId === connection.id);
      if (!mapping) {
        vscode.window.showWarningMessage(vscode.l10n.t('Configure mappings before generating test classes.'));
        return;
      }

      const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
      if (!workspaceFolder) {
        vscode.window.showWarningMessage(vscode.l10n.t('Open a folder in VS Code to generate files.'));
        return;
      }

      const scopedObjects = await getScopedObjects(connection, item, metadataProvider, state.filterText, state.filterMode);
      if (state.filterText.trim() && scopedObjects.length > 0) {
        vscode.window.showInformationMessage(
          vscode.l10n.t('A filter is active; only the matching {0} objects were generated.', scopedObjects.length));
      }
      const plans: GenerationPlan[] = [];

      for (const objectRef of scopedObjects) {
        const objectMapping = mapping.mappings.find((x) => x.objectType === objectRef.objectType);
        if (!objectMapping) {
          continue;
        }

        const className = buildTestClassName(objectRef, objectMapping);
        const targetFile = buildTestClassFilePath(workspaceFolder, objectRef, objectMapping);
        plans.push({
          objectRef,
          targetFile,
          namespace: objectMapping.namespace,
          content: generateTestClassTemplate(className, objectRef, objectMapping.namespace)
        });
      }

      if (!await confirmOverwrite(plans, vscode.l10n.t('test classes'))) {
        return;
      }

      for (const plan of plans) {
        await fs.mkdir(path.dirname(plan.targetFile), { recursive: true });
        await fs.writeFile(plan.targetFile, plan.content, 'utf8');
      }

      vscode.window.showInformationMessage(vscode.l10n.t('Test classes generated for {0}.', connection.name));
    }),
    registerCommand('dbSqlLikeMem.generateModelClasses', async (item?: DbNodeItem) => {
      await generateTemplateBasedFiles(state, metadataProvider, await resolveConnectionFromItem(state.connections, item), 'model', item);
    }),
    registerCommand('dbSqlLikeMem.generateRepositoryClasses', async (item?: DbNodeItem) => {
      await generateTemplateBasedFiles(state, metadataProvider, await resolveConnectionFromItem(state.connections, item), 'repository', item);
    }),
    registerCommand('dbSqlLikeMem.checkConsistency', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item);
      if (!connection) {
        return;
      }

      const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
      if (!workspaceFolder) {
        vscode.window.showWarningMessage(vscode.l10n.t('Open a folder in VS Code to check consistency.'));
        return;
      }

      const objects = await getScopedObjects(connection, item, metadataProvider, state.filterText, state.filterMode);
      if (objects.length === 0) {
        await vscode.window.showWarningMessage(vscode.l10n.t('No objects available to check. Refresh the connection or clear the filter.'));
        return;
      }
      if (state.filterText.trim()) {
        vscode.window.showInformationMessage(
          vscode.l10n.t('A filter is active; consistency was checked for the {0} matching objects.', objects.length));
      }
      const issues: string[] = [];
      let partialCount = 0;
      let missingCount = 0;
      let driftCount = 0;
      const checks = { ...state.generationCheckByObjectKey };
      const details = { ...state.generationCheckDetailsByObjectKey };
      const mapping = state.mappingConfigurations.find((x) => x.connectionId === connection.id);

      for (const objectRef of objects) {
        const testMapping = mapping?.mappings.find((x) => x.objectType === objectRef.objectType);
        const testPath = testMapping
          ? buildTestClassFilePath(workspaceFolder, objectRef, testMapping)
          : '';
        const modelPath = buildTemplateClassFilePath(
          workspaceFolder,
          objectRef,
          'model',
          state.templateSettings.modelTargetFolder,
          connection,
          state.templateSettings.modelFileNamePattern,
          testMapping?.namespace);
        const repositoryPath = buildTemplateClassFilePath(
          workspaceFolder,
          objectRef,
          'repository',
          state.templateSettings.repositoryTargetFolder,
          connection,
          state.templateSettings.repositoryFileNamePattern,
          testMapping?.namespace);

        const testFound = !!testPath && await fileExists(testPath);
        const modelFound = await fileExists(modelPath);
        const repositoryFound = await fileExists(repositoryPath);

        const key = buildObjectKey(connection.id, objectRef);
        const consistency = evaluateGenerationConsistency(testFound, modelFound, repositoryFound);
        delete details[key];

        const driftedArtifacts = await findMetadataDriftedArtifacts(objectRef, [
          { kind: 'test', exists: testFound, filePath: testPath },
          { kind: 'model', exists: modelFound, filePath: modelPath },
          { kind: 'repository', exists: repositoryFound, filePath: repositoryPath }
        ]);

        if (driftedArtifacts.length > 0) {
          checks[key] = 'drift';
          const detail = buildMetadataDriftDetail(driftedArtifacts, consistency.missingArtifacts);
          details[key] = detail;
          issues.push(`${objectRef.objectType}: ${objectRef.schema}.${objectRef.name} (${detail})`);
          driftCount += 1;
          continue;
        }

        checks[key] = consistency.status;
        if (consistency.status === 'partial') {
          partialCount += 1;
        } else if (consistency.status === 'missing') {
          missingCount += 1;
        }

        if (consistency.missingArtifacts.length > 0) {
          const detail = buildGenerationStatusDetail(consistency.missingArtifacts);
          details[key] = detail;
          issues.push(`${objectRef.objectType}: ${objectRef.schema}.${objectRef.name} (${detail})`);
        }
      }

      state.generationCheckByObjectKey = checks;
      state.generationCheckDetailsByObjectKey = details;
      await saveState(context, state);
      await refreshTree();

      if (issues.length === 0) {
        vscode.window.showInformationMessage(vscode.l10n.t('Consistency OK for {0} (green status).', connection.name));
        return;
      }

      const preview = issues.slice(0, 5).join(', ');
      vscode.window.showWarningMessage(
        vscode.l10n.t(
          'Consistency check found {0} incomplete, {1} missing, and {2} drifted object groups. Preview: {3}',
          partialCount,
          missingCount,
          driftCount,
          preview));
    }),
    registerCommand('dbSqlLikeMem.exportState', async () => {
      const saveUri = await vscode.window.showSaveDialog({ filters: { Json: ['json'] } });
      if (!saveUri) {
        return;
      }

      await vscode.workspace.fs.writeFile(saveUri, Buffer.from(JSON.stringify(withoutConnectionSecrets(state), null, 2), 'utf8'));
      vscode.window.showInformationMessage(vscode.l10n.t('State exported successfully.'));
    }),
    registerCommand('dbSqlLikeMem.importState', async () => {
      const openUri = await vscode.window.showOpenDialog({ filters: { Json: ['json'] }, canSelectMany: false });
      if (!openUri?.[0]) {
        return;
      }

      const bytes = await vscode.workspace.fs.readFile(openUri[0]);
      let importedState: ExtensionState;
      try {
        importedState = normalizeState(JSON.parse(Buffer.from(bytes).toString('utf8')) as Partial<ExtensionState>, true);
      } catch {
        vscode.window.showErrorMessage(vscode.l10n.t('Invalid settings file. Connections, mappings and templates must use the exported JSON format.'));
        return;
      }
      const confirmed = await vscode.window.showWarningMessage(
        vscode.l10n.t('Replace current settings? Imported connections without credentials must be edited before use.'),
        { modal: true }, vscode.l10n.t('Import'));
      if (confirmed !== vscode.l10n.t('Import')) {
        return;
      }
      await saveState(context, importedState);
      state = importedState;
      await refreshTree();
      vscode.window.showInformationMessage(vscode.l10n.t('State imported successfully.'));
    })
  );

  await runSafely(refreshTree);
}

export function deactivate(): void {
  // no-op
}

async function buildTree(
  connections: ConnectionDefinition[],
  filterText: string,
  filterMode: FilterMode,
  provider: DatabaseMetadataProvider,
  generationCheckByObjectKey: Record<string, GenerationCheckStatus>,
  generationCheckDetailsByObjectKey: Record<string, string>
): Promise<TreeNode[]> {
  const byType = new Map<string, TreeNode>();

  for (const connection of connections) {
    const typeNode = ensureNode(byType, connection.databaseType, {
      id: `type-${connection.databaseType}`,
      label: connection.databaseType,
      kind: 'dbType',
      children: []
    });

    const dbNode: TreeNode = {
      id: `db-${connection.id}`,
      label: connection.databaseName,
      kind: 'database',
      children: [],
      connectionId: connection.id
    };

    let objects: DatabaseObjectReference[];
    try {
      if (!connection.connectionString) {
        throw new Error(vscode.l10n.t('Edit this connection to enter its credentials.'));
      }
      objects = applyFilter(await provider.getObjects(connection), filterText, filterMode);
    } catch (error) {
      dbNode.generationStatusDetail = error instanceof Error ? error.message : String(error);
      dbNode.generationStatus = 'missing';
      typeNode.children?.push(dbNode);
      continue;
    }
    const objectGroups = Object.fromEntries(
      SUPPORTED_DATABASE_OBJECT_TYPES.map((objectType) => [objectType, objects.filter((x) => x.objectType === objectType)])
    ) as Record<DatabaseObjectType, DatabaseObjectReference[]>;

    SUPPORTED_DATABASE_OBJECT_TYPES.forEach((objectType) => {
      const objectTypeNode: TreeNode = {
        id: `objtype-${connection.id}-${objectType}`,
        label: objectType,
        kind: 'objectType',
        objectType,
        children: objectGroups[objectType].map((objectRef, index) => {
          const objectKey = buildObjectKey(connection.id, objectRef);
          const generationStatus = generationCheckByObjectKey[objectKey];
          const generationStatusDetail = generationCheckDetailsByObjectKey[objectKey];
          const children = createObjectChildren(connection.id, objectRef, index);
          return {
            id: `obj-${connection.id}-${objectType}-${index}-${objectRef.schema}.${objectRef.name}`,
            label: `${objectRef.schema}.${objectRef.name}`,
            kind: 'object',
            objectType,
            objectRef,
            connectionId: connection.id,
            generationStatus,
            generationStatusDetail,
            children
          };
        }),
        connectionId: connection.id
      };

      dbNode.children?.push(objectTypeNode);
    });

    typeNode.children?.push(dbNode);
  }

  return [...byType.values()];
}

function ensureNode(map: Map<string, TreeNode>, key: string, factory: TreeNode): TreeNode {
  const found = map.get(key);
  if (found) {
    return found;
  }

  map.set(key, factory);
  return factory;
}

function createObjectChildren(connectionId: string, objectRef: DatabaseObjectReference, index: number): TreeNode[] {
  const children: TreeNode[] = [];
  const objectKey = `${connectionId}-${objectRef.objectType}-${index}-${objectRef.schema}.${objectRef.name}`;

  if (objectRef.columns && objectRef.columns.length > 0) {
    children.push({
      id: `section-columns-${objectKey}`,
      label: vscode.l10n.t('Columns'),
      kind: 'section',
      connectionId,
      objectRef,
      children: objectRef.columns.map((column, index) => ({
        id: `column-${objectKey}-${index}-${column.name}`,
        label: `${column.name} (${column.dataType}${column.isNullable ? ', nullable' : ''})`,
        kind: 'column',
        connectionId,
        objectRef
      }))
    });
  }

  if (objectRef.foreignKeys && objectRef.foreignKeys.length > 0) {
    children.push({
      id: `section-fks-${objectKey}`,
      label: vscode.l10n.t('Foreign Keys'),
      kind: 'section',
      connectionId,
      objectRef,
      children: objectRef.foreignKeys.map((foreignKey, index) => ({
        id: `fk-${objectKey}-${index}-${foreignKey.name}`,
        label: `${foreignKey.name} → ${foreignKey.referencedSchema}.${foreignKey.referencedTable}`,
        kind: 'foreignKey',
        connectionId,
        objectRef
      }))
    });
  }

  if (objectRef.objectType === 'Procedure' || objectRef.objectType === 'Function') {
    children.push(...createRoutineChildren(connectionId, objectRef, index));
  }

  return children;
}

function createRoutineChildren(connectionId: string, objectRef: DatabaseObjectReference, index: number): TreeNode[] {
  const objectKey = `${connectionId}-${objectRef.objectType}-${index}-${objectRef.schema}.${objectRef.name}`;
  const children: TreeNode[] = [];

  if (objectRef.objectType === 'Procedure') {
    addRoutineSection(children, connectionId, objectRef, objectKey, vscode.l10n.t('Required In'), objectRef.requiredIn, 'routine-required');
    addRoutineSection(children, connectionId, objectRef, objectKey, vscode.l10n.t('Optional In'), objectRef.optionalIn, 'routine-optional');
    addRoutineSection(children, connectionId, objectRef, objectKey, vscode.l10n.t('Out Params'), objectRef.outParams, 'routine-out');
    addRoutineSection(children, connectionId, objectRef, objectKey, vscode.l10n.t('Return Param'), objectRef.returnParam, 'routine-return');
    return children;
  }

  addRoutineSection(children, connectionId, objectRef, objectKey, vscode.l10n.t('Parameters'), objectRef.parameters, 'routine-parameters');
  addRoutineScalar(children, connectionId, objectRef, objectKey, vscode.l10n.t('Return Type'), objectRef.returnTypeSql, 'routine-return-type');
  addRoutineScalar(children, connectionId, objectRef, objectKey, vscode.l10n.t('Body'), objectRef.bodySql, 'routine-body');
  return children;
}

function addRoutineSection(
  target: TreeNode[],
  connectionId: string,
  objectRef: DatabaseObjectReference,
  objectKey: string,
  sectionLabel: string,
  serializedItems: string | undefined,
  sectionKey: string
): void {
  if (!serializedItems?.trim()) {
    return;
  }

  const parsedItems = parseRoutineSerializedItems(serializedItems, objectRef.objectType);
  if (parsedItems.length === 0) {
    return;
  }

  target.push({
    id: `section-${sectionKey}-${objectKey}`,
    label: sectionLabel,
    kind: 'section',
    connectionId,
    objectRef,
    children: parsedItems.map((item, index) => ({
      id: `${sectionKey}-${objectKey}-${index}`,
      label: item,
      kind: 'column',
      connectionId,
      objectRef
    }))
  });
}

function addRoutineScalar(
  target: TreeNode[],
  connectionId: string,
  objectRef: DatabaseObjectReference,
  objectKey: string,
  sectionLabel: string,
  value: string | undefined,
  sectionKey: string
): void {
  const text = value?.trim();
  if (!text) {
    return;
  }

  target.push({
    id: `section-${sectionKey}-${objectKey}`,
    label: sectionLabel,
    kind: 'section',
    connectionId,
    objectRef,
    children: [
      {
        id: `${sectionKey}-${objectKey}-0`,
        label: text,
        kind: 'column',
        connectionId,
        objectRef
      }
    ]
  });
}

function parseRoutineSerializedItems(serialized: string, objectType: DatabaseObjectType): string[] {
  return serialized
    .split(';')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const parts = entry.split('|');
      if (objectType === 'Procedure') {
        const name = parts[0] || 'Parameter';
        const dbType = parts[1] || 'String';
        const required = parts[2] === '1' ? 'required' : 'optional';
        const value = parts[3];
        return value ? `${name} (${dbType}, ${required}) = ${value}` : `${name} (${dbType}, ${required})`;
      }

      const name = parts[0] || 'Parameter';
      const typeSql = parts[1] || 'String';
      const required = parts[2] === '1' ? 'required' : 'optional';
      const flags = [
        parts[3] === '1' ? 'variadic' : '',
        parts[4] === '1' ? 'order by' : '',
        parts[5] === '1' ? 'frame' : ''
      ].filter(Boolean);
      const value = parts[6];
      const suffix = [required, ...flags].join(', ');
      return value ? `${name} (${typeSql}, ${suffix}) = ${value}` : `${name} (${typeSql}, ${suffix})`;
    });
}

function applyFilter(
  objects: DatabaseObjectReference[],
  filterText: string,
  filterMode: FilterMode
): DatabaseObjectReference[] {
  if (!filterText.trim()) {
    return objects;
  }

  const needle = filterText.trim().toLowerCase();

  if (filterMode === 'Equals') {
    return objects.filter((x) => x.name.toLowerCase() === needle || `${x.schema}.${x.name}`.toLowerCase() === needle);
  }

  return objects.filter((x) => `${x.schema}.${x.name}`.toLowerCase().includes(needle));
}



async function getScopedObjects(
  connection: ConnectionDefinition,
  item: DbNodeItem | undefined,
  provider: DatabaseMetadataProvider,
  filterText: string,
  filterMode: FilterMode
): Promise<DatabaseObjectReference[]> {
  const filtered = applyFilter(await provider.getObjects(connection), filterText, filterMode);
  if (!item || !item.node) {
    return filtered;
  }

  if (item.node.kind === 'object' && item.node.objectRef) {
    return filtered.filter((x) =>
      x.objectType === item.node.objectRef?.objectType
      && x.schema === item.node.objectRef?.schema
      && x.name === item.node.objectRef?.name
    );
  }

  if ((item.node.kind === 'section' || item.node.kind === 'column' || item.node.kind === 'foreignKey') && item.node.objectRef) {
    return filtered.filter((x) =>
      x.objectType === item.node.objectRef?.objectType
      && x.schema === item.node.objectRef?.schema
      && x.name === item.node.objectRef?.name
    );
  }

  if (item.node.kind === 'objectType' && item.node.objectType) {
    return filtered.filter((x) => x.objectType === item.node.objectType);
  }

  return filtered;
}
async function loadState(context: vscode.ExtensionContext): Promise<ExtensionState> {
  const raw = context.globalState.get<Partial<ExtensionState>>('dbSqlLikeMem.state');
  return restoreConnectionSecrets(context, normalizeState(raw, false));
}

async function loadStateSafely(context: vscode.ExtensionContext): Promise<ExtensionState> {
  try {
    return await loadState(context);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    await vscode.window.showErrorMessage(
      vscode.l10n.t('Saved settings could not be loaded and were reset: {0}', detail),
      vscode.l10n.t('Reset settings'));
    const reset = structuredClone(DEFAULT_STATE);
    await saveState(context, reset).catch(() => undefined);
    return reset;
  }
}

function normalizeState(raw?: Partial<ExtensionState>, strict = true): ExtensionState {
  if (raw === undefined) {
    return structuredClone(DEFAULT_STATE);
  }

  const invalidState = (): never => { throw new Error(vscode.l10n.t('Invalid settings file. Connections, mappings and templates must use the exported JSON format.')); };
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)
    || !Array.isArray(raw.connections) || !Array.isArray(raw.mappingConfigurations)) {
    return invalidState();
  }
  const ids = new Set<string>();
  const validConnections: typeof raw.connections = [];
  for (const connection of raw.connections) {
    const isValid = !!connection
      && ['id', 'name', 'databaseType', 'databaseName'].every(key =>
        typeof (connection as unknown as Record<string, unknown>)[key] === 'string')
      && !!connection.id
      && !ids.has(connection.id)
      && (connection.connectionString === undefined || typeof connection.connectionString === 'string');
    if (!isValid) {
      if (strict) {
        return invalidState();
      }
      continue;
    }
    ids.add(connection.id);
    validConnections.push(connection);
  }
  const validMappings: ConnectionMappingConfiguration[] = [];
  for (const config of raw.mappingConfigurations) {
    const isValid = !!config
      && ids.has(config.connectionId)
      && Array.isArray(config.mappings)
      && config.mappings.every((mapping: unknown) => {
        const candidate = mapping as Partial<ObjectTypeMapping>;
        return !!mapping
          && typeof candidate.objectType === 'string'
          && isSupportedDatabaseObjectType(candidate.objectType)
          && typeof candidate.targetFolder === 'string'
          && typeof candidate.fileSuffix === 'string'
          && (candidate.namespace === undefined || typeof candidate.namespace === 'string');
      });
    if (!isValid) {
      if (strict) {
        return invalidState();
      }
      continue;
    }
    const types = new Set<string>();
    if ((config.mappings as ObjectTypeMapping[]).some((mapping: ObjectTypeMapping) => {
      if (types.has(mapping.objectType)) {
        return true;
      }
      types.add(mapping.objectType);
      return false;
    })) {
      if (strict) {
        return invalidState();
      }
      continue;
    }
    validMappings.push(config);
  }
  if (raw.templateSettings !== undefined && (!raw.templateSettings
    || typeof raw.templateSettings !== 'object' || Array.isArray(raw.templateSettings)
    || Object.values(raw.templateSettings).some(value => typeof value !== 'string'))) {
    if (strict) {
      return invalidState();
    }
    raw = { ...raw, templateSettings: undefined };
  }
  if ((raw.filterText !== undefined && typeof raw.filterText !== 'string')
    || (raw.filterMode !== undefined && raw.filterMode !== 'Like' && raw.filterMode !== 'Equals')) {
    if (strict) {
      return invalidState();
    }
    raw = { ...raw, filterText: undefined, filterMode: undefined };
  }
  if ((raw.generationCheckByObjectKey !== undefined
      && (typeof raw.generationCheckByObjectKey !== 'object' || Array.isArray(raw.generationCheckByObjectKey)))
    || (raw.generationCheckDetailsByObjectKey !== undefined
      && (typeof raw.generationCheckDetailsByObjectKey !== 'object' || Array.isArray(raw.generationCheckDetailsByObjectKey)))) {
    if (strict) {
      return invalidState();
    }
    raw = { ...raw, generationCheckByObjectKey: undefined, generationCheckDetailsByObjectKey: undefined };
  }
  const mappingConfigurations = validMappings.map(normalizeMappingConfiguration);
  const connections = validConnections.map((connection) => ({
    id: connection.id,
    name: connection.name,
    databaseName: connection.databaseName,
    connectionString: connection.connectionString ?? '',
    databaseType: normalizeDatabaseType(connection.databaseType)
  }));

  return {
    ...structuredClone(DEFAULT_STATE),
    connections,
    filterText: raw.filterText ?? '',
    filterMode: raw.filterMode ?? 'Like',
    templateSettings: {
      ...DEFAULT_STATE.templateSettings,
      ...(raw.templateSettings ?? {})
    },
    mappingConfigurations,
    generationCheckByObjectKey: raw.generationCheckByObjectKey ?? {},
    generationCheckDetailsByObjectKey: raw.generationCheckDetailsByObjectKey ?? {}
  };
}

function normalizeMappingConfiguration(configuration: ConnectionMappingConfiguration): ConnectionMappingConfiguration {
  if (SUPPORTED_DATABASE_OBJECT_TYPES.every(type => configuration.mappings.some(mapping => mapping.objectType === type))) {
    return configuration;
  }

  const fallback = configuration.mappings.find((mapping) => mapping.objectType === 'Procedure')
    ?? configuration.mappings[0];

  if (!fallback) {
    return {
      connectionId: configuration.connectionId,
      mappings: createDefaultMappings('Generated', 'Factory')
    };
  }

  return {
    connectionId: configuration.connectionId,
    mappings: [
      ...configuration.mappings,
      ...SUPPORTED_DATABASE_OBJECT_TYPES
        .filter(type => !configuration.mappings.some(mapping => mapping.objectType === type))
        .map(objectType => ({
          objectType,
          targetFolder: fallback.targetFolder,
          fileSuffix: fallback.fileSuffix,
          namespace: fallback.namespace
        }))
    ]
  };
}

function isSqlServerFamilyDatabaseType(databaseType: string): boolean {
  const normalized = normalizeDatabaseType(databaseType).trim().toLowerCase();
  return SQL_SERVER_FAMILY_DATABASE_TYPES.has(normalized);
}

function normalizeDatabaseType(databaseType: string): string {
  const trimmed = (databaseType ?? '').trim();
  if (!trimmed) {
    return '';
  }

  const exactMatch = SUPPORTED_DATABASE_TYPES.find((type) => type.toLowerCase() === trimmed.toLowerCase());
  if (exactMatch) {
    return exactMatch;
  }

  const normalizedKey = trimmed.replace(/[\s_\-/]/g, '').toLowerCase();
  return DATABASE_TYPE_ALIAS_MAP.get(normalizedKey) ?? trimmed;
}

async function saveState(context: vscode.ExtensionContext, state: ExtensionState): Promise<void> {
  await persistConnectionState(context, state);
}

async function readTemplateReviewMetadataFromWorkspace() {
  for (const workspaceFolder of vscode.workspace.workspaceFolders ?? []) {
    const metadataPath = path.join(workspaceFolder.uri.fsPath, 'templates', 'dbsqllikemem', 'review-metadata.json');
    try {
      const content = await fs.readFile(metadataPath, 'utf8');
      const metadata = parseTemplateReviewMetadata(content);
      if (metadata) {
        return metadata;
      }
    } catch {
      // Ignore missing or unreadable metadata files and keep the catalog defaults.
    }
  }

  return undefined;
}

function buildObjectKey(connectionId: string, objectRef: DatabaseObjectReference): string {
  return `${connectionId}|${objectRef.objectType}|${objectRef.schema}|${objectRef.name}`;
}

function buildGenerationStatusDetail(missingArtifacts: string[]): string {
  return vscode.l10n.t('Missing local artifacts: {0}', missingArtifacts.join(', '));
}

function buildMetadataDriftDetail(driftedArtifacts: string[], missingArtifacts: string[]): string {
  const driftMessage = vscode.l10n.t('Metadata drift in local artifacts: {0}', driftedArtifacts.join(', '));
  if (missingArtifacts.length === 0) {
    return driftMessage;
  }

  return vscode.l10n.t('{0}. Missing local artifacts: {1}', driftMessage, missingArtifacts.join(', '));
}

async function findMetadataDriftedArtifacts(
  objectRef: DatabaseObjectReference,
  artifacts: ReadonlyArray<{ kind: string; exists: boolean; filePath: string }>
): Promise<string[]> {
  const driftedArtifacts: string[] = [];

  for (const artifact of artifacts) {
    if (!artifact.exists || !artifact.filePath) {
      continue;
    }

    try {
      const content = await fs.readFile(artifact.filePath, 'utf8');
      const isAligned = hasStructuralGenerationMetadata(objectRef)
        ? isGeneratedArtifactStructureAligned(content, objectRef)
        : isGeneratedArtifactMetadataAligned(content, objectRef);
      if (!isAligned) {
        driftedArtifacts.push(artifact.kind);
      }
    } catch {
      driftedArtifacts.push(artifact.kind);
    }
  }

  return driftedArtifacts;
}

function hasStructuralGenerationMetadata(objectRef: DatabaseObjectReference): boolean {
  return !!objectRef.columns?.length
    || !!objectRef.foreignKeys?.length
    || !!objectRef.requiredIn?.trim()
    || !!objectRef.optionalIn?.trim()
    || !!objectRef.outParams?.trim()
    || !!objectRef.returnParam?.trim()
    || !!objectRef.parameters?.trim()
    || !!objectRef.returnTypeSql?.trim()
    || !!objectRef.bodySql?.trim();
}

async function generateTemplateBasedFiles(
  state: ExtensionState,
  metadataProvider: DatabaseMetadataProvider,
  connection: ConnectionDefinition | undefined,
  kind: GenerationKind,
  item?: DbNodeItem
): Promise<void> {
  if (!connection) {
    return;
  }

  const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  if (!workspaceFolder) {
    vscode.window.showWarningMessage(vscode.l10n.t('Open a folder in VS Code to generate files.'));
    return;
  }

  const templateRelativePath = kind === 'model' ? state.templateSettings.modelTemplatePath : state.templateSettings.repositoryTemplatePath;
  const targetFolder = kind === 'model' ? state.templateSettings.modelTargetFolder : state.templateSettings.repositoryTargetFolder;
  const objects = await getScopedObjects(connection, item, metadataProvider, state.filterText, state.filterMode);
  if (state.filterText.trim() && objects.length > 0) {
    vscode.window.showInformationMessage(
      vscode.l10n.t('A filter is active; only the matching {0} objects were generated.', objects.length));
  }
  const mapping = state.mappingConfigurations.find((x) => x.connectionId === connection.id);

  let templateText = kind === 'model'
    ? '// Model generated from {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n'
    : '// Repository generated from {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n';

  if (templateRelativePath.trim()) {
    const templateFullPath = path.resolve(workspaceFolder, templateRelativePath);
    try {
      const loadedTemplateText = await fs.readFile(templateFullPath, 'utf8');
      const unsupportedTokens = findUnsupportedTemplateTokens(loadedTemplateText);
      if (unsupportedTokens.length > 0) {
        throw new Error(vscode.l10n.t('Template {0} uses unsupported tokens: {1}.', templateRelativePath, unsupportedTokens.join(', ')));
      } else {
        templateText = loadedTemplateText;
      }
    } catch (error) {
      throw new Error(vscode.l10n.t('Could not load template {0}: {1}', templateRelativePath, error instanceof Error ? error.message : String(error)));
    }
  }

  const plans: GenerationPlan[] = [];
  for (const objectRef of objects) {
    const objectMapping = mapping?.mappings.find((x) => x.objectType === objectRef.objectType);
    const namespaceValue = objectMapping?.namespace ?? '';
    const targetFile = buildTemplateClassFilePath(
      workspaceFolder,
      objectRef,
      kind,
      targetFolder,
      connection,
      kind === 'model' ? state.templateSettings.modelFileNamePattern : state.templateSettings.repositoryFileNamePattern,
      namespaceValue
    );
    const className = sanitizeClassName(path.basename(targetFile, '.cs'));

    const content = renderTemplateContent(templateText, className, objectRef, connection, namespaceValue);

    plans.push({ objectRef, targetFile, content });
  }

  const label = kind === 'model' ? vscode.l10n.t('models') : vscode.l10n.t('repositories');
  if (!await confirmOverwrite(plans, label)) {
    return;
  }

  for (const plan of plans) {
    await fs.mkdir(path.dirname(plan.targetFile), { recursive: true });
    await fs.writeFile(plan.targetFile, plan.content, 'utf8');
  }

  vscode.window.showInformationMessage(vscode.l10n.t('{0} generated for {1}.', kind === 'model' ? vscode.l10n.t('Models') : vscode.l10n.t('Repositories'), connection.name));
}


async function confirmOverwrite(plans: GenerationPlan[], label: string): Promise<boolean> {
  if (plans.length === 0) {
    vscode.window.showWarningMessage(vscode.l10n.t('No eligible objects to generate {0}.', label));
    return false;
  }

  ensureUniqueGenerationTargets(plans.map(plan => plan.targetFile));

  const existing: string[] = [];
  for (const plan of plans) {
    if (await fileExists(plan.targetFile)) {
      existing.push(path.basename(plan.targetFile));
    }
  }

  if (existing.length === 0) {
    return true;
  }

  const preview = existing.slice(0, 5).join(', ');
  const choice = await vscode.window.showWarningMessage(
    vscode.l10n.t('{0} file(s) will be overwritten ({1}). Do you want to continue?', existing.length, preview),
    vscode.l10n.t('Generate and overwrite'),
    vscode.l10n.t('Cancel')
  );

  return choice === vscode.l10n.t('Generate and overwrite');
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function pickConnection(connections: ConnectionDefinition[]): Promise<ConnectionDefinition | undefined> {
  if (connections.length === 0) {
    vscode.window.showWarningMessage(vscode.l10n.t('No connection configured.'));
    return undefined;
  }

  const selected = await vscode.window.showQuickPick(
    connections.map((x) => ({
      label: x.name,
      description: `${x.databaseType} - ${x.databaseName}`,
      connection: x
    })),
    { placeHolder: vscode.l10n.t('Select a connection') }
  );

  return selected?.connection;
}

async function promptConnectionInput(connection?: ConnectionDefinition): Promise<ConnectionInput | undefined> {
  const name = await vscode.window.showInputBox({
    prompt: vscode.l10n.t('Connection name'),
    value: connection?.name
  });
  if (!name) {
    return undefined;
  }

  const databaseTypes = [...SUPPORTED_DATABASE_TYPES].sort((a, b) => Number(b === connection?.databaseType) - Number(a === connection?.databaseType));
  const databaseType = await vscode.window.showQuickPick(databaseTypes, {
    placeHolder: vscode.l10n.t('Database type'),
    title: connection ? vscode.l10n.t('Edit connection') : vscode.l10n.t('Add connection')
  });
  if (!databaseType) {
    return undefined;
  }

  const databaseName = await vscode.window.showInputBox({
    prompt: vscode.l10n.t('Primary database/schema name'),
    value: connection?.databaseName
  });
  if (!databaseName) {
    return undefined;
  }

  const connectionString = await vscode.window.showInputBox({
    prompt: vscode.l10n.t('Connection string (stored securely in VS Code SecretStorage)'),
    password: true,
    ignoreFocusOut: true,
    value: connection?.connectionString
  });
  if (!connectionString) {
    return undefined;
  }

  return {
    name,
    databaseType,
    databaseName,
    connectionString
  };
}

async function resolveConnectionFromItem(
  connections: ConnectionDefinition[],
  item?: DbNodeItem
): Promise<ConnectionDefinition | undefined> {
  const connectionId = item?.node.connectionId;
  if (connectionId) {
    return connections.find((x) => x.id === connectionId);
  }

  return pickConnection(connections);
}


function getManagerHtml(webview: vscode.Webview, state: ExtensionState, selectedConnectionId: string, extensionUri: vscode.Uri): string {
  const tr = {
    managerHelp: vscode.l10n.t("Manage connections and choose where generated files are saved."),
    connectionDetails: vscode.l10n.t("Connection details"),
    editHint: vscode.l10n.t("Editing a saved connection"),
    addHint: vscode.l10n.t("Add a database connection"),
    requiredHint: vscode.l10n.t("Name, database and connection string are required for new connections."),
    mappingHelp: vscode.l10n.t("Each object type keeps its own output folder, suffix and namespace."),
    targetFolder: vscode.l10n.t("Output folder"),
    fileSuffix: vscode.l10n.t("File suffix"),
    working: vscode.l10n.t("Working… Please wait."),
    ready: vscode.l10n.t("Ready."),
    unsaved: vscode.l10n.t("Unsaved changes"),
    requiredField: vscode.l10n.t("Fill in this field."),
    draftLifetime: vscode.l10n.t("Unsaved changes are kept while this Manager stays open."),
    resetConnection: vscode.l10n.t("Discard connection changes"),
    resetMapping: vscode.l10n.t("Discard mapping changes"),
    showSecret: vscode.l10n.t("Show connection string"),
    hideSecret: vscode.l10n.t("Hide connection string"),
    connectionCount: vscode.l10n.t("Connections"),
    emptyConnections: vscode.l10n.t("Add your first connection to configure generation."),
    missingConnection: vscode.l10n.t("The edited connection was removed. Copy any changes you need before starting a new connection."),
    tableLabel: vscode.l10n.t("Tables"),
    viewLabel: vscode.l10n.t("Views"),
    procedureLabel: vscode.l10n.t("Procedures"),
    functionLabel: vscode.l10n.t("Functions"),
    sequenceLabel: vscode.l10n.t("Sequences"),
    managerTitle: vscode.l10n.t("DbSqlLikeMem Manager"),
    name: vscode.l10n.t("Name"),
    databaseType: vscode.l10n.t("Database type"),
    primaryDatabase: vscode.l10n.t("Primary Database / Schema"),
    connectionString: vscode.l10n.t("Connection string"),
    keepCredentials: vscode.l10n.t("Leave blank when editing to keep the saved connection string."),
    saveConnection: vscode.l10n.t("Save connection"),
    configureMappings: vscode.l10n.t("Configure Mappings"),
    connection: vscode.l10n.t("Connection"),
    optionalNamespace: vscode.l10n.t("Namespace (optional)"),
    saveMappings: vscode.l10n.t("Save mappings"),
    addConnectionForMappings: vscode.l10n.t("Add a connection to configure mappings."),
    registeredConnections: vscode.l10n.t("Registered connections"),
    type: vscode.l10n.t("Type"),
    database: vscode.l10n.t("Database"),
    actions: vscode.l10n.t("Actions"),
    editConnection: vscode.l10n.t("Edit connection"),
    deleteConnection: vscode.l10n.t("Delete connection"),
    newConnection: vscode.l10n.t("New connection")
  };

  const data = serializeWebviewData({
    ...createManagerViewState(state), selectedConnectionId,
    databaseTypes: SUPPORTED_DATABASE_TYPES,
    objectTypes: SUPPORTED_DATABASE_OBJECT_TYPES,
    labels: tr
  });
  const nonce = generateNonce();
  const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'resources', 'manager.js'));
  const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'resources', 'manager.css'));
  const csp = "default-src 'none'; style-src " + webview.cspSource + "; script-src 'nonce-" + nonce + "';";
  const mappingFields = SUPPORTED_DATABASE_OBJECT_TYPES.map(type => {
    const id = type.toLowerCase();
    const label = tr[(id + 'Label') as keyof typeof tr];
    return '<fieldset class="mapping-card"><legend>' + escapeHtml(label) + '</legend><div class="mapping-fields">'
      + '<label for="' + id + 'Folder">' + escapeHtml(tr.targetFolder) + '<input id="' + id + 'Folder" required /></label>'
      + '<label for="' + id + 'Suffix">' + escapeHtml(tr.fileSuffix) + '<input id="' + id + 'Suffix" required /></label>'
      + '<label for="' + id + 'Namespace">' + escapeHtml(tr.optionalNamespace) + '<input id="' + id + 'Namespace" /></label>'
      + '</div></fieldset>';
  }).join('');
  return '<!DOCTYPE html><html lang="' + escapeHtml(vscode.env.language) + '"><head>'
    + '<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">'
    + '<meta http-equiv="Content-Security-Policy" content="' + csp + '">'
    + '<title>' + escapeHtml(tr.managerTitle) + '</title><link rel="stylesheet" href="' + styleUri + '"></head><body>'
    + '<main><header><h1>' + escapeHtml(tr.managerTitle) + '</h1><p class="hint">' + escapeHtml(tr.managerHelp) + '</p></header>'
    + '<p class="hint">' + escapeHtml(tr.draftLifetime) + '</p>'
    + '<div id="feedback" class="feedback" role="status" aria-live="polite">' + escapeHtml(tr.ready) + '</div>'
    + '<div class="workspace-grid"><section class="panel connection-panel" aria-labelledby="connectionHeading">'
    + '<h2 id="connectionHeading">' + escapeHtml(tr.connectionDetails) + '</h2><p id="editorMode" class="hint"></p>'
    + '<form id="connectionForm"><fieldset id="connectionFields"><legend class="sr-only">' + escapeHtml(tr.connectionDetails) + '</legend>'
    + '<input id="connectionIdHidden" type="hidden">'
    + '<label for="name">' + escapeHtml(tr.name) + '<input id="name" required autocomplete="off"></label>'
    + '<label for="databaseType">' + escapeHtml(tr.databaseType) + '<select id="databaseType"></select></label>'
    + '<label for="databaseName">' + escapeHtml(tr.primaryDatabase) + '<input id="databaseName" required></label>'
    + '<label for="connectionString">' + escapeHtml(tr.connectionString) + '</label><div class="secret-field">'
    + '<input id="connectionString" type="password" autocomplete="off" required aria-describedby="credentialsHint">'
    + '<button id="toggleSecret" class="secondary" type="button" aria-pressed="false">' + escapeHtml(tr.showSecret) + '</button></div>'
    + '<p id="credentialsHint" class="hint">' + escapeHtml(tr.requiredHint) + '</p>'
    + '<div class="button-row"><button id="saveConnection" type="submit">' + escapeHtml(tr.saveConnection) + '</button>'
    + '<button id="newConnection" class="secondary" type="button">' + escapeHtml(tr.newConnection) + '</button>'
    + '<button id="resetConnection" class="secondary" type="button">' + escapeHtml(tr.resetConnection) + '</button></div>'
    + '<p id="connectionStatus" class="hint" role="status"></p>'
    + '</fieldset></form></section>'
    + '<section class="panel mapping-panel" aria-labelledby="mappingHeading"><h2 id="mappingHeading">' + escapeHtml(tr.configureMappings) + '</h2>'
    + '<p class="hint">' + escapeHtml(tr.mappingHelp) + '</p>'
    + '<form id="mappingForm"><fieldset id="mappingFields"><legend class="sr-only">' + escapeHtml(tr.configureMappings) + '</legend>'
    + '<label for="connectionId">' + escapeHtml(tr.connection) + '<select id="connectionId"></select></label>'
    + '<p id="mappingStatus" class="hint" role="status"></p><div id="mappingCards">' + mappingFields + '</div>'
    + '<div class="button-row"><button id="saveMapping" type="submit">' + escapeHtml(tr.saveMappings) + '</button>'
    + '<button id="resetMapping" class="secondary" type="button">' + escapeHtml(tr.resetMapping) + '</button></div>'
    + '</fieldset></form><p id="emptyMapping" class="hint">' + escapeHtml(tr.addConnectionForMappings) + '</p></section></div>'
    + '<section class="panel" aria-labelledby="connectionsHeading"><div class="section-heading"><h2 id="connectionsHeading">' + escapeHtml(tr.registeredConnections) + '</h2><span id="connectionCount" class="badge"></span></div>'
    + '<p id="emptyConnections" class="hint">' + escapeHtml(tr.emptyConnections) + '</p>'
    + '<div class="table-scroll" tabindex="0" role="region" aria-labelledby="connectionsHeading"><table id="connectionsTable"><thead><tr>'
    + [tr.name, tr.type, tr.database, tr.actions].map(label => '<th scope="col">' + escapeHtml(label) + '</th>').join('')
    + '</tr></thead><tbody id="connectionRows"></tbody></table></div></section></main>'
    + '<script id="managerData" type="application/json" nonce="' + nonce + '">' + data + '</script>'
    + '<script nonce="' + nonce + '" src="' + scriptUri + '"></script></body></html>';
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function generateNonce(): string {
  return randomBytes(32).toString('base64url');
}


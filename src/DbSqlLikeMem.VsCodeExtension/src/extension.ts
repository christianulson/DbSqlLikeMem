import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import * as vscode from 'vscode';

type DatabaseObjectType = 'Table' | 'View' | 'Procedure';
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

interface ExtensionState {
  connections: ConnectionDefinition[];
  mappingConfigurations: ConnectionMappingConfiguration[];
  templateSettings: TemplateSettings;
  generationCheckByObjectKey: Record<string, GenerationCheckStatus>;
  filterText: string;
  filterMode: FilterMode;
}

type GenerationKind = 'model' | 'repository';
type GenerationCheckStatus = 'ok' | 'partial' | 'missing';

interface TemplateSettings {
  modelTemplatePath: string;
  repositoryTemplatePath: string;
  modelTargetFolder: string;
  repositoryTargetFolder: string;
}

interface ConnectionInput {
  name: string;
  databaseType: string;
  databaseName: string;
  connectionString: string;
}


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
}

const DEFAULT_STATE: ExtensionState = {
  connections: [],
  mappingConfigurations: [],
  templateSettings: {
    modelTemplatePath: '',
    repositoryTemplatePath: '',
    modelTargetFolder: 'src/Models',
    repositoryTargetFolder: 'src/Repositories'
  },
  generationCheckByObjectKey: {},
  filterText: '',
  filterMode: 'Like'
};

function createDefaultMappings(folder: string, fileSuffix: string, namespace?: string): ObjectTypeMapping[] {
  return ['Table', 'View', 'Procedure'].map((objectType) => ({
    objectType: objectType as DatabaseObjectType,
    targetFolder: folder,
    fileSuffix,
    namespace: namespace || undefined
  }));
}

class DbNodeItem extends vscode.TreeItem {
  constructor(public readonly node: TreeNode) {
    super(node.label, node.children?.length ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.None);
    this.id = node.id;

    if (node.kind === 'object') {
      this.contextValue = 'db-object';
      this.description = node.objectType;
      const status = (node as TreeNode & { generationStatus?: GenerationCheckStatus }).generationStatus;
      if (status === 'ok') {
        this.iconPath = new vscode.ThemeIcon('pass-filled');
      } else if (status === 'partial') {
        this.iconPath = new vscode.ThemeIcon('warning');
      } else if (status === 'missing') {
        this.iconPath = new vscode.ThemeIcon('error');
      }
    } else if (node.kind === 'section') {
      this.contextValue = 'db-section';
      this.iconPath = new vscode.ThemeIcon('list-unordered');
    } else if (node.kind === 'column') {
      this.contextValue = 'db-column';
      this.iconPath = new vscode.ThemeIcon('symbol-field');
    } else if (node.kind === 'foreignKey') {
      this.contextValue = 'db-foreignKey';
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
}

class SqlMetadataProvider implements DatabaseMetadataProvider {
  private warnedConnectionIds = new Set<string>();

  public async getObjects(connection: ConnectionDefinition): Promise<DatabaseObjectReference[]> {
    if (connection.databaseType.toLowerCase() !== 'sqlserver') {
      return [];
    }

    const parsed = parseSqlServerConnectionString(connection.connectionString);
    if (!parsed.server) {
      this.warnConnectionFailure(connection, vscode.l10n.t('Connection string without server (Server/Data Source).'));
      return [];
    }

    const query = "SET NOCOUNT ON; SELECT TABLE_SCHEMA AS [schema], TABLE_NAME AS [name], CASE TABLE_TYPE WHEN 'BASE TABLE' THEN 'Table' ELSE 'View' END AS [objectType] FROM INFORMATION_SCHEMA.TABLES UNION ALL SELECT ROUTINE_SCHEMA AS [schema], ROUTINE_NAME AS [name], 'Procedure' AS [objectType] FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY [schema], [name];";
    const columnQuery = "SET NOCOUNT ON; SELECT TABLE_SCHEMA AS [schema], TABLE_NAME AS [name], COLUMN_NAME AS [columnName], DATA_TYPE AS [dataType], IS_NULLABLE AS [isNullable], ORDINAL_POSITION AS [ordinalPosition] FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;";
    const foreignKeyQuery = "SET NOCOUNT ON; SELECT sch.name AS [schema], t.name AS [name], fk.name AS [fkName], refSch.name AS [referencedSchema], refTable.name AS [referencedTable] FROM sys.foreign_keys fk JOIN sys.tables t ON fk.parent_object_id = t.object_id JOIN sys.schemas sch ON t.schema_id = sch.schema_id JOIN sys.tables refTable ON fk.referenced_object_id = refTable.object_id JOIN sys.schemas refSch ON refTable.schema_id = refSch.schema_id ORDER BY sch.name, t.name, fk.name;";

    try {
      const { stdout } = await this.executeSqlCmd(parsed, query);
      const objects: DatabaseObjectReference[] = stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line && line.includes('|'))
        .map((line) => line.split('|').map((x) => x.trim()))
        .filter((parts) => parts.length >= 3)
        .map((parts) => ({ schema: parts[0], name: parts[1], objectType: parts[2] as DatabaseObjectType }))
        .filter((x) => (x.objectType === 'Table' || x.objectType === 'View' || x.objectType === 'Procedure') && x.schema && x.name);

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

      for (const objectRef of objects) {
        objectRef.columns?.sort((a: DatabaseColumnReference, b: DatabaseColumnReference) => a.ordinalPosition - b.ordinalPosition);
      }

      return objects;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.warnConnectionFailure(connection, message);
      return [];
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

  public async testConnection(connection: ConnectionDefinition): Promise<{ success: boolean; message?: string }> {
    if (connection.databaseType.toLowerCase() !== 'sqlserver') {
      return { success: true };
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
    const args = ['-S', parsed.server ?? '', '-W', '-s', '|', '-Q', query, '-h', '-1'];

    if (parsed.database) {
      args.push('-d', parsed.database);
    }

    if (parsed.userId && parsed.password) {
      args.push('-U', parsed.userId, '-P', parsed.password);
    } else {
      args.push('-E');
    }

    const execFileAsync = promisify(execFile);
    return execFileAsync('sqlcmd', args, { maxBuffer: 10 * 1024 * 1024, timeout });
  }

  private warnConnectionFailure(connection: ConnectionDefinition, message: string): void {
    if (this.warnedConnectionIds.has(connection.id)) {
      return;
    }

    this.warnedConnectionIds.add(connection.id);
    vscode.window.showWarningMessage(vscode.l10n.t('Failed to connect to {0}: {1}', connection.name, message));
  }
}



async function validateAndNotifyConnection(metadataProvider: DatabaseMetadataProvider, connection: ConnectionDefinition): Promise<boolean> {
  const result = await metadataProvider.testConnection(connection);
  if (result.success) {
    vscode.window.showInformationMessage(vscode.l10n.t('Connection {0} validated successfully.', connection.name));
    return true;
  }

  vscode.window.showErrorMessage(vscode.l10n.t('Failed to validate connection {0}: {1}', connection.name, result.message ?? vscode.l10n.t('Unknown error')));
  return false;
}
function parseSqlServerConnectionString(connectionString: string): { server?: string; database?: string; userId?: string; password?: string } {
  const map = new Map<string, string>();
  for (const pair of connectionString.split(';')) {
    const [rawKey, ...rawValue] = pair.split('=');
    if (!rawKey || rawValue.length === 0) {
      continue;
    }

    map.set(rawKey.trim().toLowerCase(), rawValue.join('=').trim());
  }

  return {
    server: map.get('server') ?? map.get('data source'),
    database: map.get('database') ?? map.get('initial catalog'),
    userId: map.get('user id') ?? map.get('uid'),
    password: map.get('password') ?? map.get('pwd')
  };
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  const metadataProvider = new SqlMetadataProvider();
  const treeProvider = new ConnectionTreeDataProvider();

  let state = await loadState(context);

  const refreshTree = async (): Promise<void> => {
    const tree = await buildTree(state.connections, state.filterText, state.filterMode, metadataProvider, state.generationCheckByObjectKey);
    treeProvider.setTree(tree);
  };

  vscode.window.registerTreeDataProvider('dbSqlLikeMem.connections', treeProvider);

  context.subscriptions.push(
    vscode.commands.registerCommand('dbSqlLikeMem.openManager', async () => {
      const panel = vscode.window.createWebviewPanel(
        'dbSqlLikeMem.manager',
        vscode.l10n.t('DbSqlLikeMem Manager'),
        vscode.ViewColumn.Active,
        { enableScripts: true }
      );

      const render = (): void => {
        panel.webview.html = getManagerHtml(state);
      };

      panel.webview.onDidReceiveMessage(async (message: unknown) => {
        if (!message || typeof message !== 'object' || !("type" in message)) {
          return;
        }

        const payload = message as Record<string, unknown>;

        if (payload.type === 'saveConnection') {
          const existingId = String(payload.connectionId ?? '').trim();
          const name = String(payload.name ?? '').trim();
          const databaseType = String(payload.databaseType ?? '').trim();
          const databaseName = String(payload.databaseName ?? '').trim();
          const connectionString = String(payload.connectionString ?? '').trim();

          if (!name || !databaseType || !databaseName || !connectionString) {
            vscode.window.showWarningMessage(vscode.l10n.t('Fill in all connection fields.'));
            return;
          }

          const draftConnection: ConnectionDefinition = {
            id: existingId || `${databaseType}-${databaseName}-${Date.now()}`,
            name,
            databaseType,
            databaseName,
            connectionString
          };

          if (!await validateAndNotifyConnection(metadataProvider, draftConnection)) {
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
          await refreshTree();
          render();
          vscode.window.showInformationMessage(existingId ? vscode.l10n.t('Connection {0} updated.', name) : vscode.l10n.t('Connection {0} saved.', name));
          return;
        }

        if (payload.type === 'removeConnection') {
          const connectionId = String(payload.connectionId ?? '');
          if (!connectionId) {
            return;
          }

          state.connections = state.connections.filter((x) => x.id !== connectionId);
          state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connectionId);
          await saveState(context, state);
          await refreshTree();
          render();
          vscode.window.showInformationMessage(vscode.l10n.t('Connection removed.'));
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
          const namespace = String(payload.namespace ?? '').trim();

          if (!connectionId || !tableFolder || !tableSuffix || !viewFolder || !viewSuffix || !procedureFolder || !procedureSuffix) {
            vscode.window.showWarningMessage(vscode.l10n.t('Fill in connection, folders and mapping suffixes.'));
            return;
          }

          const mapping: ConnectionMappingConfiguration = {
            connectionId,
            mappings: [
              { objectType: 'Table', targetFolder: tableFolder, fileSuffix: tableSuffix, namespace: namespace || undefined },
              { objectType: 'View', targetFolder: viewFolder, fileSuffix: viewSuffix, namespace: namespace || undefined },
              { objectType: 'Procedure', targetFolder: procedureFolder, fileSuffix: procedureSuffix, namespace: namespace || undefined }
            ]
          };

          state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connectionId);
          state.mappingConfigurations.push(mapping);
          await saveState(context, state);
          render();
          vscode.window.showInformationMessage(vscode.l10n.t('Mappings saved.'));
        }
      });

      render();
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.refresh', refreshTree),
    vscode.commands.registerCommand('dbSqlLikeMem.removeConnection', async (item?: DbNodeItem) => {
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
    vscode.commands.registerCommand('dbSqlLikeMem.editConnection', async (item?: DbNodeItem) => {
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
      await refreshTree();
      vscode.window.showInformationMessage(vscode.l10n.t('Connection {0} updated.', updatedConnection.name));
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.addConnection', async () => {
      const newConnection = await promptConnectionInput();
      if (!newConnection) {
        return;
      }

      const connectionId = `${newConnection.databaseType}-${newConnection.databaseName}-${Date.now()}`;
      const draftConnection: ConnectionDefinition = {
        id: connectionId,
        ...newConnection
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
      await refreshTree();
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.setFilter', async () => {
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
    vscode.commands.registerCommand('dbSqlLikeMem.configureMappings', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item) ?? await pickConnection(state.connections);
      if (!connection) {
        return;
      }

      const current = state.mappingConfigurations.find((x) => x.connectionId === connection.id);
      const currentByType = new Map((current?.mappings ?? []).map((x) => [x.objectType, x]));

      const tableFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Folder for Table'),
        value: currentByType.get('Table')?.targetFolder ?? 'src/Models/Tables'
      });
      if (!tableFolder) {
        return;
      }

      const tableSuffix = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Class suffix for Table'),
        value: currentByType.get('Table')?.fileSuffix ?? 'TableTests'
      });
      if (!tableSuffix) {
        return;
      }

      const viewFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Folder for View'),
        value: currentByType.get('View')?.targetFolder ?? 'src/Models/Views'
      });
      if (!viewFolder) {
        return;
      }

      const viewSuffix = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Class suffix for View'),
        value: currentByType.get('View')?.fileSuffix ?? 'ViewTests'
      });
      if (!viewSuffix) {
        return;
      }

      const procedureFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Folder for Procedure'),
        value: currentByType.get('Procedure')?.targetFolder ?? 'src/Models/Procedures'
      });
      if (!procedureFolder) {
        return;
      }

      const procedureSuffix = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Class suffix for Procedure'),
        value: currentByType.get('Procedure')?.fileSuffix ?? 'ProcedureTests'
      });
      if (!procedureSuffix) {
        return;
      }

      const mapping: ConnectionMappingConfiguration = {
        connectionId: connection.id,
        mappings: [
          { objectType: 'Table', targetFolder: tableFolder, fileSuffix: tableSuffix },
          { objectType: 'View', targetFolder: viewFolder, fileSuffix: viewSuffix },
          { objectType: 'Procedure', targetFolder: procedureFolder, fileSuffix: procedureSuffix }
        ]
      };

      state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connection.id);
      state.mappingConfigurations.push(mapping);
      await saveState(context, state);
      vscode.window.showInformationMessage(vscode.l10n.t('Mappings saved for {0}.', connection.name));
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.configureTemplates', async () => {
      const modelTemplatePath = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Model template (workspace-relative path)'),
        value: state.templateSettings.modelTemplatePath || 'templates/model.template.txt'
      });
      if (modelTemplatePath === undefined) {
        return;
      }

      const repositoryTemplatePath = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Repository template (workspace-relative path)'),
        value: state.templateSettings.repositoryTemplatePath || 'templates/repository.template.txt'
      });
      if (repositoryTemplatePath === undefined) {
        return;
      }

      const modelTargetFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Target folder for model classes'),
        value: state.templateSettings.modelTargetFolder
      });
      if (!modelTargetFolder) {
        return;
      }

      const repositoryTargetFolder = await vscode.window.showInputBox({
        prompt: vscode.l10n.t('Target folder for repository classes'),
        value: state.templateSettings.repositoryTargetFolder
      });
      if (!repositoryTargetFolder) {
        return;
      }

      state.templateSettings = {
        modelTemplatePath: modelTemplatePath.trim(),
        repositoryTemplatePath: repositoryTemplatePath.trim(),
        modelTargetFolder: modelTargetFolder.trim(),
        repositoryTargetFolder: repositoryTargetFolder.trim()
      };
      await saveState(context, state);
      vscode.window.showInformationMessage(vscode.l10n.t('Generation templates configured.'));
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.generateClasses', async (item?: DbNodeItem) => {
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
      const plans: GenerationPlan[] = [];

      for (const objectRef of scopedObjects) {
        const objectMapping = mapping.mappings.find((x) => x.objectType === objectRef.objectType);
        if (!objectMapping) {
          continue;
        }

        const className = sanitizeClassName(objectRef.name + objectMapping.fileSuffix);
        const targetDir = path.join(workspaceFolder, objectMapping.targetFolder);
        const targetFile = path.join(targetDir, `${className}.cs`);
        plans.push({
          objectRef,
          targetFile,
          namespace: objectMapping.namespace,
          content: generateClassTemplate(className, objectRef, objectMapping.namespace)
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
    vscode.commands.registerCommand('dbSqlLikeMem.generateModelClasses', async (item?: DbNodeItem) => {
      await generateTemplateBasedFiles(state, metadataProvider, await resolveConnectionFromItem(state.connections, item), 'model', item);
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.generateRepositoryClasses', async (item?: DbNodeItem) => {
      await generateTemplateBasedFiles(state, metadataProvider, await resolveConnectionFromItem(state.connections, item), 'repository', item);
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.checkConsistency', async (item?: DbNodeItem) => {
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
      const missing: string[] = [];
      const checks: Record<string, GenerationCheckStatus> = {};

      for (const objectRef of objects) {
        const modelExpected = sanitizeClassName(`${objectRef.name}Model.cs`);
        const repositoryExpected = sanitizeClassName(`${objectRef.name}Repository.cs`);
        const modelFound = await findFileCaseInsensitive(workspaceFolder, modelExpected);
        const repositoryFound = await findFileCaseInsensitive(workspaceFolder, repositoryExpected);

        const key = buildObjectKey(connection.id, objectRef);
        checks[key] = modelFound && repositoryFound ? 'ok' : (modelFound || repositoryFound ? 'partial' : 'missing');

        if (!modelFound || !repositoryFound) {
          missing.push(`${objectRef.objectType}: ${objectRef.schema}.${objectRef.name}`);
        }
      }

      state.generationCheckByObjectKey = {
        ...state.generationCheckByObjectKey,
        ...checks
      };
      await saveState(context, state);
      await refreshTree();

      if (missing.length === 0) {
        vscode.window.showInformationMessage(vscode.l10n.t('Consistency OK for {0} (green status).', connection.name));
        return;
      }

      const preview = missing.slice(0, 5).join(', ');
      vscode.window.showWarningMessage(vscode.l10n.t('Objects without local class ({0}) - yellow/red status: {1}', missing.length, preview));
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.exportState', async () => {
      const saveUri = await vscode.window.showSaveDialog({ filters: { Json: ['json'] } });
      if (!saveUri) {
        return;
      }

      await vscode.workspace.fs.writeFile(saveUri, Buffer.from(JSON.stringify(state, null, 2), 'utf8'));
      vscode.window.showInformationMessage(vscode.l10n.t('State exported successfully.'));
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.importState', async () => {
      const openUri = await vscode.window.showOpenDialog({ filters: { Json: ['json'] }, canSelectMany: false });
      if (!openUri?.[0]) {
        return;
      }

      const bytes = await vscode.workspace.fs.readFile(openUri[0]);
      state = JSON.parse(Buffer.from(bytes).toString('utf8')) as ExtensionState;
      await saveState(context, state);
      await refreshTree();
      vscode.window.showInformationMessage(vscode.l10n.t('State imported successfully.'));
    })
  );

  await refreshTree();
}

export function deactivate(): void {
  // no-op
}

async function buildTree(
  connections: ConnectionDefinition[],
  filterText: string,
  filterMode: FilterMode,
  provider: DatabaseMetadataProvider,
  generationCheckByObjectKey: Record<string, GenerationCheckStatus>
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

    const objects = applyFilter(await provider.getObjects(connection), filterText, filterMode);
    const objectGroups: Record<DatabaseObjectType, DatabaseObjectReference[]> = {
      Table: objects.filter((x) => x.objectType === 'Table'),
      View: objects.filter((x) => x.objectType === 'View'),
      Procedure: objects.filter((x) => x.objectType === 'Procedure')
    };

    (Object.keys(objectGroups) as DatabaseObjectType[]).forEach((objectType) => {
      const objectTypeNode: TreeNode = {
        id: `objtype-${connection.id}-${objectType}`,
        label: objectType,
        kind: 'objectType',
        objectType,
        children: objectGroups[objectType].map((objectRef) => {
          const generationStatus = generationCheckByObjectKey[buildObjectKey(connection.id, objectRef)];
          const children = createObjectChildren(connection.id, objectRef);
          return {
            id: `obj-${connection.id}-${objectType}-${objectRef.schema}.${objectRef.name}`,
            label: `${objectRef.schema}.${objectRef.name}`,
            kind: 'object',
            objectType,
            objectRef,
            connectionId: connection.id,
            generationStatus,
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

function createObjectChildren(connectionId: string, objectRef: DatabaseObjectReference): TreeNode[] {
  const children: TreeNode[] = [];
  const objectKey = `${connectionId}-${objectRef.objectType}-${objectRef.schema}.${objectRef.name}`;

  if (objectRef.columns && objectRef.columns.length > 0) {
    children.push({
      id: `section-columns-${objectKey}`,
      label: 'Columns',
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
      label: 'Foreign Keys',
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

  return children;
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
  if (!raw) {
    return structuredClone(DEFAULT_STATE);
  }

  return {
    ...structuredClone(DEFAULT_STATE),
    ...raw,
    templateSettings: {
      ...DEFAULT_STATE.templateSettings,
      ...(raw.templateSettings ?? {})
    },
    generationCheckByObjectKey: raw.generationCheckByObjectKey ?? {}
  };
}

async function saveState(context: vscode.ExtensionContext, state: ExtensionState): Promise<void> {
  await context.globalState.update('dbSqlLikeMem.state', state);
}



function buildObjectKey(connectionId: string, objectRef: DatabaseObjectReference): string {
  return `${connectionId}|${objectRef.objectType}|${objectRef.schema}|${objectRef.name}`;
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
  const suffix = kind === 'model' ? 'Model' : 'Repository';
  const objects = await getScopedObjects(connection, item, metadataProvider, state.filterText, state.filterMode);
  const mapping = state.mappingConfigurations.find((x) => x.connectionId === connection.id);

  let templateText = kind === 'model'
    ? '// Model generated from {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n'
    : '// Repository generated from {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n';

  if (templateRelativePath.trim()) {
    const templateFullPath = path.join(workspaceFolder, templateRelativePath);
    try {
      templateText = await fs.readFile(templateFullPath, 'utf8');
    } catch {
      vscode.window.showWarningMessage(vscode.l10n.t('Template not found: {0}. Using default template.', templateRelativePath));
    }
  }

  const plans: GenerationPlan[] = [];
  for (const objectRef of objects) {
    const objectMapping = mapping?.mappings.find((x) => x.objectType === objectRef.objectType);
    const namespaceValue = objectMapping?.namespace ?? '';
    const className = sanitizeClassName(`${objectRef.name}${suffix}`);
    const targetDir = path.join(workspaceFolder, targetFolder);
    const targetFile = path.join(targetDir, `${className}.cs`);

    const content = templateText
      .split('{{ClassName}}').join(className)
      .split('{{ObjectName}}').join(objectRef.name)
      .split('{{Schema}}').join(objectRef.schema)
      .split('{{ObjectType}}').join(objectRef.objectType)
      .split('{{DatabaseType}}').join(connection.databaseType)
      .split('{{DatabaseName}}').join(connection.databaseName)
      .split('{{Namespace}}').join(namespaceValue);

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

function sanitizeClassName(value: string): string {
  return value.replace(/[^a-zA-Z0-9_]/g, '_');
}

function generateClassTemplate(className: string, objectRef: DatabaseObjectReference, namespace?: string): string {
  const body = `// Auto-generated test class by DbSqlLikeMem VS Code extension\n` +
    `// Source: ${objectRef.objectType} ${objectRef.schema}.${objectRef.name}\n` +
    `using Xunit;\n\n` +
    `public class ${className}\n` +
    `{\n` +
    `    [Fact]\n` +
    `    public void Should_validate_${sanitizeClassName(objectRef.name)}()\n` +
    `    {\n` +
    `        // TODO: implementar cenário de teste\n` +
    `    }\n` +
    `}\n`;

  if (!namespace?.trim()) {
    return body;
  }

  return `namespace ${namespace.trim()}\n{\n${indentMultiline(body, 1)}}\n`;
}

function indentMultiline(value: string, level: number): string {
  const indent = '    '.repeat(level);
  return value
    .split('\n')
    .map((line) => line ? `${indent}${line}` : line)
    .join('\n');
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

  const databaseType = await vscode.window.showQuickPick(['SqlServer', 'PostgreSql', 'Oracle', 'MySql', 'Sqlite'], {
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
    prompt: vscode.l10n.t('Connection string (stored locally in extension storage)'),
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


function getManagerHtml(state: ExtensionState): string {
  const tr = {
    noConnectionConfigured: vscode.l10n.t('No connection configured.'),
    addConnectionForMappings: vscode.l10n.t('Add a connection to configure mappings.'),
    name: vscode.l10n.t('Name'),
    type: vscode.l10n.t('Type'),
    database: vscode.l10n.t('Database'),
    actions: vscode.l10n.t('Actions'),
    editConnection: vscode.l10n.t('Edit connection'),
    deleteConnection: vscode.l10n.t('Delete connection'),
    connection: vscode.l10n.t('Connection'),
    managerTitle: vscode.l10n.t('DbSqlLikeMem Manager'),
    visualInterface: vscode.l10n.t('DbSqlLikeMem - Visual Interface'),
    addEditConnection: vscode.l10n.t('Add/Edit Connection'),
    databaseType: vscode.l10n.t('Database type'),
    primaryDatabase: vscode.l10n.t('Primary Database / Schema'),
    connectionString: vscode.l10n.t('Connection string'),
    saveConnection: vscode.l10n.t('Save connection'),
    configureMappings: vscode.l10n.t('Configure Mappings'),
    tableFolder: vscode.l10n.t('Table folder'),
    tableSuffix: vscode.l10n.t('Table suffix'),
    viewFolder: vscode.l10n.t('View folder'),
    viewSuffix: vscode.l10n.t('View suffix'),
    procedureFolder: vscode.l10n.t('Procedure folder'),
    procedureSuffix: vscode.l10n.t('Procedure suffix'),
    optionalNamespace: vscode.l10n.t('Namespace (optional)'),
    saveMappings: vscode.l10n.t('Save mappings'),
    registeredConnections: vscode.l10n.t('Registered connections'),
    mappingsSummary: vscode.l10n.t('Mappings summary')
  };

  const connectionOptions = state.connections
    .map((c) => `<option value="${escapeHtml(c.id)}">${escapeHtml(c.name)} (${escapeHtml(c.databaseType)} / ${escapeHtml(c.databaseName)})</option>`)
    .join('');

  const mappingByConnection = new Map(state.mappingConfigurations.map((m) => [m.connectionId, m]));
  const mappingFormData = JSON.stringify(Object.fromEntries(
    state.mappingConfigurations.map((m) => [
      m.connectionId,
      Object.fromEntries(m.mappings.map((x) => [x.objectType, { targetFolder: x.targetFolder, fileSuffix: x.fileSuffix, namespace: x.namespace ?? '' }]))
    ])
  ));
  const connectionsTable = state.connections.length === 0
    ? `<p><em>${escapeHtml(tr.noConnectionConfigured)}</em></p>`
    : `<table><thead><tr><th>${escapeHtml(tr.name)}</th><th>${escapeHtml(tr.type)}</th><th>${escapeHtml(tr.database)}</th><th>${escapeHtml(tr.actions)}</th></tr></thead><tbody>${state.connections
      .map((c) => `<tr><td>${escapeHtml(c.name)}</td><td>${escapeHtml(c.databaseType)}</td><td>${escapeHtml(c.databaseName)}</td><td class="actions"><button class="icon-btn" title="${escapeHtml(tr.editConnection)}" aria-label="${escapeHtml(tr.editConnection)}" data-edit="${escapeHtml(c.id)}" data-name="${escapeHtml(c.name)}" data-type="${escapeHtml(c.databaseType)}" data-db="${escapeHtml(c.databaseName)}">✏️</button><button class="icon-btn" title="${escapeHtml(tr.deleteConnection)}" aria-label="${escapeHtml(tr.deleteConnection)}" data-remove="${escapeHtml(c.id)}">🗑️</button></td></tr>`)
      .join('')}</tbody></table>`;

  const mappingsTable = state.connections.length === 0
    ? `<p><em>${escapeHtml(tr.addConnectionForMappings)}</em></p>`
    : `<table><thead><tr><th>${escapeHtml(tr.connection)}</th><th>Table</th><th>View</th><th>Procedure</th></tr></thead><tbody>${state.connections
      .map((c) => {
        const map = mappingByConnection.get(c.id);
        const byType = (type: DatabaseObjectType): string => {
          const m = map?.mappings.find((x) => x.objectType === type);
          return m ? `${escapeHtml(m.targetFolder)} / ${escapeHtml(m.fileSuffix)}` : '-';
        };
        return `<tr><td>${escapeHtml(c.name)}</td><td>${byType('Table')}</td><td>${byType('View')}</td><td>${byType('Procedure')}</td></tr>`;
      }).join('')}</tbody></table>`;

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${escapeHtml(tr.managerTitle)}</title>
  <style>
    body { font-family: var(--vscode-font-family); color: var(--vscode-foreground); padding: 16px; }
    h2 { margin-top: 20px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .panel { border: 1px solid var(--vscode-panel-border); padding: 12px; border-radius: 6px; }
    label { display: block; margin-top: 8px; font-size: 12px; opacity: 0.9; }
    input, select { width: 100%; margin-top: 4px; padding: 6px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
    button { margin-top: 10px; padding: 6px 10px; }
    .icon-btn { width: 30px; height: 30px; padding: 0; margin-top: 0; margin-right: 4px; font-size: 16px; line-height: 1; }
    .actions { white-space: nowrap; }
    table { width: 100%; border-collapse: collapse; margin-top: 8px; }
    th, td { border: 1px solid var(--vscode-panel-border); padding: 6px; text-align: left; }
    .full { grid-column: 1 / -1; }
  </style>
</head>
<body>
  <h1>${escapeHtml(tr.visualInterface)}</h1>
  <div class="grid">
    <section class="panel">
      <h2>${escapeHtml(tr.addEditConnection)}</h2>
      <input id="connectionIdHidden" type="hidden" />
      <label>${escapeHtml(tr.name)}</label><input id="name" />
      <label>${escapeHtml(tr.databaseType)}</label>
      <select id="databaseType">
        <option>SqlServer</option><option>PostgreSql</option><option>Oracle</option><option>MySql</option><option>Sqlite</option><option>Db2</option>
      </select>
      <label>${escapeHtml(tr.primaryDatabase)}</label><input id="databaseName" />
      <label>${escapeHtml(tr.connectionString)}</label><input id="connectionString" type="password" />
      <button id="saveConnection">${escapeHtml(tr.saveConnection)}</button>
    </section>

    <section class="panel">
      <h2>${escapeHtml(tr.configureMappings)}</h2>
      <label>${escapeHtml(tr.connection)}</label>
      <select id="connectionId">${connectionOptions}</select>
      <label>${escapeHtml(tr.tableFolder)}</label><input id="tableFolder" value="src/Models/Tables" />
      <label>${escapeHtml(tr.tableSuffix)}</label><input id="tableSuffix" value="TableTests" />
      <label>${escapeHtml(tr.viewFolder)}</label><input id="viewFolder" value="src/Models/Views" />
      <label>${escapeHtml(tr.viewSuffix)}</label><input id="viewSuffix" value="ViewTests" />
      <label>${escapeHtml(tr.procedureFolder)}</label><input id="procedureFolder" value="src/Models/Procedures" />
      <label>${escapeHtml(tr.procedureSuffix)}</label><input id="procedureSuffix" value="ProcedureTests" />
      <label>${escapeHtml(tr.optionalNamespace)}</label><input id="namespace" />
      <button id="saveMapping">${escapeHtml(tr.saveMappings)}</button>
    </section>

    <section class="panel full">
      <h2>${escapeHtml(tr.registeredConnections)}</h2>
      ${connectionsTable}
    </section>

    <section class="panel full">
      <h2>${escapeHtml(tr.mappingsSummary)}</h2>
      ${mappingsTable}
    </section>
  </div>

  <script>
    const vscode = acquireVsCodeApi();
    const mappingFormData = ${mappingFormData};

    function hydrateMappingForm() {
      const connectionId = document.getElementById('connectionId').value;
      const selected = mappingFormData[connectionId] || {};
      document.getElementById('tableFolder').value = selected.Table?.targetFolder || 'src/Models/Tables';
      document.getElementById('tableSuffix').value = selected.Table?.fileSuffix || 'TableTests';
      document.getElementById('viewFolder').value = selected.View?.targetFolder || 'src/Models/Views';
      document.getElementById('viewSuffix').value = selected.View?.fileSuffix || 'ViewTests';
      document.getElementById('procedureFolder').value = selected.Procedure?.targetFolder || 'src/Models/Procedures';
      document.getElementById('procedureSuffix').value = selected.Procedure?.fileSuffix || 'ProcedureTests';
      document.getElementById('namespace').value = selected.Table?.namespace || selected.View?.namespace || selected.Procedure?.namespace || '';
    }

    document.getElementById('saveConnection')?.addEventListener('click', () => {
      vscode.postMessage({
        type: 'saveConnection',
        connectionId: document.getElementById('connectionIdHidden').value,
        name: document.getElementById('name').value,
        databaseType: document.getElementById('databaseType').value,
        databaseName: document.getElementById('databaseName').value,
        connectionString: document.getElementById('connectionString').value
      });
    });

    document.getElementById('connectionId')?.addEventListener('change', hydrateMappingForm);
    hydrateMappingForm();

    document.getElementById('saveMapping')?.addEventListener('click', () => {
      vscode.postMessage({
        type: 'saveMapping',
        connectionId: document.getElementById('connectionId').value,
        tableFolder: document.getElementById('tableFolder').value,
        tableSuffix: document.getElementById('tableSuffix').value,
        viewFolder: document.getElementById('viewFolder').value,
        viewSuffix: document.getElementById('viewSuffix').value,
        procedureFolder: document.getElementById('procedureFolder').value,
        procedureSuffix: document.getElementById('procedureSuffix').value,
        namespace: document.getElementById('namespace').value
      });
    });

    document.querySelectorAll('[data-edit]').forEach((btn) => {
      btn.addEventListener('click', () => {
        document.getElementById('connectionIdHidden').value = btn.getAttribute('data-edit') || '';
        document.getElementById('name').value = btn.getAttribute('data-name') || '';
        document.getElementById('databaseType').value = btn.getAttribute('data-type') || 'SqlServer';
        document.getElementById('databaseName').value = btn.getAttribute('data-db') || '';
      });
    });

    document.querySelectorAll('[data-remove]').forEach((btn) => {
      btn.addEventListener('click', () => {
        vscode.postMessage({ type: 'removeConnection', connectionId: btn.getAttribute('data-remove') });
      });
    });
  </script>
</body>
</html>`;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function findFileCaseInsensitive(root: string, fileName: string): Promise<boolean> {
  const entries = await fs.readdir(root, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(root, entry.name);

    if (entry.isFile() && entry.name.toLowerCase() === fileName.toLowerCase()) {
      return true;
    }

    if (entry.isDirectory()) {
      const found = await findFileCaseInsensitive(fullPath, fileName);
      if (found) {
        return true;
      }
    }
  }

  return false;
}

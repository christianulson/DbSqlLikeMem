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


interface GenerationPlan {
  objectRef: DatabaseObjectReference;
  targetFile: string;
  content: string;
}
interface TreeNode {
  id: string;
  label: string;
  kind: 'dbType' | 'database' | 'objectType' | 'object';
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
}

class SqlMetadataProvider implements DatabaseMetadataProvider {
  private warnedConnectionIds = new Set<string>();

  public async getObjects(connection: ConnectionDefinition): Promise<DatabaseObjectReference[]> {
    if (connection.databaseType.toLowerCase() !== 'sqlserver') {
      return [];
    }

    const parsed = parseSqlServerConnectionString(connection.connectionString);
    if (!parsed.server) {
      this.warnConnectionFailure(connection, 'Connection string sem servidor (Server/Data Source).');
      return [];
    }

    const query = "SET NOCOUNT ON; SELECT TABLE_SCHEMA AS [schema], TABLE_NAME AS [name], CASE TABLE_TYPE WHEN 'BASE TABLE' THEN 'Table' ELSE 'View' END AS [objectType] FROM INFORMATION_SCHEMA.TABLES UNION ALL SELECT ROUTINE_SCHEMA AS [schema], ROUTINE_NAME AS [name], 'Procedure' AS [objectType] FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY [schema], [name];";
    const args = ['-S', parsed.server, '-W', '-s', '|', '-Q', query, '-h', '-1'];

    if (parsed.database) {
      args.push('-d', parsed.database);
    }

    if (parsed.userId && parsed.password) {
      args.push('-U', parsed.userId, '-P', parsed.password);
    } else {
      args.push('-E');
    }

    try {
      const execFileAsync = promisify(execFile);
      const { stdout } = await execFileAsync('sqlcmd', args, { maxBuffer: 10 * 1024 * 1024 });
      return stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line && line.includes('|'))
        .map((line) => line.split('|').map((x) => x.trim()))
        .filter((parts) => parts.length >= 3)
        .map((parts) => ({ schema: parts[0], name: parts[1], objectType: parts[2] as DatabaseObjectType }))
        .filter((x) => (x.objectType === 'Table' || x.objectType === 'View' || x.objectType === 'Procedure') && x.schema && x.name);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.warnConnectionFailure(connection, message);
      return [];
    }
  }

  private warnConnectionFailure(connection: ConnectionDefinition, message: string): void {
    if (this.warnedConnectionIds.has(connection.id)) {
      return;
    }

    this.warnedConnectionIds.add(connection.id);
    vscode.window.showWarningMessage(`Falha ao conectar em ${connection.name}: ${message}`);
  }
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
        'DbSqlLikeMem Manager',
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
            vscode.window.showWarningMessage('Preencha todos os campos da conexão.');
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
            const connectionId = `${databaseType}-${databaseName}-${Date.now()}`;
            state.connections.push({
              id: connectionId,
              name,
              databaseType,
              databaseName,
              connectionString
            });

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
          vscode.window.showInformationMessage(existingId ? `Conexão ${name} atualizada.` : `Conexão ${name} salva.`);
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
          vscode.window.showInformationMessage('Conexão removida.');
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
            vscode.window.showWarningMessage('Preencha conexão, pastas e sufixos dos mapeamentos.');
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
          vscode.window.showInformationMessage('Mapeamentos salvos.');
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
        `Remover conexão ${connection.name}?`,
        { modal: true },
        'Remover'
      );

      if (confirmed !== 'Remover') {
        return;
      }

      state.connections = state.connections.filter((x) => x.id !== connection.id);
      state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connection.id);
      await saveState(context, state);
      await refreshTree();
      vscode.window.showInformationMessage(`Conexão ${connection.name} removida.`);
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.addConnection', async () => {
      const name = await vscode.window.showInputBox({ prompt: 'Nome da conexão' });
      if (!name) {
        return;
      }

      const databaseType = await vscode.window.showQuickPick(['SqlServer', 'PostgreSql', 'Oracle', 'MySql', 'Sqlite'], {
        placeHolder: 'Tipo do banco'
      });

      if (!databaseType) {
        return;
      }

      const databaseName = await vscode.window.showInputBox({ prompt: 'Nome do database/schema principal' });
      if (!databaseName) {
        return;
      }

      const connectionString = await vscode.window.showInputBox({
        prompt: 'Connection string (armazenada localmente no storage da extensão)',
        password: true,
        ignoreFocusOut: true
      });

      if (!connectionString) {
        return;
      }

      const connectionId = `${databaseType}-${databaseName}-${Date.now()}`;
      state.connections.push({
        id: connectionId,
        name,
        databaseType,
        databaseName,
        connectionString
      });

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
        prompt: 'Filtro (vazio para limpar)',
        value: state.filterText
      });

      if (text === undefined) {
        return;
      }

      const mode = await vscode.window.showQuickPick(['Like', 'Equals'], {
        placeHolder: 'Modo de filtro'
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
        prompt: 'Pasta para Table',
        value: currentByType.get('Table')?.targetFolder ?? 'src/Models/Tables'
      });
      if (!tableFolder) {
        return;
      }

      const tableSuffix = await vscode.window.showInputBox({
        prompt: 'Sufixo de classe para Table',
        value: currentByType.get('Table')?.fileSuffix ?? 'TableTests'
      });
      if (!tableSuffix) {
        return;
      }

      const viewFolder = await vscode.window.showInputBox({
        prompt: 'Pasta para View',
        value: currentByType.get('View')?.targetFolder ?? 'src/Models/Views'
      });
      if (!viewFolder) {
        return;
      }

      const viewSuffix = await vscode.window.showInputBox({
        prompt: 'Sufixo de classe para View',
        value: currentByType.get('View')?.fileSuffix ?? 'ViewTests'
      });
      if (!viewSuffix) {
        return;
      }

      const procedureFolder = await vscode.window.showInputBox({
        prompt: 'Pasta para Procedure',
        value: currentByType.get('Procedure')?.targetFolder ?? 'src/Models/Procedures'
      });
      if (!procedureFolder) {
        return;
      }

      const procedureSuffix = await vscode.window.showInputBox({
        prompt: 'Sufixo de classe para Procedure',
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
      vscode.window.showInformationMessage(`Mapeamentos salvos para ${connection.name}.`);
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.configureTemplates', async () => {
      const modelTemplatePath = await vscode.window.showInputBox({
        prompt: 'Template de modelo (caminho relativo ao workspace)',
        value: state.templateSettings.modelTemplatePath || 'templates/model.template.txt'
      });
      if (modelTemplatePath === undefined) {
        return;
      }

      const repositoryTemplatePath = await vscode.window.showInputBox({
        prompt: 'Template de repositório (caminho relativo ao workspace)',
        value: state.templateSettings.repositoryTemplatePath || 'templates/repository.template.txt'
      });
      if (repositoryTemplatePath === undefined) {
        return;
      }

      const modelTargetFolder = await vscode.window.showInputBox({
        prompt: 'Pasta destino para classes de modelo',
        value: state.templateSettings.modelTargetFolder
      });
      if (!modelTargetFolder) {
        return;
      }

      const repositoryTargetFolder = await vscode.window.showInputBox({
        prompt: 'Pasta destino para classes de repositório',
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
      vscode.window.showInformationMessage('Templates de geração configurados.');
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.generateClasses', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item);
      if (!connection) {
        return;
      }

      const mapping = state.mappingConfigurations.find((x) => x.connectionId === connection.id);
      if (!mapping) {
        vscode.window.showWarningMessage('Configure os mapeamentos antes de gerar as classes de teste.');
        return;
      }

      const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
      if (!workspaceFolder) {
        vscode.window.showWarningMessage('Abra uma pasta no VS Code para gerar arquivos.');
        return;
      }

      const objects = await metadataProvider.getObjects(connection);
      const filtered = applyFilter(objects, state.filterText, state.filterMode);
      const plans: GenerationPlan[] = [];

      for (const objectRef of filtered) {
        const objectMapping = mapping.mappings.find((x) => x.objectType === objectRef.objectType);
        if (!objectMapping) {
          continue;
        }

        const className = sanitizeClassName(objectRef.name + objectMapping.fileSuffix);
        const targetDir = path.join(workspaceFolder, objectMapping.targetFolder);
        const targetFile = path.join(targetDir, `${className}.cs`);
        plans.push({ objectRef, targetFile, content: generateClassTemplate(className, objectRef) });
      }

      if (!await confirmOverwrite(plans, 'classes de teste')) {
        return;
      }

      for (const plan of plans) {
        await fs.mkdir(path.dirname(plan.targetFile), { recursive: true });
        await fs.writeFile(plan.targetFile, plan.content, 'utf8');
      }

      vscode.window.showInformationMessage(`Classes de teste geradas para ${connection.name}.`);
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.generateModelClasses', async (item?: DbNodeItem) => {
      await generateTemplateBasedFiles(state, metadataProvider, await resolveConnectionFromItem(state.connections, item), 'model');
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.generateRepositoryClasses', async (item?: DbNodeItem) => {
      await generateTemplateBasedFiles(state, metadataProvider, await resolveConnectionFromItem(state.connections, item), 'repository');
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.checkConsistency', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item);
      if (!connection) {
        return;
      }

      const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
      if (!workspaceFolder) {
        vscode.window.showWarningMessage('Abra uma pasta no VS Code para checar consistência.');
        return;
      }

      const objects = await metadataProvider.getObjects(connection);
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
        vscode.window.showInformationMessage(`Consistência OK para ${connection.name} (status verde).`);
        return;
      }

      const preview = missing.slice(0, 5).join(', ');
      vscode.window.showWarningMessage(`Objetos sem classe local (${missing.length}) - status amarelo/vermelho: ${preview}`);
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.exportState', async () => {
      const saveUri = await vscode.window.showSaveDialog({ filters: { Json: ['json'] } });
      if (!saveUri) {
        return;
      }

      await vscode.workspace.fs.writeFile(saveUri, Buffer.from(JSON.stringify(state, null, 2), 'utf8'));
      vscode.window.showInformationMessage('Estado exportado com sucesso.');
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
      vscode.window.showInformationMessage('Estado importado com sucesso.');
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
          return {
            id: `obj-${connection.id}-${objectType}-${objectRef.schema}.${objectRef.name}`,
            label: `${objectRef.schema}.${objectRef.name}`,
            kind: 'object',
            objectType,
            objectRef,
            connectionId: connection.id,
            generationStatus
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
  kind: GenerationKind
): Promise<void> {
  if (!connection) {
    return;
  }

  const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  if (!workspaceFolder) {
    vscode.window.showWarningMessage('Abra uma pasta no VS Code para gerar arquivos.');
    return;
  }

  const templateRelativePath = kind === 'model' ? state.templateSettings.modelTemplatePath : state.templateSettings.repositoryTemplatePath;
  const targetFolder = kind === 'model' ? state.templateSettings.modelTargetFolder : state.templateSettings.repositoryTargetFolder;
  const suffix = kind === 'model' ? 'Model' : 'Repository';
  const objects = applyFilter(await metadataProvider.getObjects(connection), state.filterText, state.filterMode);

  let templateText = kind === 'model'
    ? '// Model generated from {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n'
    : '// Repository generated from {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n';

  if (templateRelativePath.trim()) {
    const templateFullPath = path.join(workspaceFolder, templateRelativePath);
    try {
      templateText = await fs.readFile(templateFullPath, 'utf8');
    } catch {
      vscode.window.showWarningMessage(`Template não encontrado: ${templateRelativePath}. Usando template padrão.`);
    }
  }

  const plans: GenerationPlan[] = [];
  for (const objectRef of objects) {
    const className = sanitizeClassName(`${objectRef.name}${suffix}`);
    const targetDir = path.join(workspaceFolder, targetFolder);
    const targetFile = path.join(targetDir, `${className}.cs`);

    const content = templateText
      .split('{{ClassName}}').join(className)
      .split('{{ObjectName}}').join(objectRef.name)
      .split('{{Schema}}').join(objectRef.schema)
      .split('{{ObjectType}}').join(objectRef.objectType)
      .split('{{DatabaseType}}').join(connection.databaseType)
      .split('{{DatabaseName}}').join(connection.databaseName);

    plans.push({ objectRef, targetFile, content });
  }

  const label = kind === 'model' ? 'modelos' : 'repositórios';
  if (!await confirmOverwrite(plans, label)) {
    return;
  }

  for (const plan of plans) {
    await fs.mkdir(path.dirname(plan.targetFile), { recursive: true });
    await fs.writeFile(plan.targetFile, plan.content, 'utf8');
  }

  vscode.window.showInformationMessage(`${kind === 'model' ? 'Modelos' : 'Repositórios'} gerados para ${connection.name}.`);
}


async function confirmOverwrite(plans: GenerationPlan[], label: string): Promise<boolean> {
  if (plans.length === 0) {
    vscode.window.showWarningMessage(`Nenhum objeto elegível para gerar ${label}.`);
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
    `${existing.length} arquivo(s) serão sobrescritos (${preview}). Deseja continuar?`,
    'Gerar e sobrescrever',
    'Cancelar'
  );

  return choice === 'Gerar e sobrescrever';
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

function generateClassTemplate(className: string, objectRef: DatabaseObjectReference): string {
  return `// Auto-generated test class by DbSqlLikeMem VS Code extension\n` +
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
}

async function pickConnection(connections: ConnectionDefinition[]): Promise<ConnectionDefinition | undefined> {
  if (connections.length === 0) {
    vscode.window.showWarningMessage('Nenhuma conexão configurada.');
    return undefined;
  }

  const selected = await vscode.window.showQuickPick(
    connections.map((x) => ({
      label: x.name,
      description: `${x.databaseType} - ${x.databaseName}`,
      connection: x
    })),
    { placeHolder: 'Selecione uma conexão' }
  );

  return selected?.connection;
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
    ? '<p><em>Nenhuma conexão configurada.</em></p>'
    : `<table><thead><tr><th>Nome</th><th>Tipo</th><th>Database</th><th>Ações</th></tr></thead><tbody>${state.connections
      .map((c) => `<tr><td>${escapeHtml(c.name)}</td><td>${escapeHtml(c.databaseType)}</td><td>${escapeHtml(c.databaseName)}</td><td class="actions"><button class="icon-btn" title="Editar conexão" aria-label="Editar conexão" data-edit="${escapeHtml(c.id)}" data-name="${escapeHtml(c.name)}" data-type="${escapeHtml(c.databaseType)}" data-db="${escapeHtml(c.databaseName)}">✏️</button><button class="icon-btn" title="Excluir conexão" aria-label="Excluir conexão" data-remove="${escapeHtml(c.id)}">🗑️</button></td></tr>`)
      .join('')}</tbody></table>`;

  const mappingsTable = state.connections.length === 0
    ? '<p><em>Adicione uma conexão para configurar mapeamentos.</em></p>'
    : `<table><thead><tr><th>Conexão</th><th>Table</th><th>View</th><th>Procedure</th></tr></thead><tbody>${state.connections
      .map((c) => {
        const map = mappingByConnection.get(c.id);
        const byType = (type: DatabaseObjectType): string => {
          const m = map?.mappings.find((x) => x.objectType === type);
          return m ? `${escapeHtml(m.targetFolder)} / ${escapeHtml(m.fileSuffix)}` : '-';
        };
        return `<tr><td>${escapeHtml(c.name)}</td><td>${byType('Table')}</td><td>${byType('View')}</td><td>${byType('Procedure')}</td></tr>`;
      }).join('')}</tbody></table>`;

  return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>DbSqlLikeMem Manager</title>
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
  <h1>DbSqlLikeMem - Interface Gráfica</h1>
  <div class="grid">
    <section class="panel">
      <h2>Adicionar/Editar Conexão</h2>
      <input id="connectionIdHidden" type="hidden" />
      <label>Nome</label><input id="name" />
      <label>Tipo do banco</label>
      <select id="databaseType">
        <option>SqlServer</option><option>PostgreSql</option><option>Oracle</option><option>MySql</option><option>Sqlite</option><option>Db2</option>
      </select>
      <label>Database / Schema principal</label><input id="databaseName" />
      <label>Connection string</label><input id="connectionString" type="password" />
      <button id="saveConnection">Salvar conexão</button>
    </section>

    <section class="panel">
      <h2>Configurar Mapeamentos</h2>
      <label>Conexão</label>
      <select id="connectionId">${connectionOptions}</select>
      <label>Pasta Table</label><input id="tableFolder" value="src/Models/Tables" />
      <label>Sufixo Table</label><input id="tableSuffix" value="TableTests" />
      <label>Pasta View</label><input id="viewFolder" value="src/Models/Views" />
      <label>Sufixo View</label><input id="viewSuffix" value="ViewTests" />
      <label>Pasta Procedure</label><input id="procedureFolder" value="src/Models/Procedures" />
      <label>Sufixo Procedure</label><input id="procedureSuffix" value="ProcedureTests" />
      <label>Namespace (opcional)</label><input id="namespace" />
      <button id="saveMapping">Salvar mapeamentos</button>
    </section>

    <section class="panel full">
      <h2>Conexões cadastradas</h2>
      ${connectionsTable}
    </section>

    <section class="panel full">
      <h2>Resumo dos mapeamentos</h2>
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

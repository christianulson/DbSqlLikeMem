import * as fs from 'node:fs/promises';
import * as path from 'node:path';
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
  filterText: string;
  filterMode: FilterMode;
}

interface TreeNode {
  id: string;
  label: string;
  kind: 'dbType' | 'database' | 'objectType' | 'object';
  objectType?: DatabaseObjectType;
  children?: TreeNode[];
  connectionId?: string;
  objectRef?: DatabaseObjectReference;
}

const DEFAULT_STATE: ExtensionState = {
  connections: [],
  mappingConfigurations: [],
  filterText: '',
  filterMode: 'Like'
};

class DbNodeItem extends vscode.TreeItem {
  constructor(public readonly node: TreeNode) {
    super(node.label, node.children?.length ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.None);
    this.id = node.id;

    if (node.kind === 'object') {
      this.contextValue = 'db-object';
      this.description = node.objectType;
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

class FakeMetadataProvider {
  public getObjects(connection: ConnectionDefinition): DatabaseObjectReference[] {
    const schema = connection.databaseType.toLowerCase() === 'oracle' ? 'SYSTEM' : 'dbo';

    return [
      { schema, name: 'Customers', objectType: 'Table' },
      { schema, name: 'Orders', objectType: 'Table' },
      { schema, name: 'ActiveCustomers', objectType: 'View' },
      { schema, name: 'RecalculateBalances', objectType: 'Procedure' }
    ];
  }
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  const metadataProvider = new FakeMetadataProvider();
  const treeProvider = new ConnectionTreeDataProvider();

  let state = await loadState(context);

  const refreshTree = (): void => {
    const tree = buildTree(state.connections, state.filterText, state.filterMode, metadataProvider);
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
          const name = String(payload.name ?? '').trim();
          const databaseType = String(payload.databaseType ?? '').trim();
          const databaseName = String(payload.databaseName ?? '').trim();
          const connectionString = String(payload.connectionString ?? '').trim();

          if (!name || !databaseType || !databaseName || !connectionString) {
            vscode.window.showWarningMessage('Preencha todos os campos da conexão.');
            return;
          }

          state.connections.push({
            id: `${databaseType}-${databaseName}-${Date.now()}`,
            name,
            databaseType,
            databaseName,
            connectionString
          });

          await saveState(context, state);
          refreshTree();
          render();
          vscode.window.showInformationMessage(`Conexão ${name} salva.`);
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
          refreshTree();
          render();
          vscode.window.showInformationMessage('Conexão removida.');
          return;
        }

        if (payload.type === 'saveMapping') {
          const connectionId = String(payload.connectionId ?? '').trim();
          const folder = String(payload.folder ?? '').trim();
          const fileSuffix = String(payload.fileSuffix ?? '').trim();
          const namespace = String(payload.namespace ?? '').trim();

          if (!connectionId || !folder || !fileSuffix) {
            vscode.window.showWarningMessage('Preencha conexão, pasta e sufixo do mapeamento.');
            return;
          }

          const mapping: ConnectionMappingConfiguration = {
            connectionId,
            mappings: ['Table', 'View', 'Procedure'].map((objectType) => ({
              objectType: objectType as DatabaseObjectType,
              targetFolder: folder,
              fileSuffix,
              namespace: namespace || undefined
            }))
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

      state.connections.push({
        id: `${databaseType}-${databaseName}-${Date.now()}`,
        name,
        databaseType,
        databaseName,
        connectionString
      });

      await saveState(context, state);
      refreshTree();
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
      refreshTree();
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.configureMappings', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item) ?? await pickConnection(state.connections);
      if (!connection) {
        return;
      }

      const folder = await vscode.window.showInputBox({
        prompt: 'Pasta alvo para classes (relativa ao workspace, ex: src/Domain/Models)',
        value: 'src/Models'
      });

      if (!folder) {
        return;
      }

      const fileSuffix = await vscode.window.showInputBox({
        prompt: 'Sufixo dos arquivos/classe (ex: Entity)',
        value: 'Entity'
      });

      if (!fileSuffix) {
        return;
      }

      const mapping: ConnectionMappingConfiguration = {
        connectionId: connection.id,
        mappings: ['Table', 'View', 'Procedure'].map((objectType) => ({
          objectType: objectType as DatabaseObjectType,
          targetFolder: folder,
          fileSuffix
        }))
      };

      state.mappingConfigurations = state.mappingConfigurations.filter((x) => x.connectionId !== connection.id);
      state.mappingConfigurations.push(mapping);
      await saveState(context, state);
      vscode.window.showInformationMessage(`Mapeamentos salvos para ${connection.name}.`);
    }),
    vscode.commands.registerCommand('dbSqlLikeMem.generateClasses', async (item?: DbNodeItem) => {
      const connection = await resolveConnectionFromItem(state.connections, item);
      if (!connection) {
        return;
      }

      const mapping = state.mappingConfigurations.find((x) => x.connectionId === connection.id);
      if (!mapping) {
        vscode.window.showWarningMessage('Configure os mapeamentos antes de gerar as classes.');
        return;
      }

      const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
      if (!workspaceFolder) {
        vscode.window.showWarningMessage('Abra uma pasta no VS Code para gerar arquivos.');
        return;
      }

      const objects = metadataProvider.getObjects(connection);
      const filtered = applyFilter(objects, state.filterText, state.filterMode);

      for (const objectRef of filtered) {
        const objectMapping = mapping.mappings.find((x) => x.objectType === objectRef.objectType);
        if (!objectMapping) {
          continue;
        }

        const className = sanitizeClassName(objectRef.name + objectMapping.fileSuffix);
        const targetDir = path.join(workspaceFolder, objectMapping.targetFolder);
        const targetFile = path.join(targetDir, `${className}.cs`);

        await fs.mkdir(targetDir, { recursive: true });
        await fs.writeFile(targetFile, generateClassTemplate(className, objectRef), 'utf8');
      }

      vscode.window.showInformationMessage(`Classes geradas para ${connection.name}.`);
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

      const objects = metadataProvider.getObjects(connection);
      const missing: string[] = [];

      for (const objectRef of objects) {
        const expected = sanitizeClassName(`${objectRef.name}Entity.cs`);
        const found = await findFileCaseInsensitive(workspaceFolder, expected);
        if (!found) {
          missing.push(`${objectRef.objectType}: ${objectRef.schema}.${objectRef.name}`);
        }
      }

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
      refreshTree();
      vscode.window.showInformationMessage('Estado importado com sucesso.');
    })
  );

  refreshTree();
}

export function deactivate(): void {
  // no-op
}

function buildTree(
  connections: ConnectionDefinition[],
  filterText: string,
  filterMode: FilterMode,
  provider: FakeMetadataProvider
): TreeNode[] {
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

    const objects = applyFilter(provider.getObjects(connection), filterText, filterMode);
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
        children: objectGroups[objectType].map((objectRef) => ({
          id: `obj-${connection.id}-${objectType}-${objectRef.schema}.${objectRef.name}`,
          label: `${objectRef.schema}.${objectRef.name}`,
          kind: 'object',
          objectType,
          objectRef,
          connectionId: connection.id
        })),
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
  const raw = context.globalState.get<ExtensionState>('dbSqlLikeMem.state');
  return raw ?? structuredClone(DEFAULT_STATE);
}

async function saveState(context: vscode.ExtensionContext, state: ExtensionState): Promise<void> {
  await context.globalState.update('dbSqlLikeMem.state', state);
}

function sanitizeClassName(value: string): string {
  return value.replace(/[^a-zA-Z0-9_]/g, '_');
}

function generateClassTemplate(className: string, objectRef: DatabaseObjectReference): string {
  return `// Auto-generated by DbSqlLikeMem VS Code extension\n` +
    `// Source: ${objectRef.objectType} ${objectRef.schema}.${objectRef.name}\n` +
    `public class ${className}\n` +
    `{\n` +
    `    // TODO: map columns\n` +
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
  const connectionsTable = state.connections.length === 0
    ? '<p><em>Nenhuma conexão configurada.</em></p>'
    : `<table><thead><tr><th>Nome</th><th>Tipo</th><th>Database</th><th>Ações</th></tr></thead><tbody>${state.connections
      .map((c) => `<tr><td>${escapeHtml(c.name)}</td><td>${escapeHtml(c.databaseType)}</td><td>${escapeHtml(c.databaseName)}</td><td><button data-remove="${escapeHtml(c.id)}">Remover</button></td></tr>`)
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
    table { width: 100%; border-collapse: collapse; margin-top: 8px; }
    th, td { border: 1px solid var(--vscode-panel-border); padding: 6px; text-align: left; }
    .full { grid-column: 1 / -1; }
  </style>
</head>
<body>
  <h1>DbSqlLikeMem - Interface Gráfica</h1>
  <div class="grid">
    <section class="panel">
      <h2>Adicionar Conexão</h2>
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
      <label>Pasta alvo</label><input id="folder" value="src/Models" />
      <label>Sufixo de arquivo/classe</label><input id="fileSuffix" value="Entity" />
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
    document.getElementById('saveConnection')?.addEventListener('click', () => {
      vscode.postMessage({
        type: 'saveConnection',
        name: document.getElementById('name').value,
        databaseType: document.getElementById('databaseType').value,
        databaseName: document.getElementById('databaseName').value,
        connectionString: document.getElementById('connectionString').value
      });
    });

    document.getElementById('saveMapping')?.addEventListener('click', () => {
      vscode.postMessage({
        type: 'saveMapping',
        connectionId: document.getElementById('connectionId').value,
        folder: document.getElementById('folder').value,
        fileSuffix: document.getElementById('fileSuffix').value,
        namespace: document.getElementById('namespace').value
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

using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.ComponentModel;
using System.IO;
using System.Text;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.Core.Persistence;
using DbSqlLikeMem.VisualStudioExtension.Core.Services;
using DbSqlLikeMem.VisualStudioExtension.Core.Validation;
using DbSqlLikeMem.VisualStudioExtension.Services;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

/// <summary>
/// Represents the view model that drives the DbSqlLikeMem tool window UI.
/// Representa o view model que controla a interface da janela DbSqlLikeMem.
/// </summary>
public sealed class DbSqlLikeMemToolWindowViewModel : INotifyPropertyChanged
{
    private readonly StatePersistenceService statePersistenceService = new();
    private readonly SqlDatabaseMetadataProvider metadataProvider = new(new AdoNetSqlQueryExecutor());
    private readonly SemaphoreSlim operationLock = new(1, 1);
    private readonly string stateFilePath;

    private CancellationTokenSource? currentOperationCts;

    private readonly ObservableCollection<ConnectionDefinition> connections = [];
    private readonly ObservableCollection<ConnectionMappingConfiguration> mappings = [];
    private readonly Dictionary<string, IReadOnlyCollection<DatabaseObjectReference>> objectsByConnection = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ObjectHealthResult> healthByObject = new(StringComparer.OrdinalIgnoreCase);
    private TemplateConfiguration templateConfiguration = TemplateConfiguration.Default;
    private readonly ObjectFilterService objectFilterService = new();
    private readonly Dictionary<string, (string Text, FilterMode Mode)> objectTypeFilters = new(StringComparer.OrdinalIgnoreCase);

    private string objectFilterText = string.Empty;
    private FilterMode objectFilterMode = FilterMode.Like;

    /// <summary>
    /// Initializes the view model, loads persisted state, and builds the initial tree.
    /// Inicializa o view model, carrega o estado persistido e monta a árvore inicial.
    /// </summary>
    public DbSqlLikeMemToolWindowViewModel()
    {
        stateFilePath = statePersistenceService.GetDefaultStatePath();
        LoadState();
        RefreshTree();
    }

    /// <summary>
    /// Gets the root explorer nodes shown in the tree.
    /// Obtém os nós raiz do explorador mostrados na árvore.
    /// </summary>
    public ObservableCollection<ExplorerNode> Nodes { get; } = [];

    /// <summary>
    /// Gets the current status message displayed to the user.
    /// Obtém a mensagem de status atual exibida ao usuário.
    /// </summary>
    public string StatusMessage { get; private set; } = "Pronto.";

    /// <summary>
    /// Gets a value indicating whether an operation is currently running.
    /// Obtém um valor que indica se uma operação está em andamento.
    /// </summary>
    public bool IsBusy { get; private set; }

    /// <summary>
    /// Gets or sets the text used to filter displayed database objects.
    /// Obtém ou define o texto usado para filtrar objetos de banco exibidos.
    /// </summary>
    public string ObjectFilterText
    {
        get => objectFilterText;
        set
        {
            if (string.Equals(objectFilterText, value, StringComparison.Ordinal))
            {
                return;
            }

            objectFilterText = value;
            OnPropertyChanged(nameof(ObjectFilterText));
            RefreshTree();
        }
    }

    /// <summary>
    /// Gets or sets the filtering mode used when applying object filters.
    /// Obtém ou define o modo de filtragem usado ao aplicar filtros de objetos.
    /// </summary>
    public FilterMode ObjectFilterMode
    {
        get => objectFilterMode;
        set
        {
            if (objectFilterMode == value)
            {
                return;
            }

            objectFilterMode = value;
            OnPropertyChanged(nameof(ObjectFilterMode));
            RefreshTree();
        }
    }

    /// <summary>
    /// Occurs when a bindable property value changes.
    /// Ocorre quando o valor de uma propriedade vinculável é alterado.
    /// </summary>
    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>
    /// Tests a database connection and returns whether it succeeds with a message.
    /// Testa uma conexão com banco e retorna se ela foi bem-sucedida com uma mensagem.
    /// </summary>
    public async Task<(bool Success, string Message)> TestConnectionAsync(string databaseType, string connectionString)
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var factory = AdoNetSqlQueryExecutor.GetFactory(databaseType);
            using var connection = factory.CreateConnection() ?? throw new InvalidOperationException("Falha ao criar conexão.");
            connection.ConnectionString = connectionString;
            await connection.OpenAsync(timeout.Token);
            return (true, "Conexão validada com sucesso.");
        }
        catch (Exception ex)
        {
            ExtensionLogger.Log($"TestConnectionAsync error: {ex}");
            return (false, $"Falha ao conectar: {ex.Message}");
        }
    }

    /// <summary>
    /// Adds a new connection and default mappings to the persisted state.
    /// Adiciona uma nova conexão e mapeamentos padrão ao estado persistido.
    /// </summary>
    public void AddConnection(string name, string databaseType, string connectionString)
    {
        var databaseName = ExtractDatabaseName(connectionString, name);
        var connection = new ConnectionDefinition(Guid.NewGuid().ToString("N"), databaseType, databaseName, connectionString, name);
        connections.Add(connection);

        if (!mappings.Any(m => m.ConnectionId == connection.Id))
        {
            mappings.Add(new ConnectionMappingConfiguration(connection.Id, CreateDefaultMappings("Generated", "{NamePascal}{Type}Factory.cs")));
        }

        SaveState();
        RefreshTree();
    }

    /// <summary>
    /// Gets a connection definition by its identifier.
    /// Obtém uma definição de conexão pelo identificador.
    /// </summary>
    public ConnectionDefinition? GetConnection(string connectionId)
        => connections.FirstOrDefault(c => c.Id == connectionId);

    /// <summary>
    /// Updates an existing connection represented by the selected explorer node.
    /// Atualiza uma conexão existente representada pelo nó selecionado no explorador.
    /// </summary>
    public void UpdateConnection(ExplorerNode selectedConnectionNode, string name, string databaseType, string connectionString)
    {
        if (selectedConnectionNode.ConnectionId is null)
        {
            return;
        }

        var old = connections.FirstOrDefault(c => c.Id == selectedConnectionNode.ConnectionId);
        if (old is null)
        {
            return;
        }

        var index = connections.IndexOf(old);
        var databaseName = ExtractDatabaseName(connectionString, name);
        connections[index] = new ConnectionDefinition(old.Id, databaseType, databaseName, connectionString, name);
        objectsByConnection.Remove(old.Id);
        healthByObject.Clear();
        SaveState();
        RefreshTree();
    }

    /// <summary>
    /// Removes a connection and related mappings from the current state.
    /// Remove uma conexão e os mapeamentos relacionados do estado atual.
    /// </summary>
    public void RemoveConnection(ExplorerNode selectedConnectionNode)
    {
        if (selectedConnectionNode.ConnectionId is null)
        {
            return;
        }

        var connection = connections.FirstOrDefault(c => c.Id == selectedConnectionNode.ConnectionId);
        if (connection is null)
        {
            return;
        }

        _ = connections.Remove(connection);
        var mapping = mappings.FirstOrDefault(m => m.ConnectionId == connection.Id);
        if (mapping is not null)
        {
            _ = mappings.Remove(mapping);
        }

        objectsByConnection.Remove(connection.Id);
        SaveState();
        RefreshTree();
    }

    /// <summary>
    /// Applies default mapping values to all configured connections.
    /// Aplica valores padrão de mapeamento para todas as conexões configuradas.
    /// </summary>
    public void ApplyDefaultMapping(string fileNamePattern, string outputDirectory)
    {
        for (var i = 0; i < mappings.Count; i++)
        {
            var current = mappings[i];
            mappings[i] = new ConnectionMappingConfiguration(current.ConnectionId, CreateDefaultMappings(outputDirectory, fileNamePattern));
        }

        SaveState();
        SetStatusMessage("Mapeamentos atualizados.");
    }

    /// <summary>
    /// Gets the current default mapping values used by the mapping dialog.
    /// Obtém os valores padrão de mapeamento usados pela janela de mapeamento.
    /// </summary>
    public (string FileNamePattern, string OutputDirectory) GetMappingDefaults()
    {
        var firstMapping = mappings.FirstOrDefault()?.Mappings.Values.FirstOrDefault();
        return (firstMapping?.FileNamePattern ?? "{NamePascal}{Type}Factory.cs", firstMapping?.OutputDirectory ?? "Generated");
    }

    public (string FilterText, FilterMode FilterMode) GetObjectTypeFilter(ExplorerNode objectTypeNode)
    {
        if (objectTypeNode.Kind != ExplorerNodeKind.ObjectType || objectTypeNode.ConnectionId is null || objectTypeNode.ObjectType is null)
        {
            return (string.Empty, FilterMode.Like);
        }

        var key = BuildObjectTypeFilterKey(objectTypeNode.ConnectionId, objectTypeNode.ObjectType.Value);
        return objectTypeFilters.TryGetValue(key, out var filter)
            ? filter
            : (string.Empty, FilterMode.Like);
    }

    public void SetObjectTypeFilter(ExplorerNode objectTypeNode, string filterText, FilterMode filterMode)
    {
        if (objectTypeNode.Kind != ExplorerNodeKind.ObjectType || objectTypeNode.ConnectionId is null || objectTypeNode.ObjectType is null)
        {
            return;
        }

        var key = BuildObjectTypeFilterKey(objectTypeNode.ConnectionId, objectTypeNode.ObjectType.Value);
        var normalized = filterText?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(normalized))
        {
            objectTypeFilters.Remove(key);
        }
        else
        {
            objectTypeFilters[key] = (normalized, filterMode);
        }

        RefreshTree();
    }

    public void ClearObjectTypeFilter(ExplorerNode objectTypeNode)
    {
        if (objectTypeNode.Kind != ExplorerNodeKind.ObjectType || objectTypeNode.ConnectionId is null || objectTypeNode.ObjectType is null)
        {
            return;
        }

        objectTypeFilters.Remove(BuildObjectTypeFilterKey(objectTypeNode.ConnectionId, objectTypeNode.ObjectType.Value));
        RefreshTree();
    }

    /// <summary>
    /// Gets the current template configuration.
    /// Obtém a configuração atual de templates.
    /// </summary>
    public TemplateConfiguration GetTemplateConfiguration() => templateConfiguration;

    /// <summary>
    /// Updates the template and output settings used for model and repository generation.
    /// Atualiza os templates e diretórios usados na geração de modelos e repositórios.
    /// </summary>
    public void ConfigureTemplates(string modelTemplatePath, string repositoryTemplatePath, string modelOutputDirectory, string repositoryOutputDirectory)
    {
        templateConfiguration = new TemplateConfiguration(
            NormalizePathOrEmpty(modelTemplatePath),
            NormalizePathOrEmpty(repositoryTemplatePath),
            NormalizePath(modelOutputDirectory),
            NormalizePath(repositoryOutputDirectory));
        SaveState();
        SetStatusMessage("Templates de model/repositório atualizados.");
    }


    /// <summary>
    /// Requests cancellation of the currently running operation.
    /// Solicita o cancelamento da operação em execução.
    /// </summary>
    public void CancelCurrentOperation()
    {
        currentOperationCts?.Cancel();
        SetStatusMessage("Cancelamento solicitado.");
    }


    /// <summary>
    /// Ensures objects for the selected connection are loaded (lazy load on tree selection).
    /// Garante que os objetos da conexão selecionada estejam carregados (carga sob demanda).
    /// </summary>
    public async Task EnsureConnectionObjectsLoadedAsync(ExplorerNode selectedNode)
    {
        var connectionId = selectedNode.Kind switch
        {
            ExplorerNodeKind.Connection => selectedNode.ConnectionId,
            ExplorerNodeKind.Schema => selectedNode.ConnectionId,
            ExplorerNodeKind.ObjectType => selectedNode.ConnectionId,
            ExplorerNodeKind.Object => selectedNode.ConnectionId,
            ExplorerNodeKind.TableDetailGroup => selectedNode.ConnectionId,
            ExplorerNodeKind.TableDetailItem => selectedNode.ConnectionId,
            _ => null
        };

        if (string.IsNullOrWhiteSpace(connectionId))
        {
            return;
        }

        var connection = connections.FirstOrDefault(c => c.Id == connectionId);
        if (connection is null)
        {
            return;
        }

        var needsObjectList = !objectsByConnection.ContainsKey(connectionId);
        var needsSelectedTableDetails = selectedNode.Kind == ExplorerNodeKind.Object
            && selectedNode.ObjectType == DatabaseObjectType.Table
            && (selectedNode.DatabaseObject?.Properties is null || selectedNode.DatabaseObject.Properties.Count == 0);

        if (!needsObjectList && !needsSelectedTableDetails)
        {
            return;
        }

        if (!TryBeginOperation($"Carregando objetos de {connection.FriendlyName}..."))
        {
            return;
        }

        try
        {
            var token = currentOperationCts?.Token ?? CancellationToken.None;

            if (needsObjectList)
            {
                var objects = await metadataProvider.ListObjectsAsync(connection, token);
                objectsByConnection[connection.Id] = await EnrichTableObjectsAsync(connection, objects, token);
            }

            if (selectedNode.Kind == ExplorerNodeKind.Object && selectedNode.ObjectType == DatabaseObjectType.Table)
            {
                await EnsureTableDetailsLoadedAsync(connection, selectedNode, token);
            }

            RefreshTree();
            SetStatusMessage($"Objetos carregados para {connection.FriendlyName}.");
        }
        catch (Exception ex)
        {
            ExtensionLogger.Log($"EnsureConnectionObjectsLoadedAsync error [{connection.DatabaseName}]: {ex}");
            SetStatusMessage($"Falha ao carregar objetos de {connection.FriendlyName}. Veja o log.");
        }
        finally
        {
            EndOperation();
        }
    }

    /// <summary>
    /// Reloads database objects for all configured connections.
    /// Recarrega os objetos de banco para todas as conexões configuradas.
    /// </summary>
    public async Task RefreshObjectsAsync()
    {
        if (!TryBeginOperation("Atualizando objetos de banco..."))
        {
            return;
        }

        try
        {
            objectsByConnection.Clear();
            healthByObject.Clear();

            var token = currentOperationCts?.Token ?? CancellationToken.None;
            var failures = new List<string>();

            var tasks = connections.Select(async connection =>
            {
                try
                {
                    var objects = await metadataProvider.ListObjectsAsync(connection, token);
                    var enriched = await EnrichTableObjectsAsync(connection, objects, token);
                    return (connection.Id, objects: enriched, error: (string?)null);
                }
                catch (Exception ex)
                {
                    ExtensionLogger.Log($"RefreshObjectsAsync error [{connection.DatabaseName}]: {ex}");
                    return (connection.Id, objects: (IReadOnlyCollection<DatabaseObjectReference>)[], error: $"{connection.DatabaseName}: {ex.Message}");
                }
            });

            var results = await Task.WhenAll(tasks);
            foreach (var result in results)
            {
                if (!string.IsNullOrWhiteSpace(result.error))
                {
                    failures.Add(result.error!);
                    continue;
                }

                objectsByConnection[result.Id] = result.objects;
            }

            RefreshTree();
            SetStatusMessage(failures.Count == 0
                ? $"Objetos atualizados para {objectsByConnection.Count} conexão(ões)."
                : $"Atualização parcial ({failures.Count} falha(s)). Veja o log para detalhes.");
        }
        finally
        {
            EndOperation();
        }
    }

    /// <summary>
    /// Previews generated files that would be overwritten for a selected node.
    /// Lista previamente os arquivos gerados que seriam sobrescritos para um nó selecionado.
    /// </summary>
    public IReadOnlyCollection<string> PreviewConflictsForNode(ExplorerNode node)
    {
        var connection = ResolveConnection(node);
        if (connection is null || !objectsByConnection.TryGetValue(connection.Id, out var objects))
        {
            return [];
        }

        var selectedObjects = ResolveSelectedObjects(node, objects).ToArray();
        if (selectedObjects.Length == 0)
        {
            return [];
        }

        var mapping = mappings.FirstOrDefault(m => m.ConnectionId == connection.Id)
            ?? new ConnectionMappingConfiguration(connection.Id, CreateDefaultMappings("Generated", "{NamePascal}{Type}Factory.cs"));

        return [.. FindExistingFiles(connection, mapping, selectedObjects)];
    }

    /// <summary>
    /// Generates test classes for the selected node and returns generated file paths.
    /// Gera classes de teste para o nó selecionado e retorna os caminhos gerados.
    /// </summary>
    public async Task<IReadOnlyCollection<string>> GenerateForNodeAsync(ExplorerNode node)
    {
        if (!TryBeginOperation("Gerando classes de teste..."))
        {
            return [];
        }

        try
        {
            var token = currentOperationCts?.Token ?? CancellationToken.None;
            var connection = ResolveConnection(node);
            if (connection is null)
            {
                return [];
            }

            if (!objectsByConnection.TryGetValue(connection.Id, out var objects))
            {
                SetStatusMessage("Nenhum objeto carregado para gerar. Use Atualizar objetos primeiro.");
                return [];
            }

            var selectedObjects = ResolveSelectedObjects(node, objects).ToArray();
            if (selectedObjects.Length == 0)
            {
                SetStatusMessage("Nenhum objeto selecionado para geração.");
                return [];
            }

            var mapping = mappings.FirstOrDefault(m => m.ConnectionId == connection.Id)
                ?? new ConnectionMappingConfiguration(connection.Id, CreateDefaultMappings("Generated", "{NamePascal}{Type}Factory.cs"));

            var generator = new ClassGenerator();
            var request = new GenerationRequest(connection, selectedObjects);
            var generated = await generator.GenerateAsync(request, mapping, o => StructuredClassContentFactory.Build(o, "Generated", connection.DatabaseType), token);
            SetStatusMessage($"Geração de classes de teste concluída. Arquivos gerados: {generated.Count}.");
            return generated;
        }
        finally
        {
            EndOperation();
        }
    }


    /// <summary>
    /// Generates model classes for the selected node using template settings.
    /// Gera classes de modelo para o nó selecionado usando as configurações de template.
    /// </summary>
    public Task<IReadOnlyCollection<string>> GenerateModelClassesForNodeAsync(ExplorerNode node)
        => GenerateFromTemplateForNodeAsync(node, templateConfiguration.ModelTemplatePath, templateConfiguration.ModelOutputDirectory, "Model", "// Model for {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n");

    /// <summary>
    /// Generates repository classes for the selected node using template settings.
    /// Gera classes de repositório para o nó selecionado usando as configurações de template.
    /// </summary>
    public Task<IReadOnlyCollection<string>> GenerateRepositoryClassesForNodeAsync(ExplorerNode node)
        => GenerateFromTemplateForNodeAsync(node, templateConfiguration.RepositoryTemplatePath, templateConfiguration.RepositoryOutputDirectory, "Repository", "// Repository for {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n");

    private async Task<IReadOnlyCollection<string>> GenerateFromTemplateForNodeAsync(ExplorerNode node, string templatePath, string outputDirectory, string suffix, string fallbackTemplate)
    {
        if (!TryBeginOperation($"Gerando classes de {suffix.ToLowerInvariant()}..."))
        {
            return [];
        }

        try
        {
            var token = currentOperationCts?.Token ?? CancellationToken.None;
            var connection = ResolveConnection(node);
            if (connection is null || !objectsByConnection.TryGetValue(connection.Id, out var objects))
            {
                SetStatusMessage("Nenhum objeto carregado para gerar.");
                return [];
            }

            var selectedObjects = ResolveSelectedObjects(node, objects).ToArray();
            if (selectedObjects.Length == 0)
            {
                SetStatusMessage("Nenhum objeto selecionado para geração.");
                return [];
            }

            var template = fallbackTemplate;
            if (!string.IsNullOrWhiteSpace(templatePath))
            {
                var normalizedTemplatePath = NormalizePath(templatePath);
                if (!File.Exists(normalizedTemplatePath))
                {
                    SetStatusMessage($"Template não encontrado: {normalizedTemplatePath}");
                    return [];
                }

                template = await Task.Run(() => File.ReadAllText(normalizedTemplatePath), token);
            }

            var normalizedOutputDirectory = NormalizePath(outputDirectory);
            Directory.CreateDirectory(normalizedOutputDirectory);
            var generatedFiles = new List<string>(selectedObjects.Length);

            foreach (var dbObject in selectedObjects)
            {
                var className = $"{GenerationRuleSet.ToPascalCase(dbObject.Name)}{suffix}";
                var content = ReplaceIgnoreCase(template, "{{ClassName}}", className);
                content = ReplaceIgnoreCase(content, "{{ObjectName}}", dbObject.Name);
                content = ReplaceIgnoreCase(content, "{{Schema}}", dbObject.Schema);
                content = ReplaceIgnoreCase(content, "{{ObjectType}}", dbObject.Type.ToString());
                content = ReplaceIgnoreCase(content, "{{DatabaseType}}", connection.DatabaseType);
                content = ReplaceIgnoreCase(content, "{{DatabaseName}}", connection.DatabaseName);

                var filePath = Path.Combine(normalizedOutputDirectory, $"{className}.cs");
                await Task.Run(() => File.WriteAllText(filePath, content), token);
                generatedFiles.Add(filePath);
            }

            SetStatusMessage($"Classes de {suffix.ToLowerInvariant()} geradas: {selectedObjects.Length}.");
            return generatedFiles;
        }
        finally
        {
            EndOperation();
        }
    }

    /// <summary>
    /// Checks consistency between database metadata and local generated artifacts.
    /// Verifica a consistência entre metadados do banco e artefatos locais gerados.
    /// </summary>
    public async Task CheckConsistencyAsync(ExplorerNode node)
    {
        if (!TryBeginOperation("Checando consistência..."))
        {
            return;
        }

        try
        {
            var token = currentOperationCts?.Token ?? CancellationToken.None;
            var connection = ResolveConnection(node);
            if (connection is null)
            {
                return;
            }

            if (!objectsByConnection.TryGetValue(connection.Id, out var objects))
            {
                SetStatusMessage("Nenhum objeto carregado para checar consistência.");
                return;
            }

            var mapping = mappings.FirstOrDefault(m => m.ConnectionId == connection.Id);
            if (mapping is null)
            {
                SetStatusMessage("Mapeamento não encontrado para a conexão selecionada.");
                return;
            }

            var checker = new ObjectConsistencyChecker();
            var selectedObjects = ResolveSelectedObjects(node, objects).ToArray();
            var updates = new ConcurrentBag<(string Key, ObjectHealthResult Result)>();

            var tasks = selectedObjects.Select(async dbObject =>
            {
                token.ThrowIfCancellationRequested();

                if (!mapping.Mappings.TryGetValue(dbObject.Type, out var objectMapping))
                {
                    return;
                }

                var filePath = Path.Combine(objectMapping.OutputDirectory, ResolveFileName(objectMapping.FileNamePattern, connection, dbObject));
                var modelPath = Path.Combine(NormalizePath(templateConfiguration.ModelOutputDirectory), $"{GenerationRuleSet.ToPascalCase(dbObject.Name)}Model.cs");
                var repositoryPath = Path.Combine(NormalizePath(templateConfiguration.RepositoryOutputDirectory), $"{GenerationRuleSet.ToPascalCase(dbObject.Name)}Repository.cs");

                var hasModel = File.Exists(modelPath);
                var hasRepository = File.Exists(repositoryPath);
                if (!File.Exists(filePath) || !hasModel || !hasRepository)
                {
                    var status = !hasModel && !hasRepository
                        ? ObjectHealthStatus.MissingLocalArtifacts
                        : ObjectHealthStatus.DifferentFromDatabase;
                    updates.Add((BuildObjectKey(connection.Id, dbObject), new ObjectHealthResult(dbObject, filePath, status, "Arquivos gerados ausentes (classe/model/repositório).")));
                    return;
                }

                var snapshot = await GeneratedClassSnapshotReader.ReadAsync(filePath, dbObject, token);
                var result = await checker.CheckAsync(connection, snapshot, metadataProvider, token);
                updates.Add((BuildObjectKey(connection.Id, dbObject), result));
            });

            await Task.WhenAll(tasks);

            foreach (var update in updates)
            {
                healthByObject[update.Key] = update.Result;
            }

            RefreshTree();
            SetStatusMessage($"Checagem de consistência finalizada para {updates.Count} objeto(s).");
        }
        finally
        {
            EndOperation();
        }
    }

    private static IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> CreateDefaultMappings(string outputDirectory, string fileNamePattern)
    {
        return ((DatabaseObjectType[])Enum.GetValues(typeof(DatabaseObjectType)))
            .ToDictionary(
                t => t,
                t => new ObjectTypeMapping(t, outputDirectory, fileNamePattern));
    }

    private void LoadState()
    {
        try
        {
            var loaded = statePersistenceService.Load(stateFilePath);
            if (loaded is null)
            {
                return;
            }

            foreach (var connection in loaded.Connections)
            {
                var decrypted = ConnectionStringProtector.Unprotect(connection.ConnectionString);
                connections.Add(new ConnectionDefinition(connection.Id, connection.DatabaseType, connection.DatabaseName, decrypted, connection.DisplayName));
            }

            foreach (var mapping in loaded.Mappings)
            {
                mappings.Add(mapping);
            }

            templateConfiguration = loaded.TemplateConfiguration ?? TemplateConfiguration.Default;
        }
        catch (Exception ex)
        {
            ExtensionLogger.Log($"LoadState error: {ex}");
            SetStatusMessage("Estado local inválido. Iniciando com configuração vazia.");
        }
    }

    private void SaveState()
    {
        var safeConnections = connections
            .Select(c => new ConnectionDefinition(c.Id, c.DatabaseType, c.DatabaseName, ConnectionStringProtector.Protect(c.ConnectionString), c.DisplayName))
            .ToArray();

        var state = new ExtensionState(safeConnections, [.. mappings], templateConfiguration);
        statePersistenceService.Save(state, stateFilePath);
    }

    private void RefreshTree()
    {
        var expandedNodeKeys = CaptureExpandedNodeKeys();
        Nodes.Clear();

        var byType = connections
            .OrderBy(c => c.DatabaseType, StringComparer.OrdinalIgnoreCase)
            .GroupBy(c => c.DatabaseType, StringComparer.OrdinalIgnoreCase);

        foreach (var typeGroup in byType)
        {
            var typeNode = new ExplorerNode(typeGroup.Key, ExplorerNodeKind.DatabaseType);
            foreach (var connection in typeGroup.OrderBy(c => c.DatabaseName, StringComparer.OrdinalIgnoreCase))
            {
                var connectionNode = new ExplorerNode(connection.FriendlyName, ExplorerNodeKind.Connection)
                {
                    ConnectionId = connection.Id
                };

                var objects = objectsByConnection.TryGetValue(connection.Id, out var loadedObjects)
                    ? loadedObjects
                    : [];

                var schemaGroups = objects
                    .GroupBy(o => string.IsNullOrWhiteSpace(o.Schema) ? connection.DatabaseName : o.Schema, StringComparer.OrdinalIgnoreCase)
                    .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase);

                foreach (var schemaGroup in schemaGroups)
                {
                    var schemaNode = new ExplorerNode(schemaGroup.Key, ExplorerNodeKind.Schema)
                    {
                        ConnectionId = connection.Id
                    };

                    foreach (DatabaseObjectType objectType in Enum.GetValues(typeof(DatabaseObjectType)))
                    {
                        var filter = GetObjectTypeFilter(connection.Id, objectType);
                        var hasFilter = !string.IsNullOrWhiteSpace(filter.Text);
                        var objectTypeNode = new ExplorerNode(
                            BuildObjectTypeLabel(objectType, hasFilter),
                            ExplorerNodeKind.ObjectType)
                        {
                            ConnectionId = connection.Id,
                            ObjectType = objectType
                        };

                        var typedObjects = schemaGroup
                            .Where(o => o.Type == objectType);

                        if (hasFilter)
                        {
                            typedObjects = objectFilterService.Filter(typedObjects, filter.Text, filter.Mode);
                        }

                        typedObjects = typedObjects
                            .OrderBy(o => o.Name, StringComparer.OrdinalIgnoreCase);

                        foreach (var dbObject in typedObjects)
                        {
                            var key = BuildObjectKey(connection.Id, dbObject);
                            ObjectHealthStatus? status = healthByObject.TryGetValue(key, out var health) ? health.Status : (ObjectHealthStatus?)null;
                            var objectNode = new ExplorerNode(dbObject.Name, ExplorerNodeKind.Object)
                            {
                                ConnectionId = connection.Id,
                                ObjectType = dbObject.Type,
                                DatabaseObject = dbObject,
                                HealthStatus = status
                            };

                            if (dbObject.Type == DatabaseObjectType.Table)
                            {
                                AddTableDetailsNodes(objectNode, dbObject);
                            }

                            objectTypeNode.Children.Add(objectNode);
                        }

                        if (objectTypeNode.Children.Count > 0)
                        {
                            schemaNode.Children.Add(objectTypeNode);
                        }
                    }

                    connectionNode.Children.Add(schemaNode);
                }

                typeNode.Children.Add(connectionNode);
            }

            Nodes.Add(typeNode);
        }

        RestoreExpandedNodeState(expandedNodeKeys);
        OnPropertyChanged(nameof(Nodes));
    }

    private HashSet<string> CaptureExpandedNodeKeys()
    {
        var expandedKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var node in EnumerateNodes(Nodes))
        {
            if (node.IsExpanded)
            {
                expandedKeys.Add(BuildNodeKey(node));
            }
        }

        return expandedKeys;
    }

    private void RestoreExpandedNodeState(HashSet<string> expandedNodeKeys)
    {
        foreach (var node in EnumerateNodes(Nodes))
        {
            node.IsExpanded = expandedNodeKeys.Contains(BuildNodeKey(node));
        }
    }

    private static IEnumerable<ExplorerNode> EnumerateNodes(IEnumerable<ExplorerNode> roots)
    {
        foreach (var node in roots)
        {
            yield return node;
            foreach (var child in EnumerateNodes(node.Children))
            {
                yield return child;
            }
        }
    }

    private (string Text, FilterMode Mode) GetObjectTypeFilter(string connectionId, DatabaseObjectType objectType)
    {
        var key = BuildObjectTypeFilterKey(connectionId, objectType);
        return objectTypeFilters.TryGetValue(key, out var filter)
            ? filter
            : (string.Empty, FilterMode.Like);
    }

    private static string BuildObjectTypeFilterKey(string connectionId, DatabaseObjectType objectType)
        => $"{connectionId}|{objectType}";

    private static string BuildObjectTypeLabel(DatabaseObjectType objectType, bool hasFilter)
    {
        var baseLabel = objectType switch
        {
            DatabaseObjectType.Table => "Tables",
            DatabaseObjectType.View => "Views",
            DatabaseObjectType.Procedure => "Procedures",
            _ => objectType.ToString()
        };

        return hasFilter ? $"{baseLabel} 🔎" : baseLabel;
    }

    private static string BuildNodeKey(ExplorerNode node)
        => node.Kind switch
        {
            ExplorerNodeKind.DatabaseType => $"dbtype|{node.Label}",
            ExplorerNodeKind.Connection => $"conn|{node.ConnectionId}",
            ExplorerNodeKind.Schema => $"schema|{node.ConnectionId}|{node.Label}",
            ExplorerNodeKind.ObjectType => $"otype|{node.ConnectionId}|{node.ObjectType}",
            ExplorerNodeKind.Object when node.DatabaseObject is not null =>
                $"obj|{node.ConnectionId}|{node.DatabaseObject.Type}|{node.DatabaseObject.Schema}|{node.DatabaseObject.Name}",
            ExplorerNodeKind.TableDetailGroup => $"detailgroup|{node.ConnectionId}|{node.TableDetailKind}|{node.Label}",
            ExplorerNodeKind.TableDetailItem => $"detailitem|{node.ConnectionId}|{node.TableDetailKind}|{node.Label}",
            _ => $"other|{node.Label}"
        };

    private async Task<IReadOnlyCollection<DatabaseObjectReference>> EnrichTableObjectsAsync(
        ConnectionDefinition connection,
        IReadOnlyCollection<DatabaseObjectReference> objects,
        CancellationToken token)
    {
        var tableObjects = objects.Where(o => o.Type == DatabaseObjectType.Table).ToArray();
        if (tableObjects.Length == 0)
        {
            return objects;
        }

        var detailedMap = new Dictionary<string, DatabaseObjectReference>(StringComparer.OrdinalIgnoreCase);
        foreach (var table in tableObjects)
        {
            var detailed = await metadataProvider.GetObjectAsync(connection, table, token);
            if (detailed is not null)
            {
                detailedMap[BuildObjectKey(connection.Id, table)] = detailed;
            }
        }

        if (detailedMap.Count == 0)
        {
            return objects;
        }

        return objects
            .Select(o => detailedMap.TryGetValue(BuildObjectKey(connection.Id, o), out var detailed) ? detailed : o)
            .ToArray();
    }

    private async Task EnsureTableDetailsLoadedAsync(ConnectionDefinition connection, ExplorerNode selectedNode, CancellationToken token)
    {
        if (selectedNode.DatabaseObject is null)
        {
            return;
        }

        var detailed = await metadataProvider.GetObjectAsync(connection, selectedNode.DatabaseObject, token);
        if (detailed is null || !objectsByConnection.TryGetValue(connection.Id, out var existing))
        {
            return;
        }

        objectsByConnection[connection.Id] = existing
            .Select(o =>
                o.Type == detailed.Type
                && string.Equals(o.Schema, detailed.Schema, StringComparison.OrdinalIgnoreCase)
                && string.Equals(o.Name, detailed.Name, StringComparison.OrdinalIgnoreCase)
                    ? detailed
                    : o)
            .ToArray();
    }

    private static void AddTableDetailsNodes(ExplorerNode tableNode, DatabaseObjectReference dbObject)
    {
        var columns = ParseColumns(dbObject);
        var indexes = ParseIndexes(dbObject);
        var foreignKeys = ParseForeignKeys(dbObject);
        var triggers = ParseTriggers(dbObject);

        var pkColumns = ParsePrimaryKey(dbObject);
        var fkColumns = new HashSet<string>(foreignKeys.Select(f => f.Column), StringComparer.OrdinalIgnoreCase);

        var columnsNode = new ExplorerNode("Colunas", ExplorerNodeKind.TableDetailGroup)
        {
            ConnectionId = tableNode.ConnectionId,
            TableDetailKind = "Columns"
        };

        foreach (var column in columns)
        {
            var icon = pkColumns.Contains(column.Name)
                ? "🔑"
                : fkColumns.Contains(column.Name)
                    ? "🔗"
                    : "•";

            columnsNode.Children.Add(new ExplorerNode($"{icon} {column.Name} ({column.DataType})", ExplorerNodeKind.TableDetailItem)
            {
                ConnectionId = tableNode.ConnectionId,
                TableDetailKind = "Column"
            });
        }

        var indexesNode = new ExplorerNode("Índices", ExplorerNodeKind.TableDetailGroup)
        {
            ConnectionId = tableNode.ConnectionId,
            TableDetailKind = "Indexes"
        };

        foreach (var index in indexes)
        {
            indexesNode.Children.Add(new ExplorerNode(index, ExplorerNodeKind.TableDetailItem)
            {
                ConnectionId = tableNode.ConnectionId,
                TableDetailKind = "Index"
            });
        }

        var foreignKeysNode = new ExplorerNode("Chave estrangeira", ExplorerNodeKind.TableDetailGroup)
        {
            ConnectionId = tableNode.ConnectionId,
            TableDetailKind = "ForeignKeys"
        };

        foreach (var fk in foreignKeys)
        {
            foreignKeysNode.Children.Add(new ExplorerNode($"{fk.Column} → {fk.RefTable}.{fk.RefColumn}", ExplorerNodeKind.TableDetailItem)
            {
                ConnectionId = tableNode.ConnectionId,
                TableDetailKind = "ForeignKey"
            });
        }

        var triggersNode = new ExplorerNode("Triggers", ExplorerNodeKind.TableDetailGroup)
        {
            ConnectionId = tableNode.ConnectionId,
            TableDetailKind = "Triggers"
        };

        foreach (var trigger in triggers)
        {
            triggersNode.Children.Add(new ExplorerNode(trigger, ExplorerNodeKind.TableDetailItem)
            {
                ConnectionId = tableNode.ConnectionId,
                TableDetailKind = "Trigger"
            });
        }

        tableNode.Children.Add(columnsNode);
        tableNode.Children.Add(indexesNode);
        tableNode.Children.Add(foreignKeysNode);
        tableNode.Children.Add(triggersNode);
    }

    private static HashSet<string> ParsePrimaryKey(DatabaseObjectReference dbObject)
        => SplitEscaped(GetMetadata(dbObject, "PrimaryKey"), ',')
            .Select(Unescape)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

    private static IReadOnlyCollection<(string Name, string DataType)> ParseColumns(DatabaseObjectReference dbObject)
    {
        var text = GetMetadata(dbObject, "Columns");
        if (string.IsNullOrWhiteSpace(text))
        {
            return [];
        }

        var cols = new List<(string Name, string DataType)>();
        foreach (var row in SplitEscaped(text, ';'))
        {
            if (string.IsNullOrWhiteSpace(row))
            {
                continue;
            }

            var parts = SplitEscaped(row, '|').ToArray();
            if (parts.Length < 2)
            {
                continue;
            }

            var name = Unescape(parts[0]);
            var dataType = Unescape(parts[1]);
            if (!string.IsNullOrWhiteSpace(name))
            {
                cols.Add((name, dataType));
            }
        }

        return cols;
    }

    private static IReadOnlyCollection<string> ParseIndexes(DatabaseObjectReference dbObject)
    {
        var text = GetMetadata(dbObject, "Indexes");
        if (string.IsNullOrWhiteSpace(text))
        {
            return [];
        }

        var list = new List<string>();
        foreach (var row in SplitEscaped(text, ';'))
        {
            var parts = SplitEscaped(row, '|').ToArray();
            if (parts.Length < 3)
            {
                continue;
            }

            var name = Unescape(parts[0]);
            var columns = SplitEscaped(parts[2], ',').Select(Unescape).Where(x => !string.IsNullOrWhiteSpace(x));
            list.Add($"{name} ({string.Join(", ", columns)})");
        }

        return list;
    }

    private static IReadOnlyCollection<(string Column, string RefTable, string RefColumn)> ParseForeignKeys(DatabaseObjectReference dbObject)
    {
        var text = GetMetadata(dbObject, "ForeignKeys");
        if (string.IsNullOrWhiteSpace(text))
        {
            return [];
        }

        var list = new List<(string, string, string)>();
        foreach (var row in SplitEscaped(text, ';'))
        {
            var parts = SplitEscaped(row, '|').ToArray();
            if (parts.Length < 3)
            {
                continue;
            }

            list.Add((Unescape(parts[0]), Unescape(parts[1]), Unescape(parts[2])));
        }

        return list;
    }

    private static IReadOnlyCollection<string> ParseTriggers(DatabaseObjectReference dbObject)
    {
        var text = GetMetadata(dbObject, "Triggers");
        if (string.IsNullOrWhiteSpace(text))
        {
            return [];
        }

        return SplitEscaped(text, ';').Select(Unescape).Where(x => !string.IsNullOrWhiteSpace(x)).ToArray();
    }

    private static string GetMetadata(DatabaseObjectReference dbObject, string key)
        => dbObject.Properties is not null && dbObject.Properties.TryGetValue(key, out var value)
            ? value
            : string.Empty;

    private static IEnumerable<string> SplitEscaped(string text, char separator)
    {
        var current = new StringBuilder();
        var escape = false;

        foreach (var ch in text)
        {
            if (escape)
            {
                current.Append(ch);
                escape = false;
                continue;
            }

            if (ch == '\\')
            {
                escape = true;
                continue;
            }

            if (ch == separator)
            {
                yield return current.ToString();
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        yield return current.ToString();
    }

    private static string Unescape(string value)
        => value.Replace("\\|", "|")
            .Replace("\\;", ";")
            .Replace("\\,", ",")
            .Replace("\\\\", "\\");

    private bool TryBeginOperation(string message)
    {
        if (!operationLock.Wait(0))
        {
            SetStatusMessage("Já existe uma operação em andamento.");
            return false;
        }

        currentOperationCts?.Dispose();
        currentOperationCts = new CancellationTokenSource();
        IsBusy = true;
        OnPropertyChanged(nameof(IsBusy));
        SetStatusMessage(message);
        return true;
    }

    private void EndOperation()
    {
        currentOperationCts?.Dispose();
        currentOperationCts = null;

        if (IsBusy)
        {
            IsBusy = false;
            OnPropertyChanged(nameof(IsBusy));
        }

        operationLock.Release();
    }

    private static string ExtractDatabaseName(string connectionString, string fallback)
    {
        try
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            foreach (var key in new[] { "Database", "Initial Catalog" })
            {
                if (builder.TryGetValue(key, out var value))
                {
                    var candidate = Convert.ToString(value)?.Trim();
                    if (!string.IsNullOrWhiteSpace(candidate))
                    {
                        return candidate;
                    }
                }
            }
        }
        catch
        {
            // ignore parse errors and keep fallback
        }

        return fallback;
    }

    private ConnectionDefinition? ResolveConnection(ExplorerNode node)
    {
        if (node.ConnectionId is null)
        {
            return null;
        }

        return connections.FirstOrDefault(c => c.Id == node.ConnectionId);
    }

    private static IEnumerable<DatabaseObjectReference> ResolveSelectedObjects(ExplorerNode node, IReadOnlyCollection<DatabaseObjectReference> objects)
    {
        if (node.Kind == ExplorerNodeKind.Object && node.DatabaseObject is not null)
        {
            return [node.DatabaseObject];
        }

        if (node.Kind == ExplorerNodeKind.ObjectType && node.ObjectType is not null)
        {
            return objects.Where(o => o.Type == node.ObjectType.Value);
        }

        if (node.Kind == ExplorerNodeKind.Connection)
        {
            return objects;
        }

        return [];
    }

    private static IEnumerable<string> FindExistingFiles(
        ConnectionDefinition connection,
        ConnectionMappingConfiguration mapping,
        IReadOnlyCollection<DatabaseObjectReference> objects)
    {
        foreach (var obj in objects)
        {
            if (!mapping.Mappings.TryGetValue(obj.Type, out var objectMapping))
            {
                continue;
            }

            var path = Path.Combine(objectMapping.OutputDirectory, ResolveFileName(objectMapping.FileNamePattern, connection, obj));
            if (File.Exists(path))
            {
                yield return path;
            }
        }
    }

    private static string ResolveFileName(string fileNamePattern, ConnectionDefinition connection, DatabaseObjectReference dbObject)
    {
        var safePattern = string.IsNullOrWhiteSpace(fileNamePattern)
            ? "{NamePascal}{Type}Factory.cs"
            : fileNamePattern;

        var namePascal = GenerationRuleSet.ToPascalCase(dbObject.Name);
        var typeName = dbObject.Type.ToString();

        var resolved = ReplaceIgnoreCase(safePattern, "{NamePascal}", namePascal);
        resolved = ReplaceIgnoreCase(resolved, "{Name}", dbObject.Name);
        resolved = ReplaceIgnoreCase(resolved, "{Type}", typeName);
        resolved = ReplaceIgnoreCase(resolved, "{Schema}", dbObject.Schema);
        resolved = ReplaceIgnoreCase(resolved, "{DatabaseType}", connection.DatabaseType);
        return ReplaceIgnoreCase(resolved, "{DatabaseName}", connection.DatabaseName);
    }


    private static string ReplaceIgnoreCase(string input, string oldValue, string newValue)
    {
        if (string.IsNullOrEmpty(input) || string.IsNullOrEmpty(oldValue))
        {
            return input;
        }

        var startIndex = 0;
        while (true)
        {
            var index = input.IndexOf(oldValue, startIndex, StringComparison.OrdinalIgnoreCase);
            if (index < 0)
            {
                return input;
            }

            input = input.Substring(0, index) + newValue + input.Substring(index + oldValue.Length);
            startIndex = index + newValue.Length;
        }
    }

    private static string BuildObjectKey(string connectionId, DatabaseObjectReference dbObject)
        => $"{connectionId}|{dbObject.Schema}|{dbObject.Name}|{dbObject.Type}";

    private static string NormalizePath(string path)
    {
        if (Path.IsPathRooted(path))
        {
            return Path.GetFullPath(path);
        }

        var baseDirectory = Directory.GetCurrentDirectory();
        return Path.GetFullPath(Path.Combine(baseDirectory, path));
    }

    private static string NormalizePathOrEmpty(string path)
        => string.IsNullOrWhiteSpace(path) ? string.Empty : NormalizePath(path);

    private void SetStatusMessage(string message)
    {
        StatusMessage = message;
        OnPropertyChanged(nameof(StatusMessage));
    }

    private void OnPropertyChanged(string propertyName)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
}

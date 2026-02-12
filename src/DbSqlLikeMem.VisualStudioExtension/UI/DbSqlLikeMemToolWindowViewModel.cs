using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.Core.Persistence;
using DbSqlLikeMem.VisualStudioExtension.Core.Validation;
using DbSqlLikeMem.VisualStudioExtension.Services;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public sealed class DbSqlLikeMemToolWindowViewModel : INotifyPropertyChanged
{
    private readonly StatePersistenceService statePersistenceService = new();
    private readonly SqlDatabaseMetadataProvider metadataProvider = new(new AdoNetSqlQueryExecutor());
    private readonly SemaphoreSlim operationLock = new(1, 1);
    private readonly string stateFilePath;

    private CancellationTokenSource? currentOperationCts;

    private readonly ObservableCollection<ConnectionDefinition> connections = new();
    private readonly ObservableCollection<ConnectionMappingConfiguration> mappings = new();
    private readonly Dictionary<string, IReadOnlyCollection<DatabaseObjectReference>> objectsByConnection = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ObjectHealthResult> healthByObject = new(StringComparer.OrdinalIgnoreCase);
    private TemplateConfiguration templateConfiguration = TemplateConfiguration.Default;

    public DbSqlLikeMemToolWindowViewModel()
    {
        stateFilePath = statePersistenceService.GetDefaultStatePath();
        LoadState();
        RefreshTree();
    }

    public ObservableCollection<ExplorerNode> Nodes { get; } = new();

    public string StatusMessage { get; private set; } = "Pronto.";

    public bool IsBusy { get; private set; }

    public event PropertyChangedEventHandler? PropertyChanged;

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

    public void AddConnection(string name, string databaseType, string connectionString)
    {
        var connection = new ConnectionDefinition(Guid.NewGuid().ToString("N"), databaseType, name, connectionString, name);
        connections.Add(connection);

        if (!mappings.Any(m => m.ConnectionId == connection.Id))
        {
            mappings.Add(new ConnectionMappingConfiguration(connection.Id, CreateDefaultMappings("Generated", "{NamePascal}{Type}Factory.cs")));
        }

        SaveState();
        RefreshTree();
    }

    public ConnectionDefinition? GetConnection(string connectionId)
        => connections.FirstOrDefault(c => c.Id == connectionId);

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
        connections[index] = new ConnectionDefinition(old.Id, databaseType, name, connectionString, name);
        objectsByConnection.Remove(old.Id);
        healthByObject.Clear();
        SaveState();
        RefreshTree();
    }

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

    public (string FileNamePattern, string OutputDirectory) GetMappingDefaults()
    {
        var firstMapping = mappings.FirstOrDefault()?.Mappings.Values.FirstOrDefault();
        return (firstMapping?.FileNamePattern ?? "{NamePascal}{Type}Factory.cs", firstMapping?.OutputDirectory ?? "Generated");
    }

    public TemplateConfiguration GetTemplateConfiguration() => templateConfiguration;

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


    public void CancelCurrentOperation()
    {
        currentOperationCts?.Cancel();
        SetStatusMessage("Cancelamento solicitado.");
    }

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
                    return (connection.Id, objects, error: (string?)null);
                }
                catch (Exception ex)
                {
                    ExtensionLogger.Log($"RefreshObjectsAsync error [{connection.DatabaseName}]: {ex}");
                    return (connection.Id, objects: (IReadOnlyCollection<DatabaseObjectReference>)Array.Empty<DatabaseObjectReference>(), error: $"{connection.DatabaseName}: {ex.Message}");
                }
            });

            var results = await Task.WhenAll(tasks);
            foreach (var result in results)
            {
                if (!string.IsNullOrWhiteSpace(result.error))
                {
                    failures.Add(result.error);
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

    public IReadOnlyCollection<string> PreviewConflictsForNode(ExplorerNode node)
    {
        var connection = ResolveConnection(node);
        if (connection is null || !objectsByConnection.TryGetValue(connection.Id, out var objects))
        {
            return Array.Empty<string>();
        }

        var selectedObjects = ResolveSelectedObjects(node, objects).ToArray();
        if (selectedObjects.Length == 0)
        {
            return Array.Empty<string>();
        }

        var mapping = mappings.FirstOrDefault(m => m.ConnectionId == connection.Id)
            ?? new ConnectionMappingConfiguration(connection.Id, CreateDefaultMappings("Generated", "{NamePascal}{Type}Factory.cs"));

        return FindExistingFiles(connection, mapping, selectedObjects).ToArray();
    }

    public async Task<IReadOnlyCollection<string>> GenerateForNodeAsync(ExplorerNode node)
    {
        if (!TryBeginOperation("Gerando classes de teste..."))
        {
            return Array.Empty<string>();
        }

        try
        {
            var token = currentOperationCts?.Token ?? CancellationToken.None;
            var connection = ResolveConnection(node);
            if (connection is null)
            {
                return Array.Empty<string>();
            }

            if (!objectsByConnection.TryGetValue(connection.Id, out var objects))
            {
                SetStatusMessage("Nenhum objeto carregado para gerar. Use Atualizar objetos primeiro.");
                return Array.Empty<string>();
            }

            var selectedObjects = ResolveSelectedObjects(node, objects).ToArray();
            if (selectedObjects.Length == 0)
            {
                SetStatusMessage("Nenhum objeto selecionado para geração.");
                return Array.Empty<string>();
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


    public Task<IReadOnlyCollection<string>> GenerateModelClassesForNodeAsync(ExplorerNode node)
        => GenerateFromTemplateForNodeAsync(node, templateConfiguration.ModelTemplatePath, templateConfiguration.ModelOutputDirectory, "Model", "// Model for {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n");

    public Task<IReadOnlyCollection<string>> GenerateRepositoryClassesForNodeAsync(ExplorerNode node)
        => GenerateFromTemplateForNodeAsync(node, templateConfiguration.RepositoryTemplatePath, templateConfiguration.RepositoryOutputDirectory, "Repository", "// Repository for {{Schema}}.{{ObjectName}}\npublic class {{ClassName}}\n{\n}\n");

    private async Task<IReadOnlyCollection<string>> GenerateFromTemplateForNodeAsync(ExplorerNode node, string templatePath, string outputDirectory, string suffix, string fallbackTemplate)
    {
        if (!TryBeginOperation($"Gerando classes de {suffix.ToLowerInvariant()}..."))
        {
            return Array.Empty<string>();
        }

        try
        {
            var connection = ResolveConnection(node);
            if (connection is null || !objectsByConnection.TryGetValue(connection.Id, out var objects))
            {
                SetStatusMessage("Nenhum objeto carregado para gerar.");
                return Array.Empty<string>();
            }

            var selectedObjects = ResolveSelectedObjects(node, objects).ToArray();
            if (selectedObjects.Length == 0)
            {
                SetStatusMessage("Nenhum objeto selecionado para geração.");
                return Array.Empty<string>();
            }

            var template = fallbackTemplate;
            if (!string.IsNullOrWhiteSpace(templatePath))
            {
                var normalizedTemplatePath = NormalizePath(templatePath);
                if (!File.Exists(normalizedTemplatePath))
                {
                    SetStatusMessage($"Template não encontrado: {normalizedTemplatePath}");
                    return Array.Empty<string>();
                }

                template = await File.ReadAllTextAsync(normalizedTemplatePath);
            }

            var normalizedOutputDirectory = NormalizePath(outputDirectory);
            Directory.CreateDirectory(normalizedOutputDirectory);
            var generatedFiles = new List<string>(selectedObjects.Length);

            foreach (var dbObject in selectedObjects)
            {
                var className = $"{GenerationRuleSet.ToPascalCase(dbObject.Name)}{suffix}";
                var content = template
                    .Replace("{{ClassName}}", className, StringComparison.OrdinalIgnoreCase)
                    .Replace("{{ObjectName}}", dbObject.Name, StringComparison.OrdinalIgnoreCase)
                    .Replace("{{Schema}}", dbObject.Schema, StringComparison.OrdinalIgnoreCase)
                    .Replace("{{ObjectType}}", dbObject.Type.ToString(), StringComparison.OrdinalIgnoreCase)
                    .Replace("{{DatabaseType}}", connection.DatabaseType, StringComparison.OrdinalIgnoreCase)
                    .Replace("{{DatabaseName}}", connection.DatabaseName, StringComparison.OrdinalIgnoreCase);

                var filePath = Path.Combine(normalizedOutputDirectory, $"{className}.cs");
                await File.WriteAllTextAsync(filePath, content);
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
        return Enum.GetValues<DatabaseObjectType>()
            .ToDictionary(
                t => t,
                t => new ObjectTypeMapping(t, outputDirectory, fileNamePattern));
    }

    private void LoadState()
    {
        try
        {
            var loaded = statePersistenceService.LoadAsync(stateFilePath).GetAwaiter().GetResult();
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

        var state = new ExtensionState(safeConnections, mappings.ToArray(), templateConfiguration);
        statePersistenceService.SaveAsync(state, stateFilePath).GetAwaiter().GetResult();
    }

    private void RefreshTree()
    {
        Nodes.Clear();

        var byType = connections
            .OrderBy(c => c.DatabaseType, StringComparer.OrdinalIgnoreCase)
            .GroupBy(c => c.DatabaseType, StringComparer.OrdinalIgnoreCase);

        foreach (var typeGroup in byType)
        {
            var typeNode = new ExplorerNode { Label = typeGroup.Key, Kind = ExplorerNodeKind.DatabaseType };
            foreach (var connection in typeGroup.OrderBy(c => c.DatabaseName, StringComparer.OrdinalIgnoreCase))
            {
                var connectionNode = new ExplorerNode
                {
                    Label = connection.DatabaseName,
                    Kind = ExplorerNodeKind.Connection,
                    ConnectionId = connection.Id
                };

                var objects = objectsByConnection.TryGetValue(connection.Id, out var loadedObjects)
                    ? loadedObjects
                    : Array.Empty<DatabaseObjectReference>();

                foreach (var objectType in Enum.GetValues<DatabaseObjectType>())
                {
                    var objectTypeNode = new ExplorerNode
                    {
                        Label = objectType switch
                        {
                            DatabaseObjectType.Table => "Tables",
                            DatabaseObjectType.View => "Views",
                            DatabaseObjectType.Procedure => "Procedures",
                            _ => objectType.ToString()
                        },
                        Kind = ExplorerNodeKind.ObjectType,
                        ConnectionId = connection.Id,
                        ObjectType = objectType
                    };

                    var typedObjects = objects
                        .Where(o => o.Type == objectType)
                        .OrderBy(o => o.Schema, StringComparer.OrdinalIgnoreCase)
                        .ThenBy(o => o.Name, StringComparer.OrdinalIgnoreCase);

                    foreach (var dbObject in typedObjects)
                    {
                        var key = BuildObjectKey(connection.Id, dbObject);
                        var status = healthByObject.TryGetValue(key, out var health) ? health.Status : null;
                        objectTypeNode.Children.Add(new ExplorerNode
                        {
                            Label = string.IsNullOrWhiteSpace(dbObject.Schema) ? dbObject.Name : $"{dbObject.Schema}.{dbObject.Name}",
                            Kind = ExplorerNodeKind.Object,
                            ConnectionId = connection.Id,
                            ObjectType = dbObject.Type,
                            DatabaseObject = dbObject,
                            HealthStatus = status
                        });
                    }

                    connectionNode.Children.Add(objectTypeNode);
                }

                typeNode.Children.Add(connectionNode);
            }

            Nodes.Add(typeNode);
        }

        OnPropertyChanged(nameof(Nodes));
    }

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
            return new[] { node.DatabaseObject };
        }

        if (node.Kind == ExplorerNodeKind.ObjectType && node.ObjectType is not null)
        {
            return objects.Where(o => o.Type == node.ObjectType.Value);
        }

        if (node.Kind == ExplorerNodeKind.Connection)
        {
            return objects;
        }

        return Array.Empty<DatabaseObjectReference>();
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

        return safePattern
            .Replace("{NamePascal}", namePascal, StringComparison.OrdinalIgnoreCase)
            .Replace("{Name}", dbObject.Name, StringComparison.OrdinalIgnoreCase)
            .Replace("{Type}", typeName, StringComparison.OrdinalIgnoreCase)
            .Replace("{Schema}", dbObject.Schema, StringComparison.OrdinalIgnoreCase)
            .Replace("{DatabaseType}", connection.DatabaseType, StringComparison.OrdinalIgnoreCase)
            .Replace("{DatabaseName}", connection.DatabaseName, StringComparison.OrdinalIgnoreCase);
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

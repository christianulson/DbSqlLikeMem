using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.Core.Persistence;
using DbSqlLikeMem.VisualStudioExtension.Core.Services;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public sealed class DbSqlLikeMemToolWindowViewModel : INotifyPropertyChanged
{
    private readonly TreeViewBuilder treeBuilder = new();
    private readonly StatePersistenceService statePersistenceService = new();
    private readonly string stateFilePath;

    private readonly ObservableCollection<ConnectionDefinition> connections = new();
    private readonly ObservableCollection<ConnectionMappingConfiguration> mappings = new();

    public DbSqlLikeMemToolWindowViewModel()
    {
        stateFilePath = statePersistenceService.GetDefaultStatePath();
        LoadState();
    }

    public ObservableCollection<TreeNode> Nodes { get; } = new();

    public event PropertyChangedEventHandler? PropertyChanged;

    public void AddConnection(string name, string databaseType, string connectionString)
    {
        var connection = new ConnectionDefinition(Guid.NewGuid().ToString("N"), databaseType, name, connectionString, name);
        connections.Add(connection);

        if (!mappings.Any(m => m.ConnectionId == connection.Id))
        {
            mappings.Add(new ConnectionMappingConfiguration(connection.Id, CreateDefaultMappings("Generated", "{NamePascal}{Type}Factory.cs")));
        }

        RefreshTree();
        SaveState();
    }

    public void ApplyDefaultMapping(string fileNamePattern, string outputDirectory)
    {
        for (var i = 0; i < mappings.Count; i++)
        {
            var current = mappings[i];
            mappings[i] = new ConnectionMappingConfiguration(current.ConnectionId, CreateDefaultMappings(outputDirectory, fileNamePattern));
        }

        SaveState();
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
                connections.Add(connection);
            }

            foreach (var mapping in loaded.Mappings)
            {
                mappings.Add(mapping);
            }

            RefreshTree();
        }
        catch
        {
            // Ignora estado corrompido e inicia vazio.
        }
    }

    private void SaveState()
    {
        var state = new ExtensionState(connections.ToArray(), mappings.ToArray());
        statePersistenceService.SaveAsync(state, stateFilePath).GetAwaiter().GetResult();
    }

    private void RefreshTree()
    {
        Nodes.Clear();

        foreach (var connection in connections.OrderBy(c => c.DatabaseType).ThenBy(c => c.DatabaseName))
        {
            var root = treeBuilder.Build(connection, Array.Empty<DatabaseObjectReference>());
            Nodes.Add(root);
        }

        OnPropertyChanged(nameof(Nodes));
    }

    private void OnPropertyChanged(string propertyName)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
}

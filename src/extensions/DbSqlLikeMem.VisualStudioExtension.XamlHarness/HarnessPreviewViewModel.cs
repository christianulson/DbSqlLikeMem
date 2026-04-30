using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.Core.Services;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

internal sealed class HarnessPreviewViewModel : INotifyPropertyChanged
{
    private readonly SqlDatabaseMetadataProvider metadataProvider = new(new HarnessSqlQueryExecutor());
    private readonly TreeViewBuilder treeViewBuilder = new();
    private string statusMessage = "Pronto.";
    private string summaryText = "Nenhuma conexao carregada.";

    public ObservableCollection<TreeNode> Nodes { get; } = [];

    public string StatusMessage
    {
        get => statusMessage;
        private set
        {
            if (string.Equals(statusMessage, value, StringComparison.Ordinal))
            {
                return;
            }

            statusMessage = value;
            OnPropertyChanged();
        }
    }

    public string SummaryText
    {
        get => summaryText;
        private set
        {
            if (string.Equals(summaryText, value, StringComparison.Ordinal))
            {
                return;
            }

            summaryText = value;
            OnPropertyChanged();
        }
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    public async Task LoadHarnessConnectionsAsync(IReadOnlyCollection<ConnectionDefinition> connections)
    {
        if (connections is null)
        {
            throw new ArgumentNullException(nameof(connections));
        }

        Nodes.Clear();

        if (connections.Count == 0)
        {
            SummaryText = "Nenhuma conexao de benchmark foi encontrada.";
            StatusMessage = "Carregue as connection strings de benchmark para ver a árvore.";
            return;
        }

        SummaryText = $"{connections.Count} conexões carregadas.";
        StatusMessage = "Carregando objetos do banco de dados...";

        var totalObjects = 0;
        foreach (var connection in connections)
        {
            try
            {
                var objects = await metadataProvider.ListObjectsAsync(connection, CancellationToken.None).ConfigureAwait(true);
                totalObjects += objects.Count;

                var connectionRoot = new TreeNode($"{connection.FriendlyName} ({connection.DatabaseType})")
                {
                    ContextKey = "connection",
                    NodeGlyph = "🔌"
                };
                connectionRoot.AddChild(treeViewBuilder.Build(connection, objects));
                Nodes.Add(connectionRoot);
            }
            catch (Exception ex)
            {
                var failedRoot = new TreeNode($"{connection.FriendlyName} ({connection.DatabaseType})")
                {
                    ContextKey = "connection",
                    NodeGlyph = "⚠"
                };
                failedRoot.AddChild(new TreeNode($"Falha ao carregar: {ex.Message}")
                {
                    NodeGlyph = "⚠"
                });
                Nodes.Add(failedRoot);
            }
        }

        StatusMessage = $"Ambiente carregado com {connections.Count} conexões e {totalObjects} objetos.";
    }

    private void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
}

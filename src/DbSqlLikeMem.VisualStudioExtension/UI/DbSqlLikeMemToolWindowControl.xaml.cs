using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using DbSqlLikeMem.VisualStudioExtension.Services;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class DbSqlLikeMemToolWindowControl : UserControl
{
    private static readonly HashSet<ExplorerNodeKind> GenerationSupportedKinds =
    [
        ExplorerNodeKind.Connection,
        ExplorerNodeKind.ObjectType,
        ExplorerNodeKind.Object
    ];

    private readonly DbSqlLikeMemToolWindowViewModel viewModel;

    public DbSqlLikeMemToolWindowControl()
    {
        InitializeComponent();
        viewModel = new DbSqlLikeMemToolWindowViewModel();
        DataContext = viewModel;
    }

    private async void OnAddConnectionClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            var dialog = new ConnectionDialog { Owner = Window.GetWindow(this) };

            if (dialog.ShowDialog() != true)
            {
                return;
            }

            var test = await viewModel.TestConnectionAsync(dialog.DatabaseType, dialog.ConnectionString);
            if (!test.Success)
            {
                MessageBox.Show(this, test.Message, "Falha de conexão", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            viewModel.AddConnection(dialog.ConnectionName, dialog.DatabaseType, dialog.ConnectionString);
            await viewModel.RefreshObjectsAsync();
        });

    private async void OnEditConnectionClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.Kind != ExplorerNodeKind.Connection)
            {
                MessageBox.Show(this, "Selecione um nó de conexão para editar.", "Editar conexão", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var existing = selected.ConnectionId is null ? null : viewModel.GetConnection(selected.ConnectionId);
            var dialog = new ConnectionDialog
            {
                Owner = Window.GetWindow(this),
                ConnectionName = existing?.DatabaseName ?? selected.Label,
                DatabaseType = existing?.DatabaseType ?? "SqlServer",
                ConnectionString = existing?.ConnectionString ?? string.Empty
            };

            if (dialog.ShowDialog() != true)
            {
                return;
            }

            var test = await viewModel.TestConnectionAsync(dialog.DatabaseType, dialog.ConnectionString);
            if (!test.Success)
            {
                MessageBox.Show(this, test.Message, "Falha de conexão", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            viewModel.UpdateConnection(selected, dialog.ConnectionName, dialog.DatabaseType, dialog.ConnectionString);
            await viewModel.RefreshObjectsAsync();
        });

    private void OnRemoveConnectionClick(object sender, RoutedEventArgs e)
    {
        if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.Kind != ExplorerNodeKind.Connection)
        {
            MessageBox.Show(this, "Selecione um nó de conexão para remover.", "Remover conexão", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var confirm = MessageBox.Show(this, $"Remover conexão '{selected.Label}'?", "Confirmação", MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (confirm == MessageBoxResult.Yes)
        {
            viewModel.RemoveConnection(selected);
        }
    }

    private void OnConfigureMappingsClick(object sender, RoutedEventArgs e)
    {
        var dialog = new MappingDialog { Owner = Window.GetWindow(this) };

        if (dialog.ShowDialog() == true)
        {
            viewModel.ApplyDefaultMapping(dialog.FileNamePattern, dialog.OutputDirectory);
        }
    }

    private async void OnRefreshObjectsClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(() => viewModel.RefreshObjectsAsync());

    private void OnCancelOperationClick(object sender, RoutedEventArgs e)
        => viewModel.CancelCurrentOperation();

    private async void OnGenerateClassesClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || !GenerationSupportedKinds.Contains(selected.Kind))
            {
                MessageBox.Show(this, "Selecione conexão, tipo de objeto ou objeto para gerar classes.", "Gerar classes", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var conflicts = viewModel.PreviewConflictsForNode(selected);
            if (conflicts.Count > 0)
            {
                var preview = string.Join(Environment.NewLine, conflicts.Take(10));
                var message = $"{conflicts.Count} arquivo(s) já existem e serão sobrescritos:\n\n{preview}";
                var confirm = MessageBox.Show(this, message, "Pré-visualização de sobrescrita", MessageBoxButton.YesNo, MessageBoxImage.Warning);
                if (confirm != MessageBoxResult.Yes)
                {
                    return;
                }
            }

            await viewModel.GenerateForNodeAsync(selected);
        });

    private async void OnCheckConsistencyClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || !GenerationSupportedKinds.Contains(selected.Kind))
            {
                MessageBox.Show(this, "Selecione conexão, tipo de objeto ou objeto para checar consistência.", "Checar consistência", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            await viewModel.CheckConsistencyAsync(selected);
        });

    private async Task RunSafeAsync(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            ExtensionLogger.Log($"UI operation error: {ex}");
            MessageBox.Show(this, ex.Message, "Erro inesperado", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }
}

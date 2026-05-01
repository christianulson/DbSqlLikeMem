using System.IO;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.Core.Services;
using DbSqlLikeMem.VisualStudioExtension.Services;
using DbSqlLikeMem.VisualStudioExtension.UI;
using Microsoft.Win32;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

/// <summary>
/// EN: Hosts the harness control that mirrors the Visual Studio tool window UI.
/// PT-br: Hospeda o controle do harness que espelha a interface da janela de ferramentas do Visual Studio.
/// </summary>
public partial class DbSqlLikeMemToolWindowControl : UserControl
{
    private const string HarnessLoadEnvironmentPropertyKey = "DbSqlLikeMem.XamlHarness.LoadEnvironment";
    private const string HarnessStorageScopePropertyKey = "DbSqlLikeMem.XamlHarness.StorageScopeKey";
    private static readonly HashSet<ExplorerNodeKind> GenerationSupportedKinds =
    [
        ExplorerNodeKind.Connection,
        ExplorerNodeKind.Schema,
        ExplorerNodeKind.ObjectType,
        ExplorerNodeKind.Object
    ];
    private readonly DbSqlLikeMemToolWindowViewModel viewModel;

    /// <summary>
    /// EN: Initializes the harness control and loads either persisted state or a clean harness state.
    /// PT-br: Inicializa o controle do harness e carrega o estado persistido ou um estado limpo do harness.
    /// </summary>
    public DbSqlLikeMemToolWindowControl()
    {
        InitializeComponent();
        var loadPersistedState = !IsHarnessLoadEnvironmentEnabled();
        viewModel = new DbSqlLikeMemToolWindowViewModel(loadPersistedState, loadPersistedState ? ResolveStorageScopeKey() : null);
        DataContext = viewModel;
    }

    /// <summary>
    /// EN: Replaces the current explorer state with the benchmark connections created by the harness profile.
    /// PT-br: Substitui o estado atual do explorador pelas conexões de benchmark criadas pelo profile do harness.
    /// </summary>
    public Task LoadHarnessConnectionsAsync(IReadOnlyCollection<ConnectionDefinition> connections)
        => viewModel.LoadHarnessConnectionsAsync(connections);

    private static bool IsHarnessLoadEnvironmentEnabled()
        => Application.Current?.Properties[HarnessLoadEnvironmentPropertyKey] is true;

    private static string? ResolveStorageScopeKey()
    {
        if (Application.Current?.Properties[HarnessStorageScopePropertyKey] is string configuredScope
            && !string.IsNullOrWhiteSpace(configuredScope))
        {
            return configuredScope.Trim();
        }

        foreach (var root in EnumerateCandidateRoots())
        {
            var solutionScope = FindNearestSolutionScope(root);
            if (!string.IsNullOrWhiteSpace(solutionScope))
            {
                return solutionScope;
            }

            var gitScope = FindNearestGitScope(root);
            if (!string.IsNullOrWhiteSpace(gitScope))
            {
                return gitScope;
            }
        }

        return null;
    }

    private static IEnumerable<string> EnumerateCandidateRoots()
    {
        yield return Directory.GetCurrentDirectory();
        yield return AppContext.BaseDirectory;
    }

    private static string? FindNearestSolutionScope(string startDirectory)
    {
        var current = new DirectoryInfo(startDirectory);
        while (current is not null)
        {
            var solution = FindFirstMatch(current.FullName, "*.slnx");
            if (solution is not null)
            {
                return solution;
            }

            solution = FindFirstMatch(current.FullName, "*.sln");
            if (solution is not null)
            {
                return solution;
            }

            current = current.Parent;
        }

        return null;
    }

    private static string? FindNearestGitScope(string startDirectory)
    {
        var current = new DirectoryInfo(startDirectory);
        while (current is not null)
        {
            var gitMarkerPath = Path.Combine(current.FullName, ".git");
            if (Directory.Exists(gitMarkerPath) || File.Exists(gitMarkerPath))
            {
                return Path.GetFullPath(current.FullName);
            }

            current = current.Parent;
        }

        return null;
    }

    private static string? FindFirstMatch(string directory, string searchPattern)
    {
        try
        {
            foreach (var file in Directory.EnumerateFiles(directory, searchPattern, SearchOption.TopDirectoryOnly))
            {
                return Path.GetFullPath(file);
            }
        }
        catch (IOException ex)
        {
            ExtensionLogger.Log($"ResolveStorageScopeKey file search error: {ex}");
        }
        catch (UnauthorizedAccessException ex)
        {
            ExtensionLogger.Log($"ResolveStorageScopeKey file search access error: {ex}");
        }

        return null;
    }

    private void OnAddConnectionClick(object sender, RoutedEventArgs e)
        => _ = RunSafeAsync(async () =>
        {
            var dialog = new ConnectionDialog { Owner = Window.GetWindow(this) };

            if (dialog.ShowDialog() != true)
            {
                return;
            }

            var test = await viewModel.TestConnectionAsync(dialog.DatabaseType, dialog.ConnectionString);
            if (!test.Success)
            {
                MessageBox.Show(Window.GetWindow(this), test.Message, "Connection validation", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            viewModel.AddConnection(dialog.ConnectionName, dialog.DatabaseType, dialog.ConnectionString);
            await viewModel.RefreshObjectsAsync();
        });

    private void OnRefreshObjectsClick(object sender, RoutedEventArgs e)
        => _ = RunSafeAsync(viewModel.RefreshObjectsAsync);

    private void OnRefreshConnectionClick(object sender, RoutedEventArgs e)
        => _ = RunSafeAsync(async () =>
        {
            var selected = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);
            if (selected is null)
            {
                return;
            }

            await viewModel.EnsureConnectionObjectsLoadedAsync(selected);
        });

    private void OnImportSettingsClick(object sender, RoutedEventArgs e)
        => _ = RunSafeAsync(async () =>
        {
            var dialog = new OpenFileDialog
            {
                Title = "Import settings",
                Filter = "JSON files (*.json)|*.json|All files (*.*)|*.*",
                CheckFileExists = true,
                Multiselect = false
            };

            if (dialog.ShowDialog(Window.GetWindow(this)) != true)
            {
                return;
            }

            await viewModel.ImportStateAsync(dialog.FileName);
        });

    private void OnExportSettingsClick(object sender, RoutedEventArgs e)
        => _ = RunSafeAsync(async () =>
        {
            var dialog = new SaveFileDialog
            {
                Title = "Export settings",
                Filter = "JSON files (*.json)|*.json|All files (*.*)|*.*",
                AddExtension = true,
                DefaultExt = "json",
                FileName = "dbsqllikemem-settings.json",
                OverwritePrompt = true
            };

            if (dialog.ShowDialog(Window.GetWindow(this)) != true)
            {
                return;
            }

            await viewModel.ExportStateAsync(dialog.FileName);
        });

    private void OnCancelOperationClick(object sender, RoutedEventArgs e)
        => viewModel.CancelCurrentOperation();

    private void OnExplorerTreePreviewMouseRightButtonDown(object sender, MouseButtonEventArgs e)
    {
        var source = e.OriginalSource as DependencyObject;
        if (source is null)
        {
            return;
        }

        var treeViewItem = FindParent<TreeViewItem>(source);
        if (treeViewItem is not null)
        {
            treeViewItem.IsSelected = true;
            treeViewItem.Focus();
        }
    }

    private void OnExplorerTreeSelectedItemChanged(object sender, RoutedPropertyChangedEventArgs<object> e)
        => _ = RunSafeAsync(async () =>
        {
            if (e.NewValue is not ExplorerNode node)
            {
                return;
            }

            var effective = GetEffectiveSelectedNode(node);
            if (effective is null)
            {
                return;
            }

            await viewModel.EnsureConnectionObjectsLoadedAsync(effective);
        });

    private void OnExplorerTreeItemExpanded(object sender, RoutedEventArgs e)
    {
        if (sender is TreeViewItem item && item.DataContext is ExplorerNode node)
        {
            node.IsExpanded = true;
        }
    }

    private void OnExplorerTreeItemCollapsed(object sender, RoutedEventArgs e)
    {
        if (sender is TreeViewItem item && item.DataContext is ExplorerNode node)
        {
            node.IsExpanded = false;
        }
    }

    private void OnExplorerContextMenuOpened(object sender, RoutedEventArgs e)
    {
        var contextNode = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);

        var isConnectionNodeSelected = contextNode?.Kind == ExplorerNodeKind.Connection;
        var isSchemaNodeSelected = contextNode?.Kind == ExplorerNodeKind.Schema;
        var isObjectTypeNodeSelected = contextNode?.Kind == ExplorerNodeKind.ObjectType;
        var isTableNodeSelected = contextNode?.Kind == ExplorerNodeKind.Object && contextNode.ObjectType == DatabaseObjectType.Table;
        var isGenerationSupportedSelected = contextNode is not null && GenerationSupportedKinds.Contains(contextNode.Kind);
        var hasObjectTypeFilter = isObjectTypeNodeSelected
            && contextNode is not null
            && !string.IsNullOrWhiteSpace(viewModel.GetObjectTypeFilter(contextNode).FilterText);

        var visibility = ExplorerContextMenuStateCalculator.Calculate(
            new ExplorerContextMenuSelection(
                isConnectionNodeSelected,
                isSchemaNodeSelected,
                isObjectTypeNodeSelected,
                isTableNodeSelected,
                hasObjectTypeFilter,
                isGenerationSupportedSelected));

        EditConnectionMenuItem.Visibility = visibility.EditConnectionVisible ? Visibility.Visible : Visibility.Collapsed;
        RefreshConnectionMenuItem.Visibility = visibility.RefreshConnectionVisible ? Visibility.Visible : Visibility.Collapsed;
        CancelConnectionOperationMenuItem.Visibility = visibility.CancelConnectionOperationVisible ? Visibility.Visible : Visibility.Collapsed;
        RemoveConnectionMenuItem.Visibility = visibility.RemoveConnectionVisible ? Visibility.Visible : Visibility.Collapsed;
        ConnectionActionsSeparator.Visibility = visibility.ConnectionActionsSeparatorVisible ? Visibility.Visible : Visibility.Collapsed;
        ConfigureMappingsMenuItem.Visibility = visibility.ConfigureMappingsVisible ? Visibility.Visible : Visibility.Collapsed;
        ConfigureTemplatesMenuItem.Visibility = visibility.ConfigureTemplatesVisible ? Visibility.Visible : Visibility.Collapsed;
        ConfigureObjectTypeFilterMenuItem.Visibility = visibility.ConfigureObjectTypeFilterVisible ? Visibility.Visible : Visibility.Collapsed;
        ClearObjectTypeFilterMenuItem.Visibility = visibility.ClearObjectTypeFilterVisible ? Visibility.Visible : Visibility.Collapsed;
        GenerationActionsSeparator.Visibility = visibility.GenerationActionsSeparatorVisible ? Visibility.Visible : Visibility.Collapsed;
        GenerateTestClassesMenuItem.Visibility = visibility.GenerateTestClassesVisible ? Visibility.Visible : Visibility.Collapsed;

        if (sender is ContextMenu contextMenu && !visibility.HasVisibleAction)
        {
            contextMenu.IsOpen = false;
        }
    }

    private void OnEditConnectionClick(object sender, RoutedEventArgs e)
        => _ = RunSafeAsync(async () =>
        {
            var selected = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);
            if (selected is null || selected.Kind != ExplorerNodeKind.Connection || selected.ConnectionId is null)
            {
                return;
            }

            var existing = viewModel.GetConnection(selected.ConnectionId);
            var dialog = new ConnectionDialog
            {
                Owner = Window.GetWindow(this),
                ConnectionName = existing?.DatabaseName ?? selected.Label,
                DatabaseType = existing?.DatabaseType ?? DatabaseTypeCatalog.DefaultDatabaseType,
                ConnectionString = existing?.ConnectionString ?? string.Empty
            };

            if (dialog.ShowDialog() != true)
            {
                return;
            }

            var test = await viewModel.TestConnectionAsync(dialog.DatabaseType, dialog.ConnectionString);
            if (!test.Success)
            {
                MessageBox.Show(Window.GetWindow(this), test.Message, "Connection validation", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            viewModel.UpdateConnection(selected, dialog.ConnectionName, dialog.DatabaseType, dialog.ConnectionString);
            await viewModel.RefreshObjectsAsync();
        });

    private void OnRemoveConnectionClick(object sender, RoutedEventArgs e)
    {
        var selected = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);
        if (selected is null || selected.Kind != ExplorerNodeKind.Connection)
        {
            return;
        }

        var confirm = MessageBox.Show(
            Window.GetWindow(this),
            $"Remove connection '{selected.Label}'?",
            "Remove connection",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (confirm == MessageBoxResult.Yes)
        {
            viewModel.RemoveConnection(selected);
        }
    }

    private void OnConfigureMappingsClick(object sender, RoutedEventArgs e)
    {
        var selected = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);
        if (selected is null || selected.Kind != ExplorerNodeKind.ObjectType || selected.ConnectionId is null || selected.ObjectType is null)
        {
            return;
        }

        var (fileNamePattern, outputDirectory, @namespace) = viewModel.GetMappingDefaults(selected);
        var dialog = new MappingDialog(selected.ObjectType.Value, fileNamePattern, outputDirectory, @namespace)
        {
            Owner = Window.GetWindow(this)
        };

        if (dialog.ShowDialog() == true)
        {
            viewModel.ApplyMappingForObjectType(
                selected.ConnectionId,
                selected.ObjectType.Value,
                dialog.FileNamePattern,
                dialog.OutputDirectory,
                dialog.Namespace);
        }
    }

    private void OnConfigureTemplatesClick(object sender, RoutedEventArgs e)
    {
        var selected = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);
        if (selected is null || selected.Kind != ExplorerNodeKind.ObjectType)
        {
            return;
        }

        var dialog = new TemplateConfigurationDialog(viewModel.GetTemplateConfiguration())
        {
            Owner = Window.GetWindow(this)
        };

        if (dialog.ShowDialog() == true)
        {
            viewModel.ConfigureTemplates(
                dialog.ModelTemplatePath,
                dialog.RepositoryTemplatePath,
                dialog.ModelOutputDirectory,
                dialog.RepositoryOutputDirectory,
                dialog.ModelFileNamePattern,
                dialog.RepositoryFileNamePattern);
        }
    }

    private void OnConfigureObjectTypeFilterClick(object sender, RoutedEventArgs e)
    {
        var selected = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);
        if (selected is null || selected.Kind != ExplorerNodeKind.ObjectType)
        {
            return;
        }

        var current = viewModel.GetObjectTypeFilter(selected);
        var dialog = new ObjectTypeFilterDialog(current.FilterText, current.FilterMode)
        {
            Owner = Window.GetWindow(this)
        };

        if (dialog.ShowDialog() == true)
        {
            viewModel.SetObjectTypeFilter(selected, dialog.FilterText, dialog.FilterMode);
        }
    }

    private void OnClearObjectTypeFilterClick(object sender, RoutedEventArgs e)
    {
        var selected = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);
        if (selected is null || selected.Kind != ExplorerNodeKind.ObjectType)
        {
            return;
        }

        viewModel.ClearObjectTypeFilter(selected);
    }

    private void OnGenerateClassesClick(object sender, RoutedEventArgs e)
        => _ = RunSafeAsync(async () =>
        {
            var selected = GetEffectiveSelectedNode(ExplorerTree.SelectedItem as ExplorerNode);
            if (selected is null || !GenerationSupportedKinds.Contains(selected.Kind))
            {
                MessageBox.Show(
                    Window.GetWindow(this),
                    "Selecione uma conexao, schema, tipo de objeto ou objeto para gerar classes de teste.",
                    "Gerar classes de teste",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            var generatedFiles = await viewModel.GenerateForNodeAsync(selected);
            if (generatedFiles.Count == 0)
            {
                MessageBox.Show(
                    Window.GetWindow(this),
                    "Nenhum arquivo foi gerado.",
                    "Gerar classes de teste",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            MessageBox.Show(
                Window.GetWindow(this),
                $"{generatedFiles.Count} arquivo(s) gerado(s).",
                "Gerar classes de teste",
                MessageBoxButton.OK,
                MessageBoxImage.Information);
        });

    private async Task RunSafeAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            ExtensionLogger.Log($"Harness UI operation error: {ex}");
            MessageBox.Show(Window.GetWindow(this), ex.Message, "DbSqlLikeMem XAML harness", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private static ExplorerNode? GetEffectiveSelectedNode(ExplorerNode? node)
    {
        if (node?.Kind is ExplorerNodeKind.TableDetailGroup or ExplorerNodeKind.TableDetailItem)
        {
            return FindParentObjectNode(node);
        }

        return node;
    }

    private static ExplorerNode? FindParentObjectNode(ExplorerNode? detailNode)
    {
        var current = detailNode?.Parent;
        while (current is not null)
        {
            if (current.Kind == ExplorerNodeKind.Object)
            {
                return current;
            }

            current = current.Parent;
        }

        return null;
    }

    private static T? FindParent<T>(DependencyObject child) where T : DependencyObject
    {
        var current = child;
        while (current is not null)
        {
            if (current is T parent)
            {
                return parent;
            }

            current = VisualTreeHelper.GetParent(current);
        }

        return null;
    }
}

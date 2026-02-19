using System.IO;
using System.Data;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.Services;
using EnvDTE;
using DteProject = EnvDTE.Project;
using DteProjectItem = EnvDTE.ProjectItem;
using DteProjectItems = EnvDTE.ProjectItems;
using Microsoft.Win32;
using Microsoft.VisualStudio.Shell;

using DbSqlLikeMem.VisualStudioExtension.Properties;

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

    /// <summary>
    /// Initializes the DbSqlLikeMem tool window user control and its view model.
    /// Inicializa o controle da janela DbSqlLikeMem e seu view model.
    /// </summary>
    public DbSqlLikeMemToolWindowControl()
    {
        InitializeComponent();
        viewModel = new DbSqlLikeMemToolWindowViewModel();
        DataContext = viewModel;
    }

    private async void OnAddConnectionClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            var dialog = new ConnectionDialog { Owner = System.Windows.Window.GetWindow(this) };

            if (dialog.ShowDialog() != true)
            {
                return;
            }

            var test = await viewModel.TestConnectionAsync(dialog.DatabaseType, dialog.ConnectionString);
            if (!test.Success)
            {
                MessageBox.Show(System.Windows.Window.GetWindow(this), test.Message, Resources.ConnectionFailureTitle, MessageBoxButton.OK, MessageBoxImage.Warning);
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
                MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectConnectionToEdit, Resources.EditConnectionMenu, MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var existing = selected.ConnectionId is null ? null : viewModel.GetConnection(selected.ConnectionId);
            var dialog = new ConnectionDialog
            {
                Owner = System.Windows.Window.GetWindow(this),
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
                MessageBox.Show(System.Windows.Window.GetWindow(this), test.Message, Resources.ConnectionFailureTitle, MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            viewModel.UpdateConnection(selected, dialog.ConnectionName, dialog.DatabaseType, dialog.ConnectionString);
            await viewModel.RefreshObjectsAsync();
        });

    private void OnRemoveConnectionClick(object sender, RoutedEventArgs e)
    {
        if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.Kind != ExplorerNodeKind.Connection)
        {
            MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectConnectionToRemove, Resources.RemoveConnectionMenu, MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var confirm = MessageBox.Show(System.Windows.Window.GetWindow(this), string.Format(Resources.RemoveConnectionQuestion, selected.Label), Resources.ConfirmTitle, MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (confirm == MessageBoxResult.Yes)
        {
            viewModel.RemoveConnection(selected);
        }
    }

    private void OnExplorerContextMenuOpened(object sender, RoutedEventArgs e)
    {
        var selectedNode = ExplorerTree.SelectedItem as ExplorerNode;
        var isConnectionNodeSelected = selectedNode?.Kind == ExplorerNodeKind.Connection;
        var isObjectTypeNodeSelected = selectedNode?.Kind == ExplorerNodeKind.ObjectType;
        var isTableNodeSelected = selectedNode?.Kind == ExplorerNodeKind.Object && selectedNode.ObjectType == DatabaseObjectType.Table;
        var isGenerationSupportedSelected = selectedNode is not null && GenerationSupportedKinds.Contains(selectedNode.Kind);
        var canExtractScenario = isConnectionNodeSelected || isObjectTypeNodeSelected || isTableNodeSelected;
        var canClearObjectTypeFilter = isObjectTypeNodeSelected
            && selectedNode is not null
            && !string.IsNullOrWhiteSpace(viewModel.GetObjectTypeFilter(selectedNode).FilterText);

        EditConnectionMenuItem.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        RemoveConnectionMenuItem.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        RefreshConnectionMenuItem.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        CancelConnectionOperationMenuItem.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        ConnectionActionsSeparator.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;

        ConfigureMappingsMenuItem.Visibility = isObjectTypeNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        ConfigureTemplatesMenuItem.Visibility = isObjectTypeNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        ConfigureObjectTypeFilterMenuItem.Visibility = isObjectTypeNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        ClearObjectTypeFilterMenuItem.Visibility = canClearObjectTypeFilter ? Visibility.Visible : Visibility.Collapsed;

        GenerationActionsSeparator.Visibility = isGenerationSupportedSelected ? Visibility.Visible : Visibility.Collapsed;
        GenerateAllClassesMenuItem.Visibility = isGenerationSupportedSelected ? Visibility.Visible : Visibility.Collapsed;
        GenerateByTypeSeparator.Visibility = isGenerationSupportedSelected ? Visibility.Visible : Visibility.Collapsed;
        GenerateTestClassesMenuItem.Visibility = isGenerationSupportedSelected ? Visibility.Visible : Visibility.Collapsed;
        GenerateModelClassesMenuItem.Visibility = isGenerationSupportedSelected ? Visibility.Visible : Visibility.Collapsed;
        GenerateRepositoryClassesMenuItem.Visibility = isGenerationSupportedSelected ? Visibility.Visible : Visibility.Collapsed;
        CheckConsistencyMenuItem.Visibility = isGenerationSupportedSelected ? Visibility.Visible : Visibility.Collapsed;
        ExtractScenarioMenuItem.Visibility = canExtractScenario ? Visibility.Visible : Visibility.Collapsed;
    }


    private async void OnExplorerTreeSelectedItemChanged(object sender, RoutedPropertyChangedEventArgs<object> e)
        => await RunSafeAsync(async () =>
        {
            if (e.NewValue is not ExplorerNode node)
            {
                return;
            }

            await viewModel.EnsureConnectionObjectsLoadedAsync(node);
        });

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

    private void OnConfigureMappingsClick(object sender, RoutedEventArgs e)
    {
        if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.Kind != ExplorerNodeKind.ObjectType)
        {
            MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectObjectTypeToConfigureMappings, Resources.ConfigureMappingsMenu, MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var defaults = viewModel.GetMappingDefaults();
        var dialog = new MappingDialog(defaults.FileNamePattern, defaults.OutputDirectory) { Owner = System.Windows.Window.GetWindow(this) };

        if (dialog.ShowDialog() == true)
        {
            viewModel.ApplyDefaultMapping(dialog.FileNamePattern, dialog.OutputDirectory);
        }
    }


    private void OnConfigureTemplatesClick(object sender, RoutedEventArgs e)
    {
        if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.Kind != ExplorerNodeKind.ObjectType)
        {
            MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectObjectTypeToConfigureTemplates, Resources.ConfigureTemplatesMenu, MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var dialog = new TemplateConfigurationDialog(viewModel.GetTemplateConfiguration()) { Owner = System.Windows.Window.GetWindow(this) };
        if (dialog.ShowDialog() == true)
        {
            viewModel.ConfigureTemplates(dialog.ModelTemplatePath, dialog.RepositoryTemplatePath, dialog.ModelOutputDirectory, dialog.RepositoryOutputDirectory);
        }
    }

    private void OnConfigureObjectTypeFilterClick(object sender, RoutedEventArgs e)
    {
        if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.Kind != ExplorerNodeKind.ObjectType)
        {
            MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectObjectTypeToConfigureFilter, Resources.ObjectFilterDialogTitle, MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var current = viewModel.GetObjectTypeFilter(selected);
        var dialog = new ObjectTypeFilterDialog(current.FilterText, current.FilterMode)
        {
            Owner = System.Windows.Window.GetWindow(this)
        };

        if (dialog.ShowDialog() == true)
        {
            viewModel.SetObjectTypeFilter(selected, dialog.FilterText, dialog.FilterMode);
        }
    }

    private void OnClearObjectTypeFilterClick(object sender, RoutedEventArgs e)
    {
        if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.Kind != ExplorerNodeKind.ObjectType)
        {
            MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectObjectTypeToClearFilter, Resources.ObjectFilterDialogTitle, MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        viewModel.ClearObjectTypeFilter(selected);
    }

    private async void OnRefreshObjectsClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(() => viewModel.RefreshObjectsAsync());

    private async void OnImportSettingsClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            var dialog = new OpenFileDialog
            {
                Title = Resources.ImportSettingsDialogTitle,
                Filter = Resources.JsonFileFilter,
                CheckFileExists = true,
                Multiselect = false
            };

            if (dialog.ShowDialog(System.Windows.Window.GetWindow(this)) != true)
            {
                return;
            }

            await viewModel.ImportStateAsync(dialog.FileName);
        });

    private async void OnExportSettingsClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            var dialog = new SaveFileDialog
            {
                Title = Resources.ExportSettingsDialogTitle,
                Filter = Resources.JsonFileFilter,
                AddExtension = true,
                DefaultExt = "json",
                FileName = "dbsqllikemem-settings.json",
                OverwritePrompt = true
            };

            if (dialog.ShowDialog(System.Windows.Window.GetWindow(this)) != true)
            {
                return;
            }

            await viewModel.ExportStateAsync(dialog.FileName);
        });

    private void OnCancelOperationClick(object sender, RoutedEventArgs e)
        => viewModel.CancelCurrentOperation();


    private async void OnGenerateAllClassesClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || !GenerationSupportedKinds.Contains(selected.Kind))
            {
                MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectNodeToGenerateClasses, Resources.GenerateClassesTitle, MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var conflicts = viewModel.PreviewConflictsForNode(selected);
            if (conflicts.Count > 0)
            {
                var preview = string.Join(Environment.NewLine, conflicts.Take(10));
                var message = string.Format(Resources.OverwritePreviewMessage, conflicts.Count, Environment.NewLine, preview);
                var confirm = MessageBox.Show(System.Windows.Window.GetWindow(this), message, Resources.OverwritePreviewTitle, MessageBoxButton.YesNo, MessageBoxImage.Warning);
                if (confirm != MessageBoxResult.Yes)
                {
                    return;
                }
            }

            var generatedTestFiles = await viewModel.GenerateForNodeAsync(selected);
            var generatedModelFiles = await viewModel.GenerateModelClassesForNodeAsync(selected);
            var generatedRepositoryFiles = await viewModel.GenerateRepositoryClassesForNodeAsync(selected);

            await ThreadHelper.JoinableTaskFactory.SwitchToMainThreadAsync();
            AddFilesToActiveProject(generatedTestFiles.Concat(generatedModelFiles).Concat(generatedRepositoryFiles));
        });

    private async void OnGenerateClassesClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || !GenerationSupportedKinds.Contains(selected.Kind))
            {
                MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectNodeToGenerateTestClasses, Resources.GenerateTestClassesMenu, MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var conflicts = viewModel.PreviewConflictsForNode(selected);
            if (conflicts.Count > 0)
            {
                var preview = string.Join(Environment.NewLine, conflicts.Take(10));
                var message = string.Format(Resources.OverwritePreviewMessage, conflicts.Count, Environment.NewLine, preview);
                var confirm = MessageBox.Show(System.Windows.Window.GetWindow(this), message, Resources.OverwritePreviewTitle, MessageBoxButton.YesNo, MessageBoxImage.Warning);
                if (confirm != MessageBoxResult.Yes)
                {
                    return;
                }
            }

            var generatedFiles = await viewModel.GenerateForNodeAsync(selected);
            await ThreadHelper.JoinableTaskFactory.SwitchToMainThreadAsync();
            AddFilesToActiveProject(generatedFiles);
        });


    private async void OnGenerateModelClassesClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || !GenerationSupportedKinds.Contains(selected.Kind))
            {
                MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectNodeToGenerateModelClasses, Resources.GenerateModelClassesMenu, MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var generatedFiles = await viewModel.GenerateModelClassesForNodeAsync(selected);
            await ThreadHelper.JoinableTaskFactory.SwitchToMainThreadAsync();
            AddFilesToActiveProject(generatedFiles);
        });

    private async void OnGenerateRepositoryClassesClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || !GenerationSupportedKinds.Contains(selected.Kind))
            {
                MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectNodeToGenerateRepositoryClasses, Resources.GenerateRepositoryClassesMenu, MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var generatedFiles = await viewModel.GenerateRepositoryClassesForNodeAsync(selected);
            await ThreadHelper.JoinableTaskFactory.SwitchToMainThreadAsync();
            AddFilesToActiveProject(generatedFiles);
        });

    private async void OnCheckConsistencyClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || !GenerationSupportedKinds.Contains(selected.Kind))
            {
                MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectNodeToCheckConsistency, Resources.CheckConsistencyMenu, MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            await viewModel.CheckConsistencyAsync(selected);
        });

    private async void OnExtractScenarioClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(async () =>
        {
            if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.ConnectionId is null)
            {
                MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.SelectNodeToExtractScenario, Resources.ExtractScenarioButton, MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var tables = await viewModel.ListScenarioTablesAsync(selected.ConnectionId);
            if (tables.Count == 0)
            {
                MessageBox.Show(System.Windows.Window.GetWindow(this), Resources.NoTablesFoundInConnection, Resources.ExtractScenarioButton, MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            var dialog = new TestScenarioDialog(tables)
            {
                Owner = System.Windows.Window.GetWindow(this)
            };

            if (selected.Kind == ExplorerNodeKind.Object && selected.DatabaseObject is not null && selected.DatabaseObject.Type == DatabaseObjectType.Table)
            {
                dialog.SetPreselectedTable(selected.DatabaseObject.Schema, selected.DatabaseObject.Name);
            }

            dialog.LoadDataRequested += async () =>
            {
                var chosen = dialog.SelectedTable;
                if (chosen is null)
                {
                    MessageBox.Show(dialog, Resources.SelectTableMessage, Resources.ExtractScenarioButton, MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                dialog.SetBusy(true);
                try
                {
                    var rows = await viewModel.PreviewScenarioRowsAsync(selected.ConnectionId, chosen.Schema, chosen.TableName, dialog.FilterText);
                    var table = BuildRowsDataTable(rows);
                    dialog.SetRows(table);
                }
                finally
                {
                    dialog.SetBusy(false);
                }
            };

            dialog.ExtractRequested += async () =>
            {
                var chosen = dialog.SelectedTable;
                if (chosen is null)
                {
                    MessageBox.Show(dialog, Resources.SelectTableMessage, Resources.ExtractScenarioButton, MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                if (string.IsNullOrWhiteSpace(dialog.ScenarioName))
                {
                    MessageBox.Show(dialog, Resources.ProvideScenarioNameMessage, Resources.ExtractScenarioButton, MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                var selectedRows = dialog.GetSelectedRows();
                if (selectedRows.Count == 0)
                {
                    MessageBox.Show(dialog, Resources.SelectAtLeastOneRowMessage, Resources.ExtractScenarioButton, MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                dialog.SetBusy(true);
                try
                {
                    var path = await viewModel.ExtractScenarioAsync(selected.ConnectionId, dialog.ScenarioName, chosen.Schema, chosen.TableName, dialog.FilterText, selectedRows, dialog.IncludeParentReferences);
                    MessageBox.Show(dialog, string.Format(Resources.ScenarioExtractedWithFile, Environment.NewLine, path), Resources.ExtractScenarioButton, MessageBoxButton.OK, MessageBoxImage.Information);
                }
                finally
                {
                    dialog.SetBusy(false);
                }
            };

            dialog.Show();
        });

    private static DataTable BuildRowsDataTable(IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
    {
        var table = new DataTable();
        table.Columns.Add("_Selected", typeof(bool));

        var orderedColumns = rows
            .SelectMany(r => r.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        foreach (var column in orderedColumns)
        {
            table.Columns.Add(column, typeof(object));
        }

        foreach (var row in rows)
        {
            var dataRow = table.NewRow();
            dataRow["_Selected"] = false;

            foreach (var column in orderedColumns)
            {
                dataRow[column] = TryReadRowValue(row, column) ?? DBNull.Value;
            }

            table.Rows.Add(dataRow);
        }

        return table;
    }

    private static object? TryReadRowValue(IReadOnlyDictionary<string, object?> row, string key)
    {
        foreach (var item in row)
        {
            if (string.Equals(item.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                return item.Value;
            }
        }

        return null;
    }

    private async Task RunSafeAsync(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            ExtensionLogger.Log($"UI operation error: {ex}");
            MessageBox.Show(System.Windows.Window.GetWindow(this), ex.Message, Resources.UnexpectedErrorTitle, MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void AddFilesToActiveProject(IEnumerable<string> files)
    {
        ThreadHelper.ThrowIfNotOnUIThread();

        var fileList = files?.Where(File.Exists).Distinct(StringComparer.OrdinalIgnoreCase).ToArray() ?? [];
        if (fileList.Length == 0)
        {
            return;
        }

        var dte = Package.GetGlobalService(typeof(DTE)) as DTE;
        if (dte?.ActiveSolutionProjects is not Array activeProjects || activeProjects.Length == 0)
        {
            return;
        }

        if (activeProjects.GetValue(0) is not DteProject project)
        {
            return;
        }

        foreach (var file in fileList)
        {
            if (!ProjectContainsFile(project.ProjectItems, file))
            {
                project.ProjectItems?.AddFromFile(file);
            }
        }
    }

    private static bool ProjectContainsFile(DteProjectItems? items, string fullPath)
    {
        ThreadHelper.ThrowIfNotOnUIThread();

        if (items is null)
        {
            return false;
        }

        foreach (DteProjectItem item in items)
        {
            var itemPath = item.FileCount > 0 ? item.FileNames[1] : string.Empty;
            if (string.Equals(Path.GetFullPath(itemPath), Path.GetFullPath(fullPath), StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (ProjectContainsFile(item.ProjectItems, fullPath))
            {
                return true;
            }
        }

        return false;
    }
}

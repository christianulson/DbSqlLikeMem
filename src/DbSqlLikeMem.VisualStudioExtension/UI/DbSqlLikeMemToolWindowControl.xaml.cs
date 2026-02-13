using System.IO;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using DbSqlLikeMem.VisualStudioExtension.Services;
using EnvDTE;
using DteProject = EnvDTE.Project;
using DteProjectItem = EnvDTE.ProjectItem;
using DteProjectItems = EnvDTE.ProjectItems;
using Microsoft.VisualStudio.Shell;

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
                MessageBox.Show(System.Windows.Window.GetWindow(this), test.Message, "Falha de conexão", MessageBoxButton.OK, MessageBoxImage.Warning);
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
                MessageBox.Show(System.Windows.Window.GetWindow(this), "Selecione um nó de conexão para editar.", "Editar conexão", MessageBoxButton.OK, MessageBoxImage.Information);
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
                MessageBox.Show(System.Windows.Window.GetWindow(this), test.Message, "Falha de conexão", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            viewModel.UpdateConnection(selected, dialog.ConnectionName, dialog.DatabaseType, dialog.ConnectionString);
            await viewModel.RefreshObjectsAsync();
        });

    private void OnRemoveConnectionClick(object sender, RoutedEventArgs e)
    {
        if (ExplorerTree.SelectedItem is not ExplorerNode selected || selected.Kind != ExplorerNodeKind.Connection)
        {
            MessageBox.Show(System.Windows.Window.GetWindow(this), "Selecione um nó de conexão para remover.", "Remover conexão", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var confirm = MessageBox.Show(System.Windows.Window.GetWindow(this), $"Remover conexão '{selected.Label}'?", "Confirmação", MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (confirm == MessageBoxResult.Yes)
        {
            viewModel.RemoveConnection(selected);
        }
    }

    private void OnExplorerContextMenuOpened(object sender, RoutedEventArgs e)
    {
        var isConnectionNodeSelected = ExplorerTree.SelectedItem is ExplorerNode selectedConnection && selectedConnection.Kind == ExplorerNodeKind.Connection;
        var isObjectTypeNodeSelected = ExplorerTree.SelectedItem is ExplorerNode selected && selected.Kind == ExplorerNodeKind.ObjectType;

        EditConnectionMenuItem.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        RemoveConnectionMenuItem.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        RefreshConnectionMenuItem.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        ConnectionActionsSeparator.Visibility = isConnectionNodeSelected ? Visibility.Visible : Visibility.Collapsed;

        ConfigureMappingsMenuItem.Visibility = isObjectTypeNodeSelected ? Visibility.Visible : Visibility.Collapsed;
        ConfigureTemplatesMenuItem.Visibility = isObjectTypeNodeSelected ? Visibility.Visible : Visibility.Collapsed;
    }

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
            MessageBox.Show(System.Windows.Window.GetWindow(this), "Selecione um tipo de objeto para configurar mapeamentos.", "Configurar mapeamentos", MessageBoxButton.OK, MessageBoxImage.Information);
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
            MessageBox.Show(System.Windows.Window.GetWindow(this), "Selecione um tipo de objeto para configurar templates.", "Configurar templates", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var dialog = new TemplateConfigurationDialog(viewModel.GetTemplateConfiguration()) { Owner = System.Windows.Window.GetWindow(this) };
        if (dialog.ShowDialog() == true)
        {
            viewModel.ConfigureTemplates(dialog.ModelTemplatePath, dialog.RepositoryTemplatePath, dialog.ModelOutputDirectory, dialog.RepositoryOutputDirectory);
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
                MessageBox.Show(System.Windows.Window.GetWindow(this), "Selecione conexão, tipo de objeto ou objeto para gerar classes de teste.", "Gerar classes de teste", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var conflicts = viewModel.PreviewConflictsForNode(selected);
            if (conflicts.Count > 0)
            {
                var preview = string.Join(Environment.NewLine, conflicts.Take(10));
                var message = $"{conflicts.Count} arquivo(s) já existem e serão sobrescritos:\n\n{preview}";
                var confirm = MessageBox.Show(System.Windows.Window.GetWindow(this), message, "Pré-visualização de sobrescrita", MessageBoxButton.YesNo, MessageBoxImage.Warning);
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
                MessageBox.Show(System.Windows.Window.GetWindow(this), "Selecione conexão, tipo de objeto ou objeto para gerar classes de modelos.", "Gerar modelos", MessageBoxButton.OK, MessageBoxImage.Information);
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
                MessageBox.Show(System.Windows.Window.GetWindow(this), "Selecione conexão, tipo de objeto ou objeto para gerar classes de repositório.", "Gerar repositórios", MessageBoxButton.OK, MessageBoxImage.Information);
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
                MessageBox.Show(System.Windows.Window.GetWindow(this), "Selecione conexão, tipo de objeto ou objeto para checar consistência.", "Checar consistência", MessageBoxButton.OK, MessageBoxImage.Information);
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
            MessageBox.Show(System.Windows.Window.GetWindow(this), ex.Message, "Erro inesperado", MessageBoxButton.OK, MessageBoxImage.Error);
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

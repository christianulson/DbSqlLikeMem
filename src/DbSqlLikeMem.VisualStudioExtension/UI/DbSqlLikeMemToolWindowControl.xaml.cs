using System.Windows;
using System.Windows.Controls;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class DbSqlLikeMemToolWindowControl : UserControl
{
    private readonly DbSqlLikeMemToolWindowViewModel viewModel;

    public DbSqlLikeMemToolWindowControl()
    {
        InitializeComponent();
        viewModel = new DbSqlLikeMemToolWindowViewModel();
        DataContext = viewModel;
    }

    private void OnAddConnectionClick(object sender, RoutedEventArgs e)
    {
        var dialog = new ConnectionDialog
        {
            Owner = Window.GetWindow(this)
        };

        if (dialog.ShowDialog() == true)
        {
            viewModel.AddConnection(dialog.ConnectionName, dialog.DatabaseType, dialog.ConnectionString);
        }
    }

    private void OnConfigureMappingsClick(object sender, RoutedEventArgs e)
    {
        var dialog = new MappingDialog
        {
            Owner = Window.GetWindow(this)
        };

        if (dialog.ShowDialog() == true)
        {
            viewModel.ApplyDefaultMapping(dialog.FileNamePattern, dialog.OutputDirectory);
        }
    }
}

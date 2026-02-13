using System;
using System.IO;
using System.Windows;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.UI;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

/// <summary>
/// Main test harness window for visual validation of extension dialogs.
/// Janela principal de teste para validação visual das janelas da extensão.
/// </summary>
public partial class MainWindow : Window
{
    /// <summary>
    /// Initializes the main harness window.
    /// Inicializa a janela principal do harness.
    /// </summary>
    public MainWindow()
    {
        InitializeComponent();
    }

    private void OnOpenConnectionDialog(object sender, RoutedEventArgs e)
    {
        _ = new ConnectionDialog
        {
            Owner = this,
            ConnectionName = "ConexaoTeste",
            DatabaseType = "SqlServer",
            ConnectionString = "Server=localhost;Database=master;Trusted_Connection=True;"
        }.ShowDialog();
    }

    private void OnOpenMappingDialog(object sender, RoutedEventArgs e)
    {
        _ = new MappingDialog("{NamePascal}{Type}Factory.cs", Path.Combine(Environment.CurrentDirectory, "Generated"))
        {
            Owner = this
        }.ShowDialog();
    }

    private void OnOpenTemplateDialog(object sender, RoutedEventArgs e)
    {
        _ = new TemplateConfigurationDialog(new TemplateConfiguration(
            string.Empty,
            string.Empty,
            Path.Combine(Environment.CurrentDirectory, "Generated", "Models"),
            Path.Combine(Environment.CurrentDirectory, "Generated", "Repositories")))
        {
            Owner = this
        }.ShowDialog();
    }
}

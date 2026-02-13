using System;
using System.IO;
using System.Windows;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.UI;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

public partial class MainWindow : Window
{
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
        _ = new TemplateConfigurationDialog(new TemplateConfiguration
        {
            ModelTemplatePath = string.Empty,
            RepositoryTemplatePath = string.Empty,
            ModelOutputDirectory = Path.Combine(Environment.CurrentDirectory, "Generated", "Models"),
            RepositoryOutputDirectory = Path.Combine(Environment.CurrentDirectory, "Generated", "Repositories")
        })
        {
            Owner = this
        }.ShowDialog();
    }
}

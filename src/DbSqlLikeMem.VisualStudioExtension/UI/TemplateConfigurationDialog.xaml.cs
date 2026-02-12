using System;
using System.IO;
using System.Windows;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class TemplateConfigurationDialog : Window
{
    public TemplateConfigurationDialog(TemplateConfiguration current)
    {
        InitializeComponent();
        ModelTemplatePathTextBox.Text = current.ModelTemplatePath;
        RepositoryTemplatePathTextBox.Text = current.RepositoryTemplatePath;
        ModelOutputDirectoryTextBox.Text = current.ModelOutputDirectory;
        RepositoryOutputDirectoryTextBox.Text = current.RepositoryOutputDirectory;
    }

    public string ModelTemplatePath { get; private set; } = string.Empty;
    public string RepositoryTemplatePath { get; private set; } = string.Empty;
    public string ModelOutputDirectory { get; private set; } = "Generated/Models";
    public string RepositoryOutputDirectory { get; private set; } = "Generated/Repositories";

    private void OnSaveClick(object sender, RoutedEventArgs e)
    {
        ModelTemplatePath = ModelTemplatePathTextBox.Text.Trim();
        RepositoryTemplatePath = RepositoryTemplatePathTextBox.Text.Trim();
        ModelOutputDirectory = ModelOutputDirectoryTextBox.Text.Trim();
        RepositoryOutputDirectory = RepositoryOutputDirectoryTextBox.Text.Trim();

        if (string.IsNullOrWhiteSpace(ModelOutputDirectory) || string.IsNullOrWhiteSpace(RepositoryOutputDirectory))
        {
            MessageBox.Show(this, "Informe os diretórios de saída para Model e Repositório.", "Validação", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        if (!ValidateTemplatePathIfProvided(ModelTemplatePath) || !ValidateTemplatePathIfProvided(RepositoryTemplatePath))
        {
            return;
        }

        if (!ValidateOutputDirectory(ModelOutputDirectory) || !ValidateOutputDirectory(RepositoryOutputDirectory))
        {
            return;
        }

        DialogResult = true;
        Close();
    }

    private bool ValidateTemplatePathIfProvided(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return true;
        }

        var fullPath = Path.IsPathRooted(path)
            ? Path.GetFullPath(path)
            : Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), path));

        if (!File.Exists(fullPath))
        {
            MessageBox.Show(this, $"Template não encontrado: {fullPath}", "Validação", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        return true;
    }

    private bool ValidateOutputDirectory(string directory)
    {
        try
        {
            var fullPath = Path.GetFullPath(directory);
            _ = Directory.CreateDirectory(fullPath);
            return true;
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, $"Diretório inválido ou sem permissão: {directory}. Detalhe: {ex.Message}", "Validação", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }
    }
}

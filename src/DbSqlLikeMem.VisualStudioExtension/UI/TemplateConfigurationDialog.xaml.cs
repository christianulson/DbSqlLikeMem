using System.IO;
using System.Windows;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

using UiResources = DbSqlLikeMem.VisualStudioExtension.Properties.Resources;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class TemplateConfigurationDialog : Window
{
    /// <summary>
    /// Initializes a dialog to configure model and repository templates and output folders.
    /// Inicializa uma janela para configurar templates e pastas de saída de modelos e repositórios.
    /// </summary>
    public TemplateConfigurationDialog(TemplateConfiguration current)
    {
        InitializeComponent();
        TemplateBaselineProfileComboBox.ItemsSource = TemplateBaselineCatalog.GetProfiles().OrderBy(profile => profile.DisplayName).ToArray();
        TemplateBaselineProfileComboBox.SelectedValue = "api";
        ModelTemplatePathTextBox.Text = current.ModelTemplatePath;
        RepositoryTemplatePathTextBox.Text = current.RepositoryTemplatePath;
        ModelOutputDirectoryTextBox.Text = current.ModelOutputDirectory;
        RepositoryOutputDirectoryTextBox.Text = current.RepositoryOutputDirectory;
        ModelFileNamePatternTextBox.Text = current.ModelFileNamePattern;
        RepositoryFileNamePatternTextBox.Text = current.RepositoryFileNamePattern;
    }

    /// <summary>
    /// Gets the configured model template path.
    /// Obtém o caminho configurado do template de modelo.
    /// </summary>
    public string ModelTemplatePath { get; private set; } = string.Empty;
    /// <summary>
    /// Gets the configured repository template path.
    /// Obtém o caminho configurado do template de repositório.
    /// </summary>
    public string RepositoryTemplatePath { get; private set; } = string.Empty;
    /// <summary>
    /// Gets the output directory for generated model classes.
    /// Obtém o diretório de saída para classes de modelo geradas.
    /// </summary>
    public string ModelOutputDirectory { get; private set; } = "Generated/Models";
    /// <summary>
    /// Gets the output directory for generated repository classes.
    /// Obtém o diretório de saída para classes de repositório geradas.
    /// </summary>
    public string RepositoryOutputDirectory { get; private set; } = "Generated/Repositories";
    /// <summary>
    /// Gets the configured file name pattern for model generation.
    /// Obtém o padrão configurado de nome de arquivo para geração de modelos.
    /// </summary>
    public string ModelFileNamePattern { get; private set; } = "{NamePascal}Model.cs";
    /// <summary>
    /// Gets the configured file name pattern for repository generation.
    /// Obtém o padrão configurado de nome de arquivo para geração de repositórios.
    /// </summary>
    public string RepositoryFileNamePattern { get; private set; } = "{NamePascal}Repository.cs";

    private void OnApplyBaselineClick(object sender, RoutedEventArgs e)
    {
        if (TemplateBaselineProfileComboBox.SelectedItem is not TemplateBaselineProfile profile)
        {
            MessageBox.Show(this, "Select a template baseline profile before applying it.", UiResources.ValidationTitle, MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var repositoryRoot = TemplateBaselineCatalog.FindRepositoryRoot(Directory.GetCurrentDirectory())
            ?? TemplateBaselineCatalog.FindRepositoryRoot(AppContext.BaseDirectory);

        if (repositoryRoot is null)
        {
            MessageBox.Show(this, "Could not locate templates/dbsqllikemem from the current environment.", UiResources.ValidationTitle, MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        var configuration = TemplateBaselineCatalog.CreateTemplateConfiguration(repositoryRoot, profile.Id);
        ModelTemplatePathTextBox.Text = configuration.ModelTemplatePath;
        RepositoryTemplatePathTextBox.Text = configuration.RepositoryTemplatePath;
        ModelOutputDirectoryTextBox.Text = configuration.ModelOutputDirectory;
        RepositoryOutputDirectoryTextBox.Text = configuration.RepositoryOutputDirectory;
        ModelFileNamePatternTextBox.Text = configuration.ModelFileNamePattern;
        RepositoryFileNamePatternTextBox.Text = configuration.RepositoryFileNamePattern;
    }

    private void OnSaveClick(object sender, RoutedEventArgs e)
    {
        ModelTemplatePath = ModelTemplatePathTextBox.Text.Trim();
        RepositoryTemplatePath = RepositoryTemplatePathTextBox.Text.Trim();
        ModelOutputDirectory = ModelOutputDirectoryTextBox.Text.Trim();
        RepositoryOutputDirectory = RepositoryOutputDirectoryTextBox.Text.Trim();
        ModelFileNamePattern = string.IsNullOrWhiteSpace(ModelFileNamePatternTextBox.Text)
            ? "{NamePascal}Model.cs"
            : ModelFileNamePatternTextBox.Text.Trim();
        RepositoryFileNamePattern = string.IsNullOrWhiteSpace(RepositoryFileNamePatternTextBox.Text)
            ? "{NamePascal}Repository.cs"
            : RepositoryFileNamePatternTextBox.Text.Trim();

        if (string.IsNullOrWhiteSpace(ModelOutputDirectory) || string.IsNullOrWhiteSpace(RepositoryOutputDirectory))
        {
            MessageBox.Show(this, UiResources.OutputDirectoriesRequired, UiResources.ValidationTitle, MessageBoxButton.OK, MessageBoxImage.Warning);
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
            MessageBox.Show(this, string.Format(UiResources.TemplateNotFound, fullPath), UiResources.ValidationTitle, MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        var unsupportedTokens = TemplateTokenCatalog.FindUnsupportedTokens(File.ReadAllText(fullPath));
        if (unsupportedTokens.Count > 0)
        {
            MessageBox.Show(
                this,
                $"Unsupported template tokens: {string.Join(", ", unsupportedTokens)}",
                UiResources.ValidationTitle,
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
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
            MessageBox.Show(this, string.Format(UiResources.InvalidDirectoryWithDetail, directory, ex.Message), UiResources.ValidationTitle, MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }
    }
}

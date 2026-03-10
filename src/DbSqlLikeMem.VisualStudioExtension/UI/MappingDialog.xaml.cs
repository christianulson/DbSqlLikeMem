using System.Windows;
using System.IO;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.Core.Services;

using UiResources = DbSqlLikeMem.VisualStudioExtension.Properties.Resources;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class MappingDialog : Window
{
    private readonly DatabaseObjectType objectType;
    private readonly ConnectionMappingService connectionMappingService = new();
    private readonly TemplateReviewMetadata? reviewMetadata;

    /// <summary>
    /// EN: Initializes a dialog to configure file naming, output directory, and optional namespace mappings for one object type.
    /// PT: Inicializa uma janela para configurar padrão de nomes, diretorio de saida e namespace opcional dos mapeamentos para um tipo de objeto.
    /// </summary>
    /// <param name="objectType">EN: Object type whose mapping is being configured. PT: Tipo de objeto cujo mapeamento esta sendo configurado.</param>
    /// <param name="fileNamePattern">EN: Current file name pattern. PT: Padrao atual de nome de arquivo.</param>
    /// <param name="outputDirectory">EN: Current output directory. PT: Diretorio atual de saida.</param>
    /// <param name="namespace">EN: Current optional namespace. PT: Namespace opcional atual.</param>
    public MappingDialog(
        DatabaseObjectType objectType,
        string fileNamePattern,
        string outputDirectory,
        string? @namespace = null)
    {
        InitializeComponent();
        this.objectType = objectType;
        reviewMetadata = LoadReviewMetadata();
        TemplateBaselineProfileComboBox.ItemsSource = TemplateBaselineCatalog.GetProfiles().OrderBy(profile => profile.DisplayName).ToArray();
        TemplateBaselineProfileComboBox.SelectedValue = "api";
        FilePatternTextBox.Text = fileNamePattern;
        OutputDirectoryTextBox.Text = outputDirectory;
        NamespaceTextBox.Text = @namespace ?? string.Empty;
        RefreshBaselineSummary();
    }

    /// <summary>
    /// Gets the file name pattern used during class generation.
    /// Obtém o padrão de nome de arquivo usado durante a geração de classes.
    /// </summary>
    public string FileNamePattern { get; private set; } = "{NamePascal}{Type}Factory.cs";

    /// <summary>
    /// Gets the output directory used to save generated files.
    /// Obtém o diretório de saída usado para salvar arquivos gerados.
    /// </summary>
    public string OutputDirectory { get; private set; } = "Generated";

    /// <summary>
    /// Gets the optional namespace applied to generated content.
    /// Obtém o namespace opcional aplicado ao conteúdo gerado.
    /// </summary>
    public string Namespace { get; private set; } = string.Empty;

    private void OnApplyBaselineClick(object sender, RoutedEventArgs e)
    {
        if (TemplateBaselineProfileComboBox.SelectedItem is not TemplateBaselineProfile profile)
        {
            MessageBox.Show(this, "Select a mapping baseline profile before applying it.", UiResources.ValidationTitle, MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var mapping = connectionMappingService.CreateRecommendedMapping(profile.Id, objectType, NamespaceTextBox.Text);
        FilePatternTextBox.Text = mapping.FileNamePattern;
        OutputDirectoryTextBox.Text = mapping.OutputDirectory;
        NamespaceTextBox.Text = mapping.Namespace ?? string.Empty;
    }

    private void OnBaselineSelectionChanged(object sender, System.Windows.Controls.SelectionChangedEventArgs e)
        => RefreshBaselineSummary();

    private void OnSaveClick(object sender, RoutedEventArgs e)
    {
        FileNamePattern = FilePatternTextBox.Text.Trim();
        OutputDirectory = OutputDirectoryTextBox.Text.Trim();
        Namespace = NamespaceTextBox.Text.Trim();

        if (string.IsNullOrWhiteSpace(FileNamePattern) || string.IsNullOrWhiteSpace(OutputDirectory))
        {
            MessageBox.Show(this, UiResources.FillMappingPatternAndOutput, UiResources.ValidationTitle, MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        DialogResult = true;
        Close();
    }

    private void RefreshBaselineSummary()
    {
        BaselineSummaryTextBlock.Text = TemplateBaselineProfileComboBox.SelectedItem is TemplateBaselineProfile profile
            ? TemplateBaselinePresentation.BuildMappingSummary(profile, objectType, reviewMetadata)
            : "Select a baseline profile to preview the recommended mapping defaults for this object type.";
    }

    private static TemplateReviewMetadata? LoadReviewMetadata()
    {
        var repositoryRoot = TemplateBaselineCatalog.FindRepositoryRoot(Directory.GetCurrentDirectory())
            ?? TemplateBaselineCatalog.FindRepositoryRoot(AppContext.BaseDirectory);
        return repositoryRoot is null
            ? null
            : TemplateReviewMetadataReader.TryLoadFromRepositoryRoot(repositoryRoot);
    }
}

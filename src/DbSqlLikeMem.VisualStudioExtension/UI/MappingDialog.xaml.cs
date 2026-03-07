using System.Windows;

using UiResources = DbSqlLikeMem.VisualStudioExtension.Properties.Resources;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class MappingDialog : Window
{
    /// <summary>
    /// Initializes a dialog to configure file naming, output directory, and optional namespace mappings.
    /// Inicializa uma janela para configurar padrão de nomes, diretório de saída e namespace opcional dos mapeamentos.
    /// </summary>
    public MappingDialog(string fileNamePattern, string outputDirectory, string? @namespace = null)
    {
        InitializeComponent();
        FilePatternTextBox.Text = fileNamePattern;
        OutputDirectoryTextBox.Text = outputDirectory;
        NamespaceTextBox.Text = @namespace ?? string.Empty;
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
}

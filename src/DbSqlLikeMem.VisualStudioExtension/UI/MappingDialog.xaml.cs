using System.Windows;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class MappingDialog : Window
{
    /// <summary>
    /// Initializes a dialog to configure file naming and output directory mappings.
    /// Inicializa uma janela para configurar o padrão de nomes e diretórios de saída.
    /// </summary>
    public MappingDialog(string fileNamePattern, string outputDirectory)
    {
        InitializeComponent();
        FilePatternTextBox.Text = fileNamePattern;
        OutputDirectoryTextBox.Text = outputDirectory;
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

    private void OnSaveClick(object sender, RoutedEventArgs e)
    {
        FileNamePattern = FilePatternTextBox.Text.Trim();
        OutputDirectory = OutputDirectoryTextBox.Text.Trim();

        if (string.IsNullOrWhiteSpace(FileNamePattern) || string.IsNullOrWhiteSpace(OutputDirectory))
        {
            MessageBox.Show(this, "Preencha o padrão de arquivo e o diretório de saída.", "Validação", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        DialogResult = true;
        Close();
    }
}

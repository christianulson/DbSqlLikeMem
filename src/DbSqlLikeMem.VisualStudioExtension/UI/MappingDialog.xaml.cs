using System.Windows;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class MappingDialog : Window
{
    public MappingDialog()
    {
        InitializeComponent();
    }

    public string FileNamePattern { get; private set; } = "{NamePascal}{Type}Factory.cs";

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

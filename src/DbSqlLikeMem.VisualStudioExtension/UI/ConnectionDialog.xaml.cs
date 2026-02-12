using System.Windows;
using System.Windows.Controls;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class ConnectionDialog : Window
{
    public ConnectionDialog()
    {
        InitializeComponent();
    }

    public string ConnectionName { get; private set; } = string.Empty;

    public string DatabaseType { get; private set; } = "SqlServer";

    public string ConnectionString { get; private set; } = string.Empty;

    private void OnSaveClick(object sender, RoutedEventArgs e)
    {
        ConnectionName = NameTextBox.Text.Trim();
        ConnectionString = ConnectionStringTextBox.Text.Trim();
        DatabaseType = (DatabaseTypeCombo.SelectedItem as ComboBoxItem)?.Content?.ToString() ?? "SqlServer";

        if (string.IsNullOrWhiteSpace(ConnectionName) || string.IsNullOrWhiteSpace(ConnectionString))
        {
            MessageBox.Show(this, "Preencha nome e connection string.", "Validação", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        DialogResult = true;
        Close();
    }
}

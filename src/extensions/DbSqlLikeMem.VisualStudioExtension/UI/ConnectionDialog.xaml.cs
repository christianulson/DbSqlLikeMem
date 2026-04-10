using System.Linq;
using System.Windows;
using System.Windows.Controls;

using UiResources = DbSqlLikeMem.VisualStudioExtension.Properties.Resources;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class ConnectionDialog : Window
{
    /// <summary>
    /// Initializes a dialog used to collect connection settings from the user.
    /// Inicializa uma janela usada para coletar configurações de conexão do usuário.
    /// </summary>
    public ConnectionDialog()
    {
        InitializeComponent();
        DatabaseTypeCombo.ItemsSource = DatabaseTypeCatalog.SupportedDatabaseTypes;
        Loaded += OnLoaded;
    }

    /// <summary>
    /// Gets or sets the display name of the saved connection.
    /// Obtém ou define o nome de exibição da conexão salva.
    /// </summary>
    public string ConnectionName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the selected database provider type.
    /// Obtém ou define o tipo de provedor de banco selecionado.
    /// </summary>
    public string DatabaseType { get; set; } = DatabaseTypeCatalog.DefaultDatabaseType;

    /// <summary>
    /// Gets or sets the connection string used to access the database.
    /// Obtém ou define a connection string usada para acessar o banco de dados.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    private void OnLoaded(object sender, RoutedEventArgs e)
    {
        NameTextBox.Text = ConnectionName;
        ConnectionStringTextBox.Text = ConnectionString;

        DatabaseTypeCombo.SelectedItem = DatabaseTypeCatalog.SupportedDatabaseTypes.FirstOrDefault(type => string.Equals(type, DatabaseType, StringComparison.OrdinalIgnoreCase))
            ?? DatabaseTypeCatalog.SupportedDatabaseTypes[0];
    }

    private void OnSaveClick(object sender, RoutedEventArgs e)
    {
        ConnectionName = NameTextBox.Text.Trim();
        ConnectionString = ConnectionStringTextBox.Text.Trim();
        DatabaseType = DatabaseTypeCombo.SelectedItem?.ToString() ?? DatabaseTypeCatalog.DefaultDatabaseType;

        if (string.IsNullOrWhiteSpace(ConnectionName) || string.IsNullOrWhiteSpace(ConnectionString))
        {
            MessageBox.Show(this, UiResources.FillConnectionNameAndString, UiResources.ValidationTitle, MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        DialogResult = true;
        Close();
    }
}

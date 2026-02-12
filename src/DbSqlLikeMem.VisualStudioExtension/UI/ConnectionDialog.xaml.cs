using System;
using System.Windows;
using System.Windows.Controls;

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
    public string DatabaseType { get; set; } = "SqlServer";

    /// <summary>
    /// Gets or sets the connection string used to access the database.
    /// Obtém ou define a connection string usada para acessar o banco de dados.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    private void OnLoaded(object sender, RoutedEventArgs e)
    {
        NameTextBox.Text = ConnectionName;
        ConnectionStringTextBox.Text = ConnectionString;

        foreach (var item in DatabaseTypeCombo.Items)
        {
            if (item is ComboBoxItem comboItem && string.Equals(comboItem.Content?.ToString(), DatabaseType, StringComparison.OrdinalIgnoreCase))
            {
                DatabaseTypeCombo.SelectedItem = comboItem;
                return;
            }
        }

        DatabaseTypeCombo.SelectedIndex = 0;
    }

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

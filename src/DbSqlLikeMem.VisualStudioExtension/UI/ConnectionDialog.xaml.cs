using System;
using System.Windows;
using System.Windows.Controls;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class ConnectionDialog : Window
{
    public ConnectionDialog()
    {
        InitializeComponent();
        Loaded += OnLoaded;
    }

    public string ConnectionName { get; set; } = string.Empty;

    public string DatabaseType { get; set; } = "SqlServer";

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

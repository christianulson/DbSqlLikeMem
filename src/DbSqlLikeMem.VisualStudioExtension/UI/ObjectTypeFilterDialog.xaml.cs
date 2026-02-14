using System.Windows;
using System.Windows.Controls;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

/// <summary>
/// Dialog used to configure object-type filtering in the explorer.
/// Janela usada para configurar o filtro por tipo de objeto no explorador.
/// </summary>
public partial class ObjectTypeFilterDialog : Window
{
    /// <summary>
    /// Initializes the dialog with current filter values.
    /// Inicializa a janela com os valores atuais de filtro.
    /// </summary>
    public ObjectTypeFilterDialog(string filterText, FilterMode filterMode)
    {
        InitializeComponent();
        FilterTextBox.Text = filterText;
        FilterModeCombo.SelectedIndex = filterMode == FilterMode.Equals ? 1 : 0;
    }

    /// <summary>
    /// Gets the filter text entered by the user.
    /// Obtém o texto de filtro informado pelo usuário.
    /// </summary>
    public string FilterText { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the matching mode selected by the user.
    /// Obtém o modo de comparação selecionado pelo usuário.
    /// </summary>
    public FilterMode FilterMode { get; private set; } = FilterMode.Like;

    private void OnSaveClick(object sender, RoutedEventArgs e)
    {
        FilterText = FilterTextBox.Text.Trim();
        var selectedTag = (FilterModeCombo.SelectedItem as ComboBoxItem)?.Tag?.ToString();
        FilterMode = string.Equals(selectedTag, "Equals", StringComparison.OrdinalIgnoreCase)
            ? FilterMode.Equals
            : FilterMode.Like;

        DialogResult = true;
        Close();
    }
}

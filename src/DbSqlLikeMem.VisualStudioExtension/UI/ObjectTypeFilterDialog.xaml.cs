using System.Windows;
using System.Windows.Controls;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class ObjectTypeFilterDialog : Window
{
    public ObjectTypeFilterDialog(string filterText, FilterMode filterMode)
    {
        InitializeComponent();
        FilterTextBox.Text = filterText;
        FilterModeCombo.SelectedIndex = filterMode == FilterMode.Equals ? 1 : 0;
    }

    public string FilterText { get; private set; } = string.Empty;

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

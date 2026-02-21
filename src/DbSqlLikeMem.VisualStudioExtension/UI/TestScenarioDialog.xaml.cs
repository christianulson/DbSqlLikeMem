using System.Data;
using System.Windows;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

/// <summary>
/// Represents the dialog used to create and extract test scenarios from table data.
/// Representa o diálogo usado para criar e extrair cenários de teste a partir de dados de tabela.
/// </summary>
public partial class TestScenarioDialog : Window
{
    /// <summary>
    /// Occurs when the user requests loading table data preview.
    /// Ocorre quando o usuário solicita carregar a pré-visualização dos dados da tabela.
    /// </summary>
    public event Func<Task>? LoadDataRequested;

    /// <summary>
    /// Occurs when the user requests scenario extraction.
    /// Ocorre quando o usuário solicita a extração do cenário.
    /// </summary>
    public event Func<Task>? ExtractRequested;

    /// <summary>
    /// Gets the scenario name.
    /// Obtém o nome do cenário.
    /// </summary>
    public string ScenarioName => ScenarioNameTextBox.Text.Trim();

    /// <summary>
    /// Gets the selected table option.
    /// Obtém a opção de tabela selecionada.
    /// </summary>
    public DbSqlLikeMemToolWindowViewModel.ScenarioTableOption? SelectedTable => TableComboBox.SelectedItem as DbSqlLikeMemToolWindowViewModel.ScenarioTableOption;

    /// <summary>
    /// Gets the SQL filter text used in the WHERE clause.
    /// Obtém o texto do filtro SQL usado na cláusula WHERE.
    /// </summary>
    public string FilterText => FilterTextBox.Text.Trim();

    /// <summary>
    /// Gets whether parent reference rows (FK) should be included.
    /// Obtém se as linhas de referência pai (FK) devem ser incluídas.
    /// </summary>
    public bool IncludeParentReferences => IncludeParentsCheckBox.IsChecked == true;

    /// <summary>
    /// Initializes the dialog with available table options.
    /// Inicializa o diálogo com as opções de tabela disponíveis.
    /// </summary>
    public TestScenarioDialog(IReadOnlyCollection<DbSqlLikeMemToolWindowViewModel.ScenarioTableOption> tables)
    {
        InitializeComponent();
        TableComboBox.ItemsSource = tables;
        TableComboBox.SelectedIndex = tables.Count > 0 ? 0 : -1;
    }

    /// <summary>
    /// Preselects a table in the combo box.
    /// Pré-seleciona uma tabela no combo.
    /// </summary>
    public void SetPreselectedTable(string schema, string table)
    {
        foreach (var item in TableComboBox.Items)
        {
            if (item is DbSqlLikeMemToolWindowViewModel.ScenarioTableOption option
                && string.Equals(option.Schema, schema, StringComparison.OrdinalIgnoreCase)
                && string.Equals(option.TableName, table, StringComparison.OrdinalIgnoreCase))
            {
                TableComboBox.SelectedItem = option;
                break;
            }
        }
    }

    /// <summary>
    /// Binds preview rows to the grid.
    /// Vincula as linhas de pré-visualização ao grid.
    /// </summary>
    public void SetRows(DataTable dataTable)
    {
        RowsGrid.ItemsSource = dataTable.DefaultView;
        if (RowsGrid.Columns.Count > 0)
        {
            RowsGrid.Columns[0].DisplayIndex = 0;
            RowsGrid.Columns[0].Width = 90;
        }
    }

    /// <summary>
    /// Sets dialog busy state.
    /// Define o estado de ocupado do diálogo.
    /// </summary>
    public void SetBusy(bool isBusy)
    {
        Cursor = isBusy ? System.Windows.Input.Cursors.Wait : null;
        IsEnabled = !isBusy;
    }

    /// <summary>
    /// Returns selected rows from the preview grid.
    /// Retorna as linhas selecionadas da grade de pré-visualização.
    /// </summary>
    public List<IReadOnlyDictionary<string, object?>> GetSelectedRows()
    {
        var selected = new List<IReadOnlyDictionary<string, object?>>();
        if (RowsGrid.ItemsSource is not DataView view)
        {
            return selected;
        }

        foreach (DataRowView rowView in view)
        {
            if (rowView.Row.Field<bool>("_Selected") != true)
            {
                continue;
            }

            var data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (DataColumn column in view.Table.Columns)
            {
                if (column.ColumnName == "_Selected")
                {
                    continue;
                }

                var value = rowView.Row[column.ColumnName];
                data[column.ColumnName] = value == DBNull.Value ? null : value;
            }

            selected.Add(data);
        }

        return selected;
    }

    private async void OnLoadDataClick(object sender, RoutedEventArgs e)
    {
        if (LoadDataRequested is not null)
        {
            await LoadDataRequested();
        }
    }

    private async void OnExtractClick(object sender, RoutedEventArgs e)
    {
        if (ExtractRequested is not null)
        {
            await ExtractRequested();
        }
    }

    private void OnCloseClick(object sender, RoutedEventArgs e)
        => Close();

    private void OnSelectAllClick(object sender, RoutedEventArgs e)
        => SetSelection(true);

    private void OnClearSelectionClick(object sender, RoutedEventArgs e)
        => SetSelection(false);

    private void SetSelection(bool selected)
    {
        if (RowsGrid.ItemsSource is not DataView view)
        {
            return;
        }

        foreach (DataRowView rowView in view)
        {
            rowView.Row["_Selected"] = selected;
        }
    }
}

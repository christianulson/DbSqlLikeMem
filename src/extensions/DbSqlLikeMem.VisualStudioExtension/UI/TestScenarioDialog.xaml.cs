using System.Data;
using System.Windows;
using System.Windows.Controls;
using DbSqlLikeMem.VisualStudioExtension.Services;
using UiResources = DbSqlLikeMem.VisualStudioExtension.Properties.Resources;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

/// <summary>
/// Represents the dialog used to create and extract test scenarios from table data.
/// Representa o diálogo usado para criar e extrair cenários de teste a partir de dados de tabela.
/// </summary>
public partial class TestScenarioDialog : Window
{
    private bool isBusy;
    private bool operationRunning;
    private bool isClosed;
    private string previewStatus = UiResources.ScenarioPreviewHint;
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
        Closed += (_, _) => isClosed = true;
        SetBusy(false);
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
    /// EN: Displays preview rows and their count while the dialog is open.
    /// PT-br: Exibe as linhas da previa e sua contagem enquanto o dialogo esta aberto.
    /// </summary>
    public void SetRows(DataTable dataTable)
    {
        if (isClosed)
        {
            return;
        }

        previewStatus = dataTable.Rows.Count == 0
            ? UiResources.ScenarioPreviewEmpty
            : string.Format(UiResources.ScenarioPreviewCount, dataTable.Rows.Count);
        RowsGrid.ItemsSource = dataTable.DefaultView;
        if (RowsGrid.Columns.Count > 0)
        {
            RowsGrid.Columns[0].DisplayIndex = 0;
            RowsGrid.Columns[0].Width = 90;
        }
        SetBusy(isBusy);
    }

    /// <summary>
    /// EN: Updates progress and enables preview actions when rows are available, keeping Close accessible.
    /// PT-br: Atualiza o progresso e habilita acoes da previa quando ha linhas, mantendo Fechar acessivel.
    /// </summary>
    public void SetBusy(bool isBusy)
    {
        this.isBusy = isBusy;
        if (isClosed)
        {
            return;
        }

        var hasRows = RowsGrid.ItemsSource is DataView { Count: > 0 };
        PreviewProgressBar.Visibility = isBusy ? Visibility.Visible : Visibility.Collapsed;
        PreviewStatusTextBlock.Text = isBusy ? UiResources.ScenarioWorking : previewStatus;
        Cursor = isBusy ? System.Windows.Input.Cursors.Wait : null;
        ScenarioNameTextBox.IsEnabled = !isBusy;
        TableComboBox.IsEnabled = !isBusy;
        FilterTextBox.IsEnabled = !isBusy;
        IncludeParentsCheckBox.IsEnabled = !isBusy;
        LoadDataButton.IsEnabled = !isBusy && SelectedTable is not null;
        SelectAllButton.IsEnabled = !isBusy && hasRows;
        ClearAllButton.IsEnabled = !isBusy && hasRows;
        RowsGrid.IsEnabled = !isBusy;
        ExtractScenarioButton.IsEnabled = !isBusy && hasRows;
    }

    /// <summary>
    /// Returns selected rows from the preview grid.
    /// Retorna as linhas selecionadas da grade de pré-visualização.
    /// </summary>
    public List<IReadOnlyDictionary<string, object?>> GetSelectedRows()
    {
        RowsGrid.CommitEdit(DataGridEditingUnit.Cell, true);
        RowsGrid.CommitEdit(DataGridEditingUnit.Row, true);
        var selected = new List<IReadOnlyDictionary<string, object?>>();
        if (RowsGrid.ItemsSource is not DataView view)
        {
            return selected;
        }

        foreach (DataRowView rowView in view)
        {
            var selectionColumn = view.Table.ExtendedProperties["SelectionColumn"] as string ?? "_Selected";
            if (rowView.Row.Field<bool>(selectionColumn) != true)
            {
                continue;
            }

            var data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (DataColumn column in view.Table.Columns)
            {
                if (column.ColumnName == selectionColumn)
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
        if (operationRunning)
        {
            return;
        }
        InvalidatePreview();
        await RunSafeAsync(LoadDataRequested);
    }

    private async void OnExtractClick(object sender, RoutedEventArgs e)
        => await RunSafeAsync(ExtractRequested);

    private async Task RunSafeAsync(Func<Task>? action)
    {
        if (action is null || operationRunning || isClosed)
        {
            return;
        }
        operationRunning = true;
        try
        {
            await action();
        }
        catch (OperationCanceledException)
        {
            // Cancellation is an expected user action.
        }
        catch (Exception ex)
        {
            ExtensionLogger.Log($"Scenario operation error: {ex}");
            previewStatus = ex.Message;
            if (!isClosed)
            {
                MessageBox.Show(this, ex.Message, UiResources.UnexpectedErrorTitle, MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        finally
        {
            operationRunning = false;
            SetBusy(false);
        }
    }

    private void OnTableSelectionChanged(object sender, SelectionChangedEventArgs e)
        => InvalidatePreview();

    private void OnFilterTextChanged(object sender, TextChangedEventArgs e)
        => InvalidatePreview();

    private void InvalidatePreview()
    {
        if (RowsGrid is null || PreviewStatusTextBlock is null || ExtractScenarioButton is null)
        {
            return;
        }

        RowsGrid.ItemsSource = null;
        previewStatus = UiResources.ScenarioPreviewHint;
        SetBusy(isBusy);
    }

    private void OnAutoGeneratingColumn(object sender, DataGridAutoGeneratingColumnEventArgs e)
    {
        var selectionColumn = (RowsGrid.ItemsSource as DataView)?.Table.ExtendedProperties["SelectionColumn"] as string ?? "_Selected";
        e.Column.IsReadOnly = e.PropertyName != selectionColumn;
        if (e.PropertyName == selectionColumn)
        {
            e.Column.Header = UiResources.SelectColumnHeader;
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
        RowsGrid.CommitEdit(DataGridEditingUnit.Cell, true);
        RowsGrid.CommitEdit(DataGridEditingUnit.Row, true);
        if (RowsGrid.ItemsSource is not DataView view)
        {
            return;
        }

        foreach (DataRowView rowView in view)
        {
            var selectionColumn = view.Table.ExtendedProperties["SelectionColumn"] as string ?? "_Selected";
            rowView.Row[selectionColumn] = selected;
        }
    }
}

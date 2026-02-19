using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using System.Windows;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public partial class TestScenarioDialog : Window
{
    public event Func<Task>? LoadDataRequested;
    public event Func<Task>? ExtractRequested;

    public string ScenarioName => ScenarioNameTextBox.Text.Trim();
    public DbSqlLikeMemToolWindowViewModel.ScenarioTableOption? SelectedTable => TableComboBox.SelectedItem as DbSqlLikeMemToolWindowViewModel.ScenarioTableOption;
    public string FilterText => FilterTextBox.Text.Trim();
    public bool IncludeParentReferences => IncludeParentsCheckBox.IsChecked == true;

    public TestScenarioDialog(IReadOnlyCollection<DbSqlLikeMemToolWindowViewModel.ScenarioTableOption> tables)
    {
        InitializeComponent();
        TableComboBox.ItemsSource = tables;
        TableComboBox.SelectedIndex = tables.Count > 0 ? 0 : -1;
    }

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

    public void SetRows(DataTable dataTable)
    {
        RowsGrid.ItemsSource = dataTable.DefaultView;
        if (RowsGrid.Columns.Count > 0)
        {
            RowsGrid.Columns[0].DisplayIndex = 0;
            RowsGrid.Columns[0].Width = 90;
        }
    }

    public void SetBusy(bool isBusy)
    {
        Cursor = isBusy ? System.Windows.Input.Cursors.Wait : null;
        IsEnabled = !isBusy;
    }

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

    private void OnLoadDataClick(object sender, RoutedEventArgs e)
        => _ = LoadDataRequested?.Invoke();

    private void OnExtractClick(object sender, RoutedEventArgs e)
        => _ = ExtractRequested?.Invoke();

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

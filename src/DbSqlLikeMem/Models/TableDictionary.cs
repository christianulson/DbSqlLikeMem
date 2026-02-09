namespace DbSqlLikeMem;

public class TableDictionary
    : Dictionary<string, ITableMock>,
    ITableDictionary
{
    public TableDictionary()
        : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    public TableDictionary(
        IDictionary<string, ITableMock>? tables)
    : base(StringComparer.OrdinalIgnoreCase)
    {
        ArgumentNullException.ThrowIfNull(tables);
        foreach (var (k, v) in tables)
            Add(k, v);
    }
}

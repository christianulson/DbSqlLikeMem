using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem;
public class TableResultMock : List<Dictionary<int, object?>>
{
    public IList<TableResultColMock> Columns
    { get; internal set; } = [];

    public IList<Dictionary<string, object?>> JoinFields { get; internal set; } = [];

    public int GetColumnIndexOrThrow(string col)
    {
        var found = Columns.FirstOrDefault(c =>
            c.ColumnAlias.Equals(col, StringComparison.OrdinalIgnoreCase)
            || c.ColumnName.Equals(col, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"Column '{col}' not found in subquery result.");
        return found.ColumIndex;
    }
}


public class TableResultColMock
{
    [SetsRequiredMembers]
    public TableResultColMock(
        string tableAlias,
        string columnAlias,
        string columnName,
        int columIndex,
        DbType dbType,
        bool isNullable
        )
    {
        TableAlias = tableAlias;
        ColumnAlias = columnAlias;
        ColumnName = columnName;
        ColumIndex = columIndex;
        DbType = dbType;
        IsNullable = isNullable;
    }

    public required string TableAlias { get; set; }
    public required string ColumnAlias { get; set; }
    public required string ColumnName { get; set; }
    public int ColumIndex { get; set; }
    public DbType DbType { get; set; }
    public bool IsNullable { get; set; }
}

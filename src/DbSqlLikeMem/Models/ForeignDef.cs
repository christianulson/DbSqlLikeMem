namespace DbSqlLikeMem.Models;

public sealed record Foreign(
    string name,
    string RefTableName,
    HashSet<(string col, string refCol)> references);


public sealed class ForeignDef
{
    internal ForeignDef(
        ITableMock table,
        string name,
        ITableMock refTable,
        HashSet<(ColumnDef col, ColumnDef refCol)> references
        )
    {
        Table = table;
        Name = name;
        RefTable = refTable;
        References = references;
    }

    /// <summary>
    /// EN: Parent table.
    /// PT: Tabela pai.
    /// </summary>
    public ITableMock Table { get; private set; }

    public string Name { get; private set; }

    public ITableMock RefTable { get; private set; }

    public HashSet<(ColumnDef col, ColumnDef refCol)> References { get; private set; }
}
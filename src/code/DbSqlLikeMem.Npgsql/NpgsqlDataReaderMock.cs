namespace DbSqlLikeMem.Npgsql;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Represents Npgsql Data Reader Mock.
/// PT: Representa Npgsql Data leitor simulado.
/// </summary>
public sealed class NpgsqlDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
    /// <inheritdoc />
    protected override string NormalizeColumnNameForGetName(string columnAlias, string columnName)
        => IsBareProjectedColumn(columnAlias, columnName)
            ? ExtractIdentifierTail(columnAlias).ToLowerInvariant()
            : columnAlias;

    /// <inheritdoc />
    public override DataTable GetSchemaTable()
    {
        var table = base.GetSchemaTable();
        foreach (DataColumn column in table.Columns)
        {
            if (column.ColumnName is not null)
                column.ColumnName = column.ColumnName.ToLowerInvariant();
        }

        return table;
    }

    private static bool IsBareProjectedColumn(string columnAlias, string columnName)
        => string.Equals(ExtractIdentifierTail(columnAlias), ExtractIdentifierTail(columnName), StringComparison.OrdinalIgnoreCase);

    private static string ExtractIdentifierTail(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        var dot = trimmed.LastIndexOf('.');
        if (dot >= 0 && dot + 1 < trimmed.Length)
            trimmed = trimmed[(dot + 1)..];

        trimmed = trimmed.Trim().Trim('`').Trim('"').Trim('[').Trim(']').Trim();
        return trimmed;
    }
}

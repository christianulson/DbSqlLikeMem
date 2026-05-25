namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents the Firebird data reader mock used by provider commands.
/// PT-br: Representa o leitor de dados simulado do Firebird usado pelos comandos do provedor.
/// </summary>
public sealed class FirebirdDataReaderMock(
    IList<TableResultMock> tables
) : DbDataReaderMockBase(tables)
{
    /// <inheritdoc />
    protected override string NormalizeColumnNameForGetName(string columnAlias, string columnName)
        => base.NormalizeColumnNameForGetName(columnAlias, columnName).ToUpperInvariant();
}


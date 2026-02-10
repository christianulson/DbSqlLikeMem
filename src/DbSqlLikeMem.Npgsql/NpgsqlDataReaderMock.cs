namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// Npgsql reader mock. Reusa a implementação do MySqlDataReaderMock.
/// </summary>
#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class NpgsqlDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// Npgsql reader mock. Reusa a implementação do MySqlDataReaderMock.
/// </summary>
#pragma warning disable CA1010 // Generic interface should also be implemented
public sealed class NpgsqlDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

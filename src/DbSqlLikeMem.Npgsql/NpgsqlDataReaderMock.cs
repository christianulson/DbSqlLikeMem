namespace DbSqlLikeMem.Npgsql;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Summary for NpgsqlDataReaderMock.
/// PT: Resumo para NpgsqlDataReaderMock.
/// </summary>
public sealed class NpgsqlDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

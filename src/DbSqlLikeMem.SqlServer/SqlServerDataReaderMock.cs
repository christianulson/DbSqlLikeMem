namespace DbSqlLikeMem.SqlServer;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Summary for SqlServerDataReaderMock.
/// PT: Resumo para SqlServerDataReaderMock.
/// </summary>
public sealed class SqlServerDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

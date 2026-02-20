namespace DbSqlLikeMem.Sqlite;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Summary for SqliteDataReaderMock.
/// PT: Resumo para SqliteDataReaderMock.
/// </summary>
public class SqliteDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{

}

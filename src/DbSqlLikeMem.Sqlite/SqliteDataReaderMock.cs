namespace DbSqlLikeMem.Sqlite;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Mock data reader for SQLite query results.
/// PT: Leitor de dados mock para resultados SQLite.
/// </summary>
public class SqliteDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{

}

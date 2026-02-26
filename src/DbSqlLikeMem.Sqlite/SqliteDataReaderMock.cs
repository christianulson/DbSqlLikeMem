namespace DbSqlLikeMem.Sqlite;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Represents Sqlite Data Reader Mock.
/// PT: Representa Sqlite Data leitor simulado.
/// </summary>
public class SqliteDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{

}

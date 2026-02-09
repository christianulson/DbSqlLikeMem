namespace DbSqlLikeMem.MySql;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Mock data reader for MySQL query results.
/// PT: Leitor de dados mock para resultados MySQL.
/// </summary>
public class MySqlDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{

}

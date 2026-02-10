namespace DbSqlLikeMem.Db2;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Mock data reader for DB2 query results.
/// PT: Leitor de dados mock para resultados DB2.
/// </summary>
public class Db2DataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{

}

namespace DbSqlLikeMem.Db2;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Summary for Db2DataReaderMock.
/// PT: Resumo para Db2DataReaderMock.
/// </summary>
public class Db2DataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{

}

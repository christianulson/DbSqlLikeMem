namespace DbSqlLikeMem.Db2;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Represents Db2 Data Reader Mock.
/// PT: Representa Db2 Data leitor simulado.
/// </summary>
public class Db2DataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{

}

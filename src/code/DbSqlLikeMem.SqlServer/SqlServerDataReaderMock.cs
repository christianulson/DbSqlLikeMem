namespace DbSqlLikeMem.SqlServer;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Represents Sql Server Data Reader Mock.
/// PT-br: Representa Sql Server Data leitor simulado.
/// </summary>
public sealed class SqlServerDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

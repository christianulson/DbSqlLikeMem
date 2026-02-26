namespace DbSqlLikeMem.Oracle;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Represents Oracle Data Reader Mock.
/// PT: Representa Oracle Data leitor simulado.
/// </summary>
public sealed class OracleDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

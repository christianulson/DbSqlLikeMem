namespace DbSqlLikeMem.Oracle;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Summary for OracleDataReaderMock.
/// PT: Resumo para OracleDataReaderMock.
/// </summary>
public sealed class OracleDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Represents Sql Azure Data Reader Mock.
/// PT: Representa Sql Azure Data leitor simulado.
/// </summary>
public sealed class SqlAzureDataReaderMock(
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}
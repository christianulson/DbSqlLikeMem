namespace DbSqlLikeMem.Npgsql;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Represents Npgsql Data Reader Mock.
/// PT: Representa Npgsql Data leitor simulado.
/// </summary>
public sealed class NpgsqlDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

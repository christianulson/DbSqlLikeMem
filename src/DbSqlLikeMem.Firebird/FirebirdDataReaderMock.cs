namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents the Firebird data reader mock used by provider commands.
/// PT: Representa o leitor de dados simulado do Firebird usado pelos comandos do provedor.
/// </summary>
public sealed class FirebirdDataReaderMock(
    IList<TableResultMock> tables
) : DbDataReaderMockBase(tables)
{
}


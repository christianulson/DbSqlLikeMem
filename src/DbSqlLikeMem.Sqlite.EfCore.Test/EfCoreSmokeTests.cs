namespace DbSqlLikeMem.Sqlite.EfCore.Test;

/// <summary>
/// EN: Executes shared EF Core smoke contract tests using the Sqlite provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de EF Core usando a fábrica de conexão do provedor Sqlite.
/// </summary>
public sealed class EfCoreSmokeTests(
    ITestOutputHelper helper
) : EfCoreSupportTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the Sqlite EF Core connection factory used by the shared contract tests.
    /// PT: Cria a fábrica de conexão EF Core de Sqlite usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemEfCoreConnectionFactory CreateFactory()
        => new SqliteEfCoreConnectionFactory();
}

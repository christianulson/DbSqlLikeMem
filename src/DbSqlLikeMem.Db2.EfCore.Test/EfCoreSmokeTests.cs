namespace DbSqlLikeMem.Db2.EfCore.Test;

/// <summary>
/// EN: Executes shared EF Core smoke contract tests using the Db2 provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de EF Core usando a fábrica de conexão do provedor Db2.
/// </summary>
public sealed class EfCoreSmokeTests(
    ITestOutputHelper helper
) : EfCoreSupportTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the Db2 EF Core connection factory used by the shared contract tests.
    /// PT: Cria a fábrica de conexão EF Core de Db2 usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemEfCoreConnectionFactory CreateFactory()
        => new Db2EfCoreConnectionFactory();
}

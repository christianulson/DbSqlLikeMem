namespace DbSqlLikeMem.SqlServer.EfCore.Test;

/// <summary>
/// EN: Executes shared EF Core smoke contract tests using the SqlServer provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de EF Core usando a fábrica de conexão do provedor SqlServer.
/// </summary>
public sealed class EfCoreSmokeTests(
    ITestOutputHelper helper
) : EfCoreSupportTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the SqlServer EF Core connection factory used by the shared contract tests.
    /// PT: Cria a fábrica de conexão EF Core de SqlServer usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemEfCoreConnectionFactory CreateFactory()
        => new SqlServerEfCoreConnectionFactory();
}

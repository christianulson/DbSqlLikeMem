namespace DbSqlLikeMem.Db2.EfCore.Test;

/// <summary>
/// EN: Executes shared EF Core smoke contract tests using the Db2 provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de EF Core usando a fábrica de conexão do provedor Db2.
/// </summary>
public sealed class EfCoreSmokeTests(
    ITestOutputHelper helper
) : EfCoreSmokeTestsBase(helper, static () => new Db2EfCoreConnectionFactory())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new DbSqlLikeMem.Db2.TestTools.Db2ProviderSqlDialect();
}

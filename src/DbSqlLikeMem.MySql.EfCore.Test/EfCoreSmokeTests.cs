namespace DbSqlLikeMem.MySql.EfCore.Test;

/// <summary>
/// EN: Executes shared EF Core smoke contract tests using the MySql provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de EF Core usando a fábrica de conexão do provedor MySql.
/// </summary>
public sealed class EfCoreSmokeTests(
    ITestOutputHelper helper
) : EfCoreSmokeTestsBase(helper, static () => new MySqlEfCoreConnectionFactory());

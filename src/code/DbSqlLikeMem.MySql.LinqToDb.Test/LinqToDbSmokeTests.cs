namespace DbSqlLikeMem.MySql.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the MySql provider connection factory.
/// PT-br: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor MySql.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSmokeTestsBase(helper, static () => new MySqlLinqToDbConnectionFactory())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new DbSqlLikeMem.MySql.TestTools.MySqlProviderSqlDialect();
}

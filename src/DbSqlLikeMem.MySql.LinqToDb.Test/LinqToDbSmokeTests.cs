namespace DbSqlLikeMem.MySql.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the MySql provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor MySql.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSupportTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the MySql LinqToDB connection factory used by shared contract tests.
    /// PT: Cria a fábrica de conexão LinqToDB de MySql usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemLinqToDbConnectionFactory CreateFactory()
        => new MySqlLinqToDbConnectionFactory();
}

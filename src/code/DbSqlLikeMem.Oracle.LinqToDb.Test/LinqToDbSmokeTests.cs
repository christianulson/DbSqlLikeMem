namespace DbSqlLikeMem.Oracle.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the Oracle provider connection factory.
/// PT-br: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor Oracle.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSupportTestsBase(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new DbSqlLikeMem.Oracle.TestTools.OracleProviderSqlDialect();

    /// <summary>
    /// EN: Creates the Oracle LinqToDB connection factory used by shared contract tests.
    /// PT-br: Cria a fábrica de conexão LinqToDB de Oracle usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemLinqToDbConnectionFactory CreateFactory()
        => new OracleLinqToDbConnectionFactory();
}

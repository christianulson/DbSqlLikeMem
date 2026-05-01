namespace DbSqlLikeMem.Sqlite.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the Sqlite provider connection factory.
/// PT-br: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor Sqlite.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSupportTestsBase(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new DbSqlLikeMem.Sqlite.TestTools.SqliteProviderSqlDialect();

    /// <summary>
    /// EN: Creates the Sqlite LinqToDB connection factory used by shared contract tests.
    /// PT-br: Cria a fábrica de conexão LinqToDB de Sqlite usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemLinqToDbConnectionFactory CreateFactory()
        => new SqliteLinqToDbConnectionFactory();
}

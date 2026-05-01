namespace DbSqlLikeMem.Firebird.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the Firebird provider connection factory.
/// PT-br: Executa testes compartilhados de contrato smoke de LinqToDB usando a fabrica de conexao do provedor Firebird.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSmokeTestsBase(helper, static () => new FirebirdLinqToDbConnectionFactory())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new FirebirdProviderSqlDialect();

    /// <inheritdoc />
    protected override string BuildPaginationQuery(string tableName, string orderByClause, int offset, int fetch) =>
        $"SELECT FIRST {fetch} SKIP {offset} id FROM {tableName} ORDER BY {orderByClause}";
}

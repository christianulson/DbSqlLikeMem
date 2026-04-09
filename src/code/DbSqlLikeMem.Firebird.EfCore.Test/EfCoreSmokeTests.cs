namespace DbSqlLikeMem.Firebird.EfCore.Test;

/// <summary>
/// EN: Executes shared EF Core smoke contract tests using the Firebird provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de EF Core usando a fabrica de conexao do provedor Firebird.
/// </summary>
public sealed class EfCoreSmokeTests(
    ITestOutputHelper helper
) : EfCoreSmokeTestsBase(helper, static () => new FirebirdEfCoreConnectionFactory())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new FirebirdProviderSqlDialect();

    /// <inheritdoc />
    protected override string BuildPaginationQuery(string tableName, string orderByClause, int offset, int fetch) =>
        $"SELECT FIRST {fetch} SKIP {offset} id FROM {tableName} ORDER BY {orderByClause}";
}

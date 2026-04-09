namespace DbSqlLikeMem.Npgsql.EfCore.Test;

/// <summary>
/// EN: Executes shared EF Core smoke contract tests using the Npgsql provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de EF Core usando a fábrica de conexão do provedor Npgsql.
/// </summary>
public sealed class EfCoreSmokeTests(
    ITestOutputHelper helper
) : EfCoreSmokeTestsBase(helper, static () => new NpgsqlEfCoreConnectionFactory())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new DbSqlLikeMem.Npgsql.TestTools.NpgsqlProviderSqlDialect();

    /// <inheritdoc />
    protected override string BuildPaginationQuery(string tableName, string orderByClause, int offset, int fetch) =>
        $"SELECT id FROM {tableName} ORDER BY {orderByClause} LIMIT {fetch} OFFSET {offset}";
}

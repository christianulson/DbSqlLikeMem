namespace DbSqlLikeMem.Npgsql.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the Npgsql provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor Npgsql.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSmokeTestsBase(helper, static () => new NpgsqlLinqToDbConnectionFactory())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new DbSqlLikeMem.Npgsql.TestTools.NpgsqlProviderSqlDialect();

    /// <inheritdoc />
    protected override string BuildPaginationQuery(string tableName, string orderByClause, int offset, int fetch) =>
        $"SELECT id FROM {tableName} ORDER BY {orderByClause} LIMIT {fetch} OFFSET {offset}";
}

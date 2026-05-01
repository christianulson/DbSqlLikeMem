namespace DbSqlLikeMem.Npgsql.Dapper.Test;

/// <summary>
/// EN: Provides provider-specific Dapper smoke coverage through the shared generic contract base.
/// PT-br: Fornece cobertura smoke específica do provedor de Dapper através da base genérica de contrato compartilhada.
/// </summary>
public sealed class DapperSmokeTests(
    ITestOutputHelper helper
) : DapperSmokeTestsBase<NpgsqlConnectionMock>(helper, static () => new NpgsqlConnectionMock())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new DbSqlLikeMem.Npgsql.TestTools.NpgsqlProviderSqlDialect();

    /// <inheritdoc />
    protected override string BuildPaginationQuery(string tableName, string orderByClause, int offset, int fetch) =>
        $"SELECT id FROM {tableName} ORDER BY {orderByClause} LIMIT {fetch} OFFSET {offset}";
}

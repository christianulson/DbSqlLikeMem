namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Validates Oracle-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de Oracle.
/// </summary>
public sealed class DbMockConnectionFactoryOracleTests(
        ITestOutputHelper helper
    ) : DbMockConnectionFactoryContractTestsBase(helper)
{
    /// <inheritdoc />
    protected override string ProviderHint => "Oracle";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(OracleDbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(OracleConnectionMock);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases => ["Oracle", "oracle", "ora", "or_acle", "  ORACLE  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateOracleWithTables(tableMappers);
}

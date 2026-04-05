namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Validates Firebird-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de Firebird.
/// </summary>
public sealed class DbMockConnectionFactoryFirebirdTests(
        ITestOutputHelper helper
    ) : DbMockConnectionFactoryContractTestsBase(helper)
{
    /// <inheritdoc />
    protected override string ProviderHint => "Firebird";

    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(FirebirdDbMock);

    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(FirebirdConnectionMock);

    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases =>
        ["Firebird", "firebird", "firebirdsql", "fire_bird", "fire-bird", "  FIREBIRD  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateFirebirdWithTables(tableMappers);
}



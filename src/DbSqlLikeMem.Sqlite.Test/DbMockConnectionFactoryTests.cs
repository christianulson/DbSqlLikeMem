namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Validates Sqlite-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de Sqlite.
/// </summary>
public sealed class DbMockConnectionFactoryTests : DbMockConnectionFactoryContractTestsBase
{
    /// <inheritdoc />
    protected override string ProviderHint => "Sqlite";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(SqliteDbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(SqliteConnectionMock);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases => ["Sqlite", "sqlite", "sqlite3", "sql_ite", "  SQLITE  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateSqliteWithTables(tableMappers);
}

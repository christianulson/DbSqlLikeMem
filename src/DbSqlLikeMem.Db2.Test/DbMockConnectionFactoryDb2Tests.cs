namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Validates Db2-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de Db2.
/// </summary>
public sealed class DbMockConnectionFactoryDb2Tests(
        ITestOutputHelper helper
    ) : DbMockConnectionFactoryContractTestsBase(helper) 
{
    /// <inheritdoc />
    protected override string ProviderHint => "Db2";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(Db2DbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(Db2ConnectionMock);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases => ["Db2", "db2", "db-2", "ibm-db2", "ibmdb2", "  DB2  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateDb2WithTables(tableMappers);
}

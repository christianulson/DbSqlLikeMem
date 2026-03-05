namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Validates Npgsql-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de Npgsql.
/// </summary>
public sealed class DbMockConnectionFactoryNpgsqlTests : DbMockConnectionFactoryContractTestsBase
{
    /// <inheritdoc />
    protected override string ProviderHint => "Npgsql";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(NpgsqlDbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(NpgsqlConnectionMock);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases =>
        ["Npgsql", "npgsql", "postgres", "postgresql", "post_gres", "post-gresql", "  POSTGRES  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateNpgsqlWithTables(tableMappers);
}

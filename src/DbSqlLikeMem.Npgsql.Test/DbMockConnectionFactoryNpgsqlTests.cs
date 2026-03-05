namespace DbSqlLikeMem.Npgsql.Test;

public sealed class DbMockConnectionFactoryNpgsqlTests : DbMockConnectionFactoryContractTestsBase
{
    protected override string ProviderHint => "Npgsql";
    protected override Type ExpectedDbType => typeof(NpgsqlDbMock);
    protected override Type ExpectedConnectionType => typeof(NpgsqlConnectionMock);
    protected override IReadOnlyList<string> ProviderAliases =>
        ["Npgsql", "npgsql", "postgres", "postgresql", "post_gres", "post-gresql", "  POSTGRES  "];

    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateNpgsqlWithTables(tableMappers);
}

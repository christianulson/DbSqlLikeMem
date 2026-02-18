namespace DbSqlLikeMem.Npgsql.Test;

public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new NpgsqlConnectionMock();
}

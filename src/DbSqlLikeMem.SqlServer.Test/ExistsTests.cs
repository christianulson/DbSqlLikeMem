namespace DbSqlLikeMem.SqlServer.Test;

public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new SqlServerConnectionMock();
}

namespace DbSqlLikeMem.Db2.Test;

public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new Db2ConnectionMock();
}

namespace DbSqlLikeMem.Oracle.Test;

public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new OracleConnectionMock();
}

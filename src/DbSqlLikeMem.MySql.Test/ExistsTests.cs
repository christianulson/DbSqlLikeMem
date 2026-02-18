namespace DbSqlLikeMem.MySql.Test;

public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new MySqlConnectionMock();
}

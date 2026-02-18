namespace DbSqlLikeMem.Sqlite.Test;

public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new SqliteConnectionMock();
}

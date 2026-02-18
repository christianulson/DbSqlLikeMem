namespace DbSqlLikeMem.SqlServer.Test;

public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<SqlServerDbMock, SqlServerMockException>(helper)
{
    protected override SqlServerDbMock CreateDb() => new();
}

namespace DbSqlLikeMem.Sqlite.Test;

public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<SqliteDbMock, SqliteMockException>(helper)
{
    protected override SqliteDbMock CreateDb() => new();
}

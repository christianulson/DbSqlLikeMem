namespace DbSqlLikeMem.MySql.Test;

public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<MySqlDbMock>(helper)
{
    protected override MySqlDbMock CreateDb() => new();
}

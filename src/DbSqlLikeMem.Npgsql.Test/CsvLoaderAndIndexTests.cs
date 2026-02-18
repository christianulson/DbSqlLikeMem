namespace DbSqlLikeMem.Npgsql.Test;

public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<NpgsqlDbMock>(helper)
{
    protected override NpgsqlDbMock CreateDb() => new();
}

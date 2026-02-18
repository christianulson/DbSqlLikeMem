namespace DbSqlLikeMem.Db2.Test;

public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<Db2DbMock>(helper)
{
    protected override Db2DbMock CreateDb() => new();
}

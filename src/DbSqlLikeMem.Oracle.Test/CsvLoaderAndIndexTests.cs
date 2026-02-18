namespace DbSqlLikeMem.Oracle.Test;

public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<OracleDbMock>(helper)
{
    protected override OracleDbMock CreateDb() => new();
}

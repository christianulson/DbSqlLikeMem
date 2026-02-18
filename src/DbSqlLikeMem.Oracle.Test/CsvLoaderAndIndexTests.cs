namespace DbSqlLikeMem.Oracle.Test;

public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<OracleDbMock, OracleMockException>(helper)
{
    protected override OracleDbMock CreateDb() => new();
}

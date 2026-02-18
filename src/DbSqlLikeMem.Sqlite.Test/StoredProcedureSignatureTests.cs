namespace DbSqlLikeMem.Sqlite.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new SqliteConnectionMock();
}

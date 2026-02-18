namespace DbSqlLikeMem.Sqlite.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<SqliteMockException>(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new SqliteConnectionMock();
}

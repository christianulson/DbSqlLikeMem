namespace DbSqlLikeMem.SqlServer.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<SqlServerMockException>(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new SqlServerConnectionMock();
}

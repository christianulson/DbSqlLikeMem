namespace DbSqlLikeMem.Db2.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new Db2ConnectionMock();
}

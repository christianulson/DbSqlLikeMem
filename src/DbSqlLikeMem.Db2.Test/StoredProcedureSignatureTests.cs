namespace DbSqlLikeMem.Db2.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<Db2MockException>(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new Db2ConnectionMock();
}

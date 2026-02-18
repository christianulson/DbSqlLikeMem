namespace DbSqlLikeMem.MySql.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<MySqlMockException>(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new MySqlConnectionMock();
}

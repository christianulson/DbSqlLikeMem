namespace DbSqlLikeMem.Npgsql.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new NpgsqlConnectionMock();
}

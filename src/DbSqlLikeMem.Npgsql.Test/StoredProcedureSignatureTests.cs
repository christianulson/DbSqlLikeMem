namespace DbSqlLikeMem.Npgsql.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<NpgsqlMockException>(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new NpgsqlConnectionMock();
}

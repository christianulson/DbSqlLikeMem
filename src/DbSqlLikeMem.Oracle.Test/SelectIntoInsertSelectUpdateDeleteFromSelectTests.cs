namespace DbSqlLikeMem.Oracle.Test;

public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<OracleDbMock>(helper)
{
    protected override OracleDbMock CreateDb() => new();

    protected override int ExecuteNonQuery(
        OracleDbMock db,
        string sql)
    {
        using var c = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }
}

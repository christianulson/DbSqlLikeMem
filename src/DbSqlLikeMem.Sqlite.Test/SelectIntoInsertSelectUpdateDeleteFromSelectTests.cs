namespace DbSqlLikeMem.Sqlite.Test;

public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<SqliteDbMock>(helper)
{
    protected override SqliteDbMock CreateDb() => new();

    protected override int ExecuteNonQuery(
        SqliteDbMock db,
        string sql)
    {
        using var c = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }
}

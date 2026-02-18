namespace DbSqlLikeMem.Db2.Test;

public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<Db2DbMock>(helper)
{
    protected override Db2DbMock CreateDb() => new();

    protected override int ExecuteNonQuery(
        Db2DbMock db,
        string sql)
    {
        using var c = new Db2ConnectionMock(db);
        using var cmd = new Db2CommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }
}

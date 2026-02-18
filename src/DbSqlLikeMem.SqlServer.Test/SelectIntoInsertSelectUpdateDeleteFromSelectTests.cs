namespace DbSqlLikeMem.SqlServer.Test;

public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<SqlServerDbMock>(helper)
{
    protected override SqlServerDbMock CreateDb() => new();

    protected override int ExecuteNonQuery(
        SqlServerDbMock db,
        string sql)
    {
        using var c = new SqlServerConnectionMock(db);
        using var cmd = new SqlServerCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }

    protected override string DeleteJoinDerivedSelectSql
        => "DELETE u FROM users u JOIN (SELECT id FROM users WHERE tenantid = 10) s ON s.id = u.id";
}

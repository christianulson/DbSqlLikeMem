namespace DbSqlLikeMem.MySql.Test;

public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<MySqlDbMock>(helper)
{
    protected override MySqlDbMock CreateDb() => new();

    protected override int ExecuteNonQuery(
        MySqlDbMock db,
        string sql)
    {
        using var c = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }

    protected override string DeleteJoinDerivedSelectSql
        => "DELETE u FROM users u JOIN (SELECT id FROM users WHERE tenantid = 10) s ON s.id = u.id";
}

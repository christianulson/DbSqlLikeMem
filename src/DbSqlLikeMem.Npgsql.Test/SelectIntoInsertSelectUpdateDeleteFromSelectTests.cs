namespace DbSqlLikeMem.Npgsql.Test;

public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<NpgsqlDbMock>(helper)
{
    protected override NpgsqlDbMock CreateDb() => new();

    protected override int ExecuteNonQuery(
        NpgsqlDbMock db,
        string sql)
    {
        using var c = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }
}

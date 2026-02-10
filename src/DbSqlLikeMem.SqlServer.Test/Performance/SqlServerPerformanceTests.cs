namespace DbSqlLikeMem.SqlServer.Test.Performance;

public sealed class SqlServerPerformanceTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _connection;

    public SqlServerPerformanceTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
        db.AddTable("Users", new ColumnDictionary
        {
            { "Id", new(0, DbType.Int32, false) },
            { "Name", new(1, DbType.String, false) },
            { "Email", new(2, DbType.String, true) }
        });

        _connection = new SqlServerConnectionMock(db);
        _connection.Open();
    }

    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_crud_baseline_metrics()
    {
        const int totalRows = 2000;
        const int sampledReads = 1000;

        using var command = new SqlServerCommandMock(_connection);

        var insertElapsedMs = Measure(() =>
        {
            for (var i = 1; i <= totalRows; i++)
            {
                command.CommandText = $"INSERT INTO Users (Id, Name, Email) VALUES ({i}, 'User {i}', 'user{i}@mail.com')";
                Assert.Equal(1, command.ExecuteNonQuery());
            }
        });

        var readElapsedMs = Measure(() =>
        {
            for (var i = 1; i <= sampledReads; i++)
            {
                var userId = (i % totalRows) + 1;
                command.CommandText = $"SELECT Id, Name, Email FROM Users WHERE Id = {userId}";
                using var reader = command.ExecuteReader();
                Assert.True(reader.Read());
            }
        });

        var updateElapsedMs = Measure(() =>
        {
            for (var i = 1; i <= totalRows; i++)
            {
                command.CommandText = $"UPDATE Users SET Name = 'Updated {i}' WHERE Id = {i}";
                Assert.Equal(1, command.ExecuteNonQuery());
            }
        });

        var deleteElapsedMs = Measure(() =>
        {
            for (var i = 1; i <= totalRows; i++)
            {
                command.CommandText = $"DELETE FROM Users WHERE Id = {i}";
                Assert.Equal(1, command.ExecuteNonQuery());
            }
        });

        Console.WriteLine($"[SqlServer][Performance] Inserts: {totalRows} in {insertElapsedMs}ms ({OpsPerSecond(totalRows, insertElapsedMs):F2} ops/s)");
        Console.WriteLine($"[SqlServer][Performance] Reads: {sampledReads} in {readElapsedMs}ms ({OpsPerSecond(sampledReads, readElapsedMs):F2} ops/s)");
        Console.WriteLine($"[SqlServer][Performance] Updates: {totalRows} in {updateElapsedMs}ms ({OpsPerSecond(totalRows, updateElapsedMs):F2} ops/s)");
        Console.WriteLine($"[SqlServer][Performance] Deletes: {totalRows} in {deleteElapsedMs}ms ({OpsPerSecond(totalRows, deleteElapsedMs):F2} ops/s)");

        Assert.Empty(_connection.GetTable("Users"));
    }

    private static long Measure(Action action)
    {
        var watch = Stopwatch.StartNew();
        action();
        watch.Stop();
        return watch.ElapsedMilliseconds;
    }

    private static double OpsPerSecond(int operationCount, long elapsedMs)
    {
        if (elapsedMs <= 0)
            return operationCount;

        return operationCount / (elapsedMs / 1000d);
    }

    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}

using System.Diagnostics;

namespace DbSqlLikeMem.Db2.Test.Performance;

/// <summary>
/// Provides performance tests for CRUD operations on a mock Db2 database using the xUnit testing framework.
/// </summary>
/// <remarks>This class measures and reports baseline metrics for insert, read, update, and delete operations
/// against a simulated Db2 environment. The tests are intended to assess the relative performance of database
/// operations and are categorized as performance tests. Results are output to the test log for analysis. This class is
/// sealed and cannot be inherited.</remarks>
public sealed class Db2PerformanceTests : XUnitTestBase
{
    private readonly Db2ConnectionMock _connection;

    /// <summary>
    /// Initializes a new instance of the Db2PerformanceTests class using the specified test output helper.
    /// </summary>
    /// <remarks>This constructor sets up a mock Db2 database with a 'Users' table and establishes a
    /// connection for performance testing scenarios. The connection is opened automatically upon
    /// initialization.</remarks>
    /// <param name="helper">The test output helper used to capture and display test output during execution. Cannot be null.</param>
    public Db2PerformanceTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new Db2DbMock();
        db.AddTable("Users", new ColumnDictionary
        {
            { "Id", new(0, DbType.Int32, false) },
            { "Name", new(1, DbType.String, false) },
            { "Email", new(2, DbType.String, true) }
        });

        _connection = new Db2ConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// Verifies that baseline performance metrics for CRUD operations on the Users table are reported and meet expected
    /// criteria.
    /// </summary>
    /// <remarks>This test measures the execution time for insert, read, update, and delete operations using a
    /// mock Db2 command, and outputs the operations per second for each. It asserts that all operations complete
    /// successfully and that the Users table is empty after deletions. Use this test to assess the relative performance
    /// of basic database operations in the test environment.</remarks>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_crud_baseline_metrics()
    {
        const int totalRows = 2000;
        const int sampledReads = 1000;

        using var command = new Db2CommandMock(_connection);

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

        Console.WriteLine($"[Db2][Performance] Inserts: {totalRows} in {insertElapsedMs}ms ({OpsPerSecond(totalRows, insertElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Db2][Performance] Reads: {sampledReads} in {readElapsedMs}ms ({OpsPerSecond(sampledReads, readElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Db2][Performance] Updates: {totalRows} in {updateElapsedMs}ms ({OpsPerSecond(totalRows, updateElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Db2][Performance] Deletes: {totalRows} in {deleteElapsedMs}ms ({OpsPerSecond(totalRows, deleteElapsedMs):F2} ops/s)");

        Assert.Empty(_connection.GetTable("users"));
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

    /// <summary>
    /// Releases the unmanaged resources used by the object and optionally releases the managed resources.
    /// </summary>
    /// <remarks>This method is called by both the public Dispose() method and the finalizer. When disposing
    /// is true, managed resources such as the underlying connection are released. When disposing is false, only
    /// unmanaged resources are released.</remarks>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}

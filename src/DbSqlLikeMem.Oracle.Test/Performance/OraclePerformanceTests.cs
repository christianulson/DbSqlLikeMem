using System.Diagnostics;

namespace DbSqlLikeMem.Oracle.Test.Performance;

/// <summary>
/// Provides performance tests for Oracle database CRUD operations using mock implementations. This class measures and
/// reports baseline metrics for insert, read, update, and delete operations within a controlled test environment.
/// </summary>
/// <remarks>Use this class to evaluate the relative performance of Oracle database operations in unit tests. The
/// tests are executed against mock database and connection objects, allowing for repeatable and isolated performance
/// measurements without requiring a live Oracle instance. Results are output to the test log for analysis. This class
/// is intended for internal testing scenarios and is not thread-safe.</remarks>
public sealed class OraclePerformanceTests : XUnitTestBase
{
    private readonly OracleConnectionMock _connection;

    /// <summary>
    /// Initializes a new instance of the OraclePerformanceTests class using the specified test output helper.
    /// </summary>
    /// <remarks>This constructor sets up a mock Oracle database and opens a connection for use in performance
    /// testing scenarios. The database schema includes a 'Users' table with sample columns, allowing tests to interact
    /// with a realistic mock environment.</remarks>
    /// <param name="helper">The test output helper used to capture and display test output during execution.</param>
    public OraclePerformanceTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new OracleDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false) ,
            new("Name", DbType.String, false) ,
            new("Email", DbType.String, true)
        ]);

        _connection = new OracleConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// Verifies that baseline performance metrics for CRUD operations on the Users table are reported and meet expected
    /// criteria using an Oracle database mock.
    /// </summary>
    /// <remarks>This test measures the execution time for insert, read, update, and delete operations on a
    /// sample dataset, outputting performance statistics to the console. It asserts that all operations complete
    /// successfully and that the Users table is empty after deletions. Use this test to monitor and validate the
    /// performance characteristics of basic database operations in the Oracle mock environment.</remarks>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_crud_baseline_metrics()
    {
        const int totalRows = 2000;
        const int sampledReads = 1000;

        using var command = new OracleCommandMock(_connection);

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

        Console.WriteLine($"[Oracle][Performance] Inserts: {totalRows} in {insertElapsedMs}ms ({OpsPerSecond(totalRows, insertElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Oracle][Performance] Reads: {sampledReads} in {readElapsedMs}ms ({OpsPerSecond(sampledReads, readElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Oracle][Performance] Updates: {totalRows} in {updateElapsedMs}ms ({OpsPerSecond(totalRows, updateElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Oracle][Performance] Deletes: {totalRows} in {deleteElapsedMs}ms ({OpsPerSecond(totalRows, deleteElapsedMs):F2} ops/s)");

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

    /// <summary>
    /// Releases the unmanaged resources used by the object and optionally releases the managed resources.
    /// </summary>
    /// <remarks>This method is called by both the public Dispose() method and the finalizer. When disposing
    /// is true, managed resources such as connections are released. Override this method to release additional
    /// resources as needed.</remarks>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}

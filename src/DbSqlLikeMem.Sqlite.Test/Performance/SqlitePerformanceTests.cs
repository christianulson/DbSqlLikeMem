using System.Diagnostics;

namespace DbSqlLikeMem.Sqlite.Test.Performance;

/// <summary>
/// Provides performance tests for basic CRUD operations using a mock SQLite database. Used to benchmark and validate
/// the efficiency of insert, read, update, and delete operations within a controlled test environment.
/// </summary>
/// <remarks>This class is intended for use with xUnit test frameworks and relies on mock implementations of
/// SQLite components to simulate database interactions. The tests measure operation timings and report metrics to
/// assist in identifying performance bottlenecks. Results are output to the test log for analysis. Thread safety is not
/// guaranteed; tests should be run sequentially.</remarks>
public sealed class SqlitePerformanceTests : XUnitTestBase
{
    private readonly SqliteConnectionMock _connection;

    /// <summary>
    /// Initializes a new instance of the SqlitePerformanceTests class using the specified test output helper.
    /// </summary>
    /// <remarks>This constructor sets up an in-memory SQLite database mock with a 'Users' table and opens a
    /// connection for use in performance testing scenarios. The database schema is predefined to facilitate consistent
    /// test results.</remarks>
    /// <param name="helper">The test output helper used to capture and display test output during performance tests.</param>
    public SqlitePerformanceTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqliteDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false) ,
            new("Name", DbType.String, false) ,
            new("Email", DbType.String, true)
        ]);

        _connection = new SqliteConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// Verifies that baseline performance metrics for CRUD operations on the Users table are reported and meet expected
    /// criteria.
    /// </summary>
    /// <remarks>This test measures the execution time for insert, read, update, and delete operations using a
    /// mock SQLite command, and outputs the operations per second for each. It asserts that all rows are correctly
    /// inserted, read, updated, and deleted, ensuring the Users table is empty at the end. Use this test to monitor and
    /// validate the performance characteristics of basic database operations.</remarks>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_crud_baseline_metrics()
    {
        const int totalRows = 2000;
        const int sampledReads = 1000;

        var insertStatements = new string[totalRows];
        var updateStatements = new string[totalRows];
        var deleteStatements = new string[totalRows];
        var readStatements = new string[sampledReads];

        for (var i = 1; i <= totalRows; i++)
        {
            var rowIndex = i - 1;
            insertStatements[rowIndex] = $"INSERT INTO Users (Id, Name, Email) VALUES ({i}, 'User {i}', 'user{i}@mail.com')";
            updateStatements[rowIndex] = $"UPDATE Users SET Name = 'Updated {i}' WHERE Id = {i}";
            deleteStatements[rowIndex] = $"DELETE FROM Users WHERE Id = {i}";
        }

        for (var i = 1; i <= sampledReads; i++)
        {
            var userId = (i % totalRows) + 1;
            readStatements[i - 1] = $"SELECT Id, Name, Email FROM Users WHERE Id = {userId}";
        }

        using var command = new SqliteCommandMock(_connection);

        var insertedRows = 0;
        var insertElapsedMs = Measure(() =>
        {
            for (var i = 0; i < insertStatements.Length; i++)
            {
                command.CommandText = insertStatements[i];
                insertedRows += command.ExecuteNonQuery();
            }
        });
        Assert.Equal(totalRows, insertedRows);

        var successfulReads = 0;
        var readElapsedMs = Measure(() =>
        {
            for (var i = 0; i < readStatements.Length; i++)
            {
                command.CommandText = readStatements[i];
                using var reader = command.ExecuteReader();
                if (reader.Read())
                    successfulReads++;
            }
        });
        Assert.Equal(sampledReads, successfulReads);

        var updatedRows = 0;
        var updateElapsedMs = Measure(() =>
        {
            for (var i = 0; i < updateStatements.Length; i++)
            {
                command.CommandText = updateStatements[i];
                updatedRows += command.ExecuteNonQuery();
            }
        });
        Assert.Equal(totalRows, updatedRows);

        var deletedRows = 0;
        var deleteElapsedMs = Measure(() =>
        {
            for (var i = 0; i < deleteStatements.Length; i++)
            {
                command.CommandText = deleteStatements[i];
                deletedRows += command.ExecuteNonQuery();
            }
        });
        Assert.Equal(totalRows, deletedRows);

        Console.WriteLine($"[Sqlite][Performance] Inserts: {totalRows} in {insertElapsedMs}ms ({OpsPerSecond(totalRows, insertElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Sqlite][Performance] Reads: {sampledReads} in {readElapsedMs}ms ({OpsPerSecond(sampledReads, readElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Sqlite][Performance] Updates: {totalRows} in {updateElapsedMs}ms ({OpsPerSecond(totalRows, updateElapsedMs):F2} ops/s)");
        Console.WriteLine($"[Sqlite][Performance] Deletes: {totalRows} in {deleteElapsedMs}ms ({OpsPerSecond(totalRows, deleteElapsedMs):F2} ops/s)");

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
    /// Releases the unmanaged resources used by the class and optionally releases the managed resources.
    /// </summary>
    /// <remarks>This method overrides Dispose to ensure that all resources associated with the class,
    /// including any managed connections, are properly released. Call Dispose when you are finished using the object to
    /// free resources promptly.</remarks>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}

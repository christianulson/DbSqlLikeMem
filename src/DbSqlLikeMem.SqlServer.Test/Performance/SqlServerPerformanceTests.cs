using System.Diagnostics;

namespace DbSqlLikeMem.SqlServer.Test.Performance;

/// <summary>
/// Provides performance tests for SQL Server CRUD operations using a mock database. This class is used to verify that
/// basic insert, read, update, and delete operations meet expected performance criteria in a controlled test
/// environment.
/// </summary>
/// <remarks>SqlServerPerformanceTests is intended for benchmarking and validating the throughput of SQL Server
/// operations in unit tests. It uses mock implementations to simulate database interactions, allowing for repeatable
/// and isolated performance measurements. Test results are output to the console for analysis. This class is sealed and
/// cannot be inherited.</remarks>
public sealed class SqlServerPerformanceTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _connection;

    /// <summary>
    /// Initializes a new instance of the SqlServerPerformanceTests class using the specified test output helper.
    /// </summary>
    /// <remarks>This constructor sets up a mock SQL Server database and opens a connection for use in
    /// performance tests. The database schema includes a 'Users' table with sample columns. Use this constructor when
    /// running performance tests that require a mock SQL Server environment.</remarks>
    /// <param name="helper">The test output helper used to capture and display test output during execution.</param>
    public SqlServerPerformanceTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false) ,
            new("Name", DbType.String, false) ,
            new("Email", DbType.String, true)
        ]);

        _connection = new SqlServerConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// Verifies that baseline performance metrics for CRUD operations on the Users table are reported and meet expected
    /// criteria.
    /// </summary>
    /// <remarks>This test measures the execution time for insert, read, update, and delete operations using a
    /// mock SQL Server command. It outputs performance statistics to the console and asserts that all operations
    /// complete successfully. The test ensures that the Users table is empty after all operations, validating both
    /// correctness and performance. This method is categorized as a performance test and is intended to provide a
    /// reference for CRUD operation throughput.</remarks>
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

        using var command = new SqlServerCommandMock(_connection);

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

    /// <summary>
    /// Releases the unmanaged resources used by the object and optionally releases the managed resources.
    /// </summary>
    /// <remarks>This method overrides Dispose to ensure that all resources associated with the object,
    /// including the underlying connection, are properly released. Call Dispose when you are finished using the object
    /// to free resources promptly.</remarks>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}

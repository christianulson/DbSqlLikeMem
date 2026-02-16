using System.Diagnostics;

namespace DbSqlLikeMem.Npgsql.Test.Performance;

/// <summary>
/// Provides performance tests for PostgreSQL CRUD operations using mock database components. Designed to measure and
/// report baseline metrics for insert, read, update, and delete operations within a controlled test environment.
/// </summary>
/// <remarks>This class is intended for use with xUnit test frameworks and leverages mock implementations of
/// Npgsql database objects to simulate PostgreSQL interactions. The tests focus on evaluating operation throughput and
/// latency, and are categorized under performance testing. Results are output to the test log for analysis. Thread
/// safety and parallel execution are not guaranteed; each test instance manages its own mock connection.</remarks>
public sealed class PostgreSqlPerformanceTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _connection;

    /// <summary>
    /// Initializes a new instance of the PostgreSqlPerformanceTests class using the specified test output helper.
    /// </summary>
    /// <remarks>This constructor sets up an in-memory mock PostgreSQL database with a 'Users' table and opens
    /// a mock connection for use in performance tests. The database schema is predefined to facilitate consistent test
    /// scenarios.</remarks>
    /// <param name="helper">The test output helper used to capture and display test output during execution. Cannot be null.</param>
    public PostgreSqlPerformanceTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new NpgsqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false) ,
            new("Name", DbType.String, false) ,
            new("Email", DbType.String, true)
        ]);

        _connection = new NpgsqlConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// Verifies that baseline performance metrics for CRUD operations on the Users table are reported correctly.
    /// </summary>
    /// <remarks>This test measures the execution time and throughput of insert, read, update, and delete
    /// operations using a mock PostgreSQL command. The results are output to the console for each operation. The test
    /// ensures that all inserted rows are deleted at the end, confirming data integrity. This method is categorized as
    /// a performance test and is intended to provide a reference for CRUD operation benchmarks.</remarks>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_crud_baseline_metrics()
    {
        const int totalRows = 2000;
        const int sampledReads = 1000;

        using var command = new NpgsqlCommandMock(_connection);

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

        Console.WriteLine($"[PostgreSql][Performance] Inserts: {totalRows} in {insertElapsedMs}ms ({OpsPerSecond(totalRows, insertElapsedMs):F2} ops/s)");
        Console.WriteLine($"[PostgreSql][Performance] Reads: {sampledReads} in {readElapsedMs}ms ({OpsPerSecond(sampledReads, readElapsedMs):F2} ops/s)");
        Console.WriteLine($"[PostgreSql][Performance] Updates: {totalRows} in {updateElapsedMs}ms ({OpsPerSecond(totalRows, updateElapsedMs):F2} ops/s)");
        Console.WriteLine($"[PostgreSql][Performance] Deletes: {totalRows} in {deleteElapsedMs}ms ({OpsPerSecond(totalRows, deleteElapsedMs):F2} ops/s)");

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
    /// <remarks>This method should be called when the object is no longer needed to ensure that all resources
    /// are properly released. Overrides Dispose to release additional resources held by the derived class.</remarks>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}

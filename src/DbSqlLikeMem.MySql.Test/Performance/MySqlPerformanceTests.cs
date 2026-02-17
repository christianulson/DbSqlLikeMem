using System.Diagnostics;

namespace DbSqlLikeMem.MySql.Test.Performance;

/// <summary>
/// Provides performance tests for MySQL CRUD operations using mock database components. This class is intended for
/// benchmarking and validating the efficiency of basic database actions within a controlled test environment.
/// </summary>
/// <remarks>MySqlPerformanceTests measures the execution time of insert, read, update, and delete operations on a
/// mock 'Users' table. The results are output to the console for analysis. These tests are categorized as performance
/// tests and are not intended for functional validation of MySQL features. The class is sealed and should not be
/// inherited.</remarks>
public sealed class MySqlPerformanceTests : XUnitTestBase
{
    private readonly MySqlConnectionMock _connection;

    /// <summary>
    /// Initializes a new instance of the MySqlPerformanceTests class using the specified test output helper.
    /// </summary>
    /// <remarks>This constructor sets up a mock MySQL database and opens a mock connection for use in
    /// performance testing scenarios. The database schema includes a 'Users' table with sample columns. Use this
    /// constructor when running tests that require a preconfigured mock database environment.</remarks>
    /// <param name="helper">The test output helper used to capture and display test output during execution.</param>
    public MySqlPerformanceTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false) ,
            new("Name", DbType.String, false) ,
            new("Email", DbType.String, true)
        ]);

        _connection = new MySqlConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// Verifies that baseline performance metrics for CRUD operations on the Users table are reported and meet expected
    /// criteria.
    /// </summary>
    /// <remarks>This test measures the execution time for insert, read, update, and delete operations using a
    /// mock MySQL command, and outputs the operations per second for each. It ensures that all rows are correctly
    /// inserted, read, updated, and deleted, and that the Users table is empty after the operations. The test is
    /// categorized under performance and is intended to provide a reference for CRUD operation throughput.</remarks>
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

        using var command = new MySqlCommandMock(_connection);

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

        Console.WriteLine($"[MySql][Performance] Inserts: {totalRows} in {insertElapsedMs}ms ({OpsPerSecond(totalRows, insertElapsedMs):F2} ops/s)");
        Console.WriteLine($"[MySql][Performance] Reads: {sampledReads} in {readElapsedMs}ms ({OpsPerSecond(sampledReads, readElapsedMs):F2} ops/s)");
        Console.WriteLine($"[MySql][Performance] Updates: {totalRows} in {updateElapsedMs}ms ({OpsPerSecond(totalRows, updateElapsedMs):F2} ops/s)");
        Console.WriteLine($"[MySql][Performance] Deletes: {totalRows} in {deleteElapsedMs}ms ({OpsPerSecond(totalRows, deleteElapsedMs):F2} ops/s)");

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
    /// <remarks>This method overrides Dispose to ensure that all resources associated with the connection are
    /// properly released. Derived classes should call this method when disposing their own resources.</remarks>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}

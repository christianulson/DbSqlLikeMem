using System.Diagnostics;

namespace DbSqlLikeMem.Npgsql.Test.Performance;

/// <summary>
/// EN: Covers PostgreSQL CRUD performance scenarios against the mock provider.
/// PT-br: Cobre cenarios de performance CRUD do PostgreSQL contra o provedor mock.
/// </summary>
/// <remarks>
/// EN: The suite measures insert, read, update, and delete throughput in a controlled test environment.
/// PT-br: A suite mede o throughput de insert, read, update e delete em um ambiente de teste controlado.
/// </remarks>
public sealed class PostgreSqlPerformanceTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _connection;

    /// <summary>
    /// EN: Creates the PostgreSQL performance test fixture with a seeded Users table.
    /// PT-br: Cria a fixture de testes de performance do PostgreSQL com a tabela Users semeada.
    /// </summary>
    /// <param name="helper">EN: The xUnit output helper. PT-br: O helper de saida do xUnit.</param>
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

        using var command = new NpgsqlCommandMock(_connection);

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

        Console.WriteLine($"[PostgreSql][Performance] Inserts: {totalRows} in {insertElapsedMs}ms ({OpsPerSecond(totalRows, insertElapsedMs):F2} ops/s, {OpsAVG(totalRows, insertElapsedMs):F2} ms/avg)");
        Console.WriteLine($"[PostgreSql][Performance] Reads: {sampledReads} in {readElapsedMs}ms ({OpsPerSecond(sampledReads, readElapsedMs):F2} ops/s, {OpsAVG(sampledReads, readElapsedMs):F2} ms/avg)");
        Console.WriteLine($"[PostgreSql][Performance] Updates: {totalRows} in {updateElapsedMs}ms ({OpsPerSecond(totalRows, updateElapsedMs):F2} ops/s, {OpsAVG(totalRows, updateElapsedMs):F2} ms/avg)");
        Console.WriteLine($"[PostgreSql][Performance] Deletes: {totalRows} in {deleteElapsedMs}ms ({OpsPerSecond(totalRows, deleteElapsedMs):F2} ops/s, {OpsAVG(totalRows, deleteElapsedMs):F2} ms/avg)");

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

    private static double OpsAVG(int operationCount, long elapsedMs)
    {
        if (operationCount <= 0)
            return elapsedMs;

        return (double)elapsedMs / operationCount;
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

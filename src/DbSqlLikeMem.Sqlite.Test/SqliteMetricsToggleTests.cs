using DbSqlLikeMem.Sqlite;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Verifies SQLite connections can disable diagnostic metric collection without changing command results.
/// PT: Verifica se conexoes SQLite podem desabilitar a coleta de metricas diagnosticas sem alterar o resultado dos comandos.
/// </summary>
public sealed class SqliteMetricsToggleTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies disabled metrics do not accumulate counters while SQL statements still execute normally.
    /// PT: Verifica se metricas desativadas nao acumulam contadores enquanto os comandos SQL continuam executando normalmente.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void DisabledMetrics_ShouldNotAccumulateCommandCounters()
    {
        var db = new SqliteDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqliteConnectionMock(db);
        connection.Metrics.Enabled = false;
        connection.CaptureExecutionPlans = false;
        connection.CaptureAffectedRowSnapshots = false;
        connection.Open();

        using (var insert = new SqliteCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            var rowsAffected = insert.ExecuteNonQuery();
            Assert.Equal(1, rowsAffected);
        }

        using (var count = new SqliteCommandMock(connection))
        {
            count.CommandText = "SELECT COUNT(*) FROM Users";
            var rowCount = Convert.ToInt32(count.ExecuteScalar(), CultureInfo.InvariantCulture);
            Assert.Equal(1, rowCount);
        }

        Assert.Equal(0, connection.Metrics.Inserts);
        Assert.Equal(0, connection.Metrics.Selects);
        Assert.Equal(0, connection.Metrics.NonQueryStatements);
        Assert.Equal(TimeSpan.Zero, connection.Metrics.Elapsed);
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Verifies batch non-query execution still persists rows when diagnostic metrics are disabled.
    /// PT: Verifica se a execucao batch non-query ainda persiste linhas quando as metricas diagnosticas estao desativadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void DisabledMetrics_ShouldNotBreakSqliteBatchExecution()
    {
        var db = new SqliteDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqliteConnectionMock(db);
        connection.Metrics.Enabled = false;
        connection.CaptureExecutionPlans = false;
        connection.CaptureAffectedRowSnapshots = false;
        connection.Open();

        using var batch = new SqliteBatchMock(connection);
        batch.BatchCommands.Add(new SqliteBatchCommandMock
        {
            CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')"
        });
        batch.BatchCommands.Add(new SqliteBatchCommandMock
        {
            CommandText = "INSERT INTO Users (Id, Name) VALUES (2, 'Beto')"
        });

        var rowsAffected = batch.ExecuteNonQuery();

        Assert.Equal(2, rowsAffected);
        Assert.Equal(2, connection.GetTable("Users").Count);
        Assert.Equal(0, connection.Metrics.BatchNonQueryCommands);
        Assert.Equal(0, connection.Metrics.BatchMaterializations);
        Assert.Equal(0, connection.Metrics.NonQueryStatements);
    }
#endif
}

using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace DbSqlLikeMem;

internal static class BatchCommandExecutionRunner
{
    public static void ExecuteIntoTables(
        DbConnectionMockBase connection,
        DbCommand command,
        ICollection<TableResultMock> tables,
        Func<DbDataReader> executeReader)
    {
        var startedAt = Stopwatch.GetTimestamp();
        connection.Metrics.IncrementBatchReaderCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Reader}{command.CommandType}");
        try
        {
            using var reader = executeReader();
            var stats = BatchReaderResultCollector.CollectAllResultSets(reader, tables);
            connection.Metrics.IncrementBatchResultTables(stats.TableCount);
            connection.Metrics.IncrementBatchRowsReturned(stats.RowCount);
            connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.Reader, Stopwatch.GetElapsedTime(startedAt).Ticks);
        }
        catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
        {
            connection.Metrics.IncrementBatchReaderFallbackToNonQuery();
            connection.Metrics.IncrementBatchNonQueryCommand();
            connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.FallbackNonQuery}{command.CommandType}");
            try
            {
                command.ExecuteNonQuery();
                connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.FallbackNonQuery, Stopwatch.GetElapsedTime(startedAt).Ticks);
            }
            catch
            {
                connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.FallbackNonQuery);
                connection.Metrics.IncrementBatchException();
                throw;
            }
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.Reader);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }

    public static async Task ExecuteIntoTablesAsync(
        DbConnectionMockBase connection,
        DbCommand command,
        ICollection<TableResultMock> tables,
        Func<CancellationToken, Task<DbDataReader>> executeReaderAsync,
        CancellationToken cancellationToken)
    {
        var startedAt = Stopwatch.GetTimestamp();
        connection.Metrics.IncrementBatchReaderCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Reader}{command.CommandType}");
        try
        {
            using var reader = await executeReaderAsync(cancellationToken).ConfigureAwait(false);
            var stats = BatchReaderResultCollector.CollectAllResultSets(reader, tables);
            connection.Metrics.IncrementBatchResultTables(stats.TableCount);
            connection.Metrics.IncrementBatchRowsReturned(stats.RowCount);
            connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.Reader, Stopwatch.GetElapsedTime(startedAt).Ticks);
        }
        catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
        {
            connection.Metrics.IncrementBatchReaderFallbackToNonQuery();
            connection.Metrics.IncrementBatchNonQueryCommand();
            connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.FallbackNonQuery}{command.CommandType}");
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.FallbackNonQuery, Stopwatch.GetElapsedTime(startedAt).Ticks);
            }
            catch
            {
                connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.FallbackNonQuery);
                connection.Metrics.IncrementBatchException();
                throw;
            }
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.Reader);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }
}

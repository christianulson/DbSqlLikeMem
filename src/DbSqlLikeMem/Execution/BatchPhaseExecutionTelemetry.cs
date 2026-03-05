using System.Diagnostics;
using System.Threading.Tasks;

namespace DbSqlLikeMem;

internal static class BatchPhaseExecutionTelemetry
{
    public static void Execute(
        DbConnectionMockBase connection,
        string phase,
        Action operation)
    {
        Execute(
            connection,
            phase,
            () =>
            {
                operation();
                return 0;
            });
    }

    public static T Execute<T>(
        DbConnectionMockBase connection,
        string phase,
        Func<T> operation)
    {
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            var result = operation();
            connection.Metrics.IncrementBatchPhaseElapsedTicks(phase, Stopwatch.GetElapsedTime(startedAt).Ticks);
            return result;
        }
        catch (global::System.OperationCanceledException)
        {
            connection.Metrics.IncrementBatchPhaseCancellation(phase);
            connection.Metrics.IncrementBatchCancellation();
            throw;
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(phase);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }

    public static Task ExecuteAsync(
        DbConnectionMockBase connection,
        string phase,
        Func<Task> operationAsync)
    {
        return ExecuteAsync(
            connection,
            phase,
            async () =>
            {
                await operationAsync().ConfigureAwait(false);
                return 0;
            });
    }

    public static async Task<T> ExecuteAsync<T>(
        DbConnectionMockBase connection,
        string phase,
        Func<Task<T>> operationAsync)
    {
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            var result = await operationAsync().ConfigureAwait(false);
            connection.Metrics.IncrementBatchPhaseElapsedTicks(phase, Stopwatch.GetElapsedTime(startedAt).Ticks);
            return result;
        }
        catch (global::System.OperationCanceledException)
        {
            connection.Metrics.IncrementBatchPhaseCancellation(phase);
            connection.Metrics.IncrementBatchCancellation();
            throw;
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(phase);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }
}

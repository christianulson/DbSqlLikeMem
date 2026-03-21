namespace DbSqlLikeMem;

internal static class BatchPhaseExecutionTelemetry
{
    public static void Execute(
        DbConnectionMockBase connection,
        string phase,
        Action operation)
    {
        if (!connection.Metrics.Enabled)
        {
            operation();
            return;
        }

        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            operation();
            connection.Metrics.IncrementBatchPhaseElapsedTicks(phase, StopwatchCompatible.GetElapsedTicks(startedAt));
        }
        catch (OperationCanceledException)
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

    public static T Execute<T>(
        DbConnectionMockBase connection,
        string phase,
        Func<T> operation)
    {
        if (!connection.Metrics.Enabled)
            return operation();

        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            var result = operation();
            connection.Metrics.IncrementBatchPhaseElapsedTicks(phase, StopwatchCompatible.GetElapsedTicks(startedAt));
            return result;
        }
        catch (OperationCanceledException)
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
        if (!connection.Metrics.Enabled)
            return operationAsync();

        return ExecuteAsyncCore(connection, phase, operationAsync);
    }

    public static async Task<T> ExecuteAsync<T>(
        DbConnectionMockBase connection,
        string phase,
        Func<Task<T>> operationAsync)
    {
        if (!connection.Metrics.Enabled)
            return await operationAsync().ConfigureAwait(false);

        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            var result = await operationAsync().ConfigureAwait(false);
            connection.Metrics.IncrementBatchPhaseElapsedTicks(phase, StopwatchCompatible.GetElapsedTicks(startedAt));
            return result;
        }
        catch (OperationCanceledException)
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

    private static async Task ExecuteAsyncCore(
        DbConnectionMockBase connection,
        string phase,
        Func<Task> operationAsync)
    {
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            await operationAsync().ConfigureAwait(false);
            connection.Metrics.IncrementBatchPhaseElapsedTicks(phase, StopwatchCompatible.GetElapsedTicks(startedAt));
        }
        catch (OperationCanceledException)
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

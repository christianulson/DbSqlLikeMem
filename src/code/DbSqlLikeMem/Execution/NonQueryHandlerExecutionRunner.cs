using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal static class NonQueryHandlerExecutionRunner
{
    private static readonly ConcurrentDictionary<Type, string> HandlerNameCache = new();

    public static bool TryHandleStatement(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        IReadOnlyList<INonQueryCommandHandler> handlers,
        out DmlExecutionResult affectedRows)
    {
        var metricsEnabled = context.Connection.Metrics.Enabled;
        var handlerCount = handlers.Count;
        for (var i = 0; i < handlerCount; i++)
        {
            var handler = handlers[i];
            string? handlerName = null;
            var startedAt = 0L;
            if (metricsEnabled)
            {
                handlerName = GetHandlerName(handler);
                startedAt = Stopwatch.GetTimestamp();
            }

            try
            {
                if (!handler.TryHandle(context, sqlRaw, out affectedRows))
                    continue;

                context.Connection.SetLastFoundRows(affectedRows.AffectedRows);
                if (metricsEnabled)
                    RegisterSuccessfulExecution(context.Connection, handlerName!, startedAt, affectedRows.AffectedRows);
                return true;
            }
            catch
            {
                if (metricsEnabled)
                    RegisterFailedExecution(context.Connection, handlerName!);
                throw;
            }
        }

        affectedRows = new DmlExecutionResult();
        return false;
    }

    private static string GetHandlerName(INonQueryCommandHandler handler)
        => HandlerNameCache.GetOrAdd(handler.GetType(), static type => type.Name);

    private static void RegisterSuccessfulExecution(
        DbConnectionMockBase connection,
        string handlerName,
        long startedAt,
        int affectedRows)
    {
        connection.Metrics.IncrementNonQueryHandlerHit(handlerName);
        var elapsedTicks = StopwatchCompatible.GetElapsedTicks(startedAt);
        connection.Metrics.IncrementNonQueryHandlerElapsedTicks(handlerName, elapsedTicks);
    }

    private static void RegisterFailedExecution(DbConnectionMockBase connection, string handlerName)
    {
        connection.Metrics.IncrementNonQueryHandlerFailure(handlerName);
        connection.Metrics.IncrementNonQueryException();
    }
}

using System.Diagnostics;
using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal static class NonQueryHandlerExecutionRunner
{
    private static readonly ConcurrentDictionary<Type, string> HandlerNameCache = new();

    public static bool TryHandleStatement(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        IReadOnlyList<INonQueryCommandHandler> handlers,
        out int affectedRows)
    {
        foreach (var handler in handlers)
        {
            var handlerName = GetHandlerName(handler);
            var startedAt = Stopwatch.GetTimestamp();

            try
            {
                if (!handler.TryHandle(context, sqlRaw, out affectedRows))
                    continue;

                RegisterSuccessfulExecution(context.Connection, handlerName, startedAt, affectedRows);
                return true;
            }
            catch
            {
                RegisterFailedExecution(context.Connection, handlerName);
                throw;
            }
        }

        affectedRows = 0;
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
        connection.SetLastFoundRows(affectedRows);
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

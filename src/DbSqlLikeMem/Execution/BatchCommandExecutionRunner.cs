namespace DbSqlLikeMem;

internal static class BatchCommandExecutionRunner
{
    public static void ExecuteIntoTables(
        DbConnectionMockBase connection,
        DbCommand command,
        ICollection<TableResultMock> tables,
        CommandBehavior behavior)
    {
        var metricsEnabled = connection.Metrics.Enabled;
        ExecuteIntoTables(connection, command, tables, behavior, metricsEnabled);
    }

    public static void ExecuteIntoTables(
        DbConnectionMockBase connection,
        DbCommand command,
        ICollection<TableResultMock> tables,
        CommandBehavior behavior,
        bool metricsEnabled)
    {
        if (metricsEnabled)
        {
            var startedAt = Stopwatch.GetTimestamp();
            connection.Metrics.IncrementBatchReaderCommand();
            connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Reader}{command.CommandType}");
            try
            {
                using var reader = command.ExecuteReader(behavior);
                var stats = BatchReaderResultCollector.CollectAllResultSetsWithStats(reader, tables);
                connection.Metrics.IncrementBatchResultTables(stats.TableCount);
                connection.Metrics.IncrementBatchRowsReturned(stats.RowCount);
                connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.Reader, StopwatchCompatible.GetElapsedTicks(startedAt));
            }
            catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
            {
                connection.Metrics.IncrementBatchReaderFallbackToNonQuery();
                connection.Metrics.IncrementBatchNonQueryCommand();
                connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.FallbackNonQuery}{command.CommandType}");
                BatchPhaseExecutionTelemetry.Execute(
                    connection,
                    BatchMetricKeys.Phases.FallbackNonQuery,
                    command.ExecuteNonQuery);
            }
            catch (OperationCanceledException)
            {
                connection.Metrics.IncrementBatchPhaseCancellation(BatchMetricKeys.Phases.Reader);
                connection.Metrics.IncrementBatchCancellation();
                throw;
            }
            catch
            {
                connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.Reader);
                connection.Metrics.IncrementBatchException();
                throw;
            }

            return;
        }

        try
        {
            using var reader = command.ExecuteReader(behavior);
            BatchReaderResultCollector.CollectAllResultSetsWithoutStats(reader, tables);
        }
        catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
        {
            command.ExecuteNonQuery();
        }
    }

    public static Task ExecuteIntoTablesAsync(
        DbConnectionMockBase connection,
        DbCommand command,
        ICollection<TableResultMock> tables,
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        var metricsEnabled = connection.Metrics.Enabled;
        return ExecuteIntoTablesAsync(connection, command, tables, behavior, cancellationToken, metricsEnabled);
    }

    public static Task ExecuteIntoTablesAsync(
        DbConnectionMockBase connection,
        DbCommand command,
        ICollection<TableResultMock> tables,
        CommandBehavior behavior,
        CancellationToken cancellationToken,
        bool metricsEnabled)
    {
        return ExecuteIntoTablesAsyncCore(connection, command, tables, behavior, cancellationToken, metricsEnabled);
    }

    private static async Task ExecuteIntoTablesAsyncCore(
        DbConnectionMockBase connection,
        DbCommand command,
        ICollection<TableResultMock> tables,
        CommandBehavior behavior,
        CancellationToken cancellationToken,
        bool metricsEnabled)
    {
        if (metricsEnabled)
        {
            var startedAt = Stopwatch.GetTimestamp();
            connection.Metrics.IncrementBatchReaderCommand();
            connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Reader}{command.CommandType}");
            try
            {
                using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
                var stats = BatchReaderResultCollector.CollectAllResultSetsWithStats(reader, tables);
                connection.Metrics.IncrementBatchResultTables(stats.TableCount);
                connection.Metrics.IncrementBatchRowsReturned(stats.RowCount);
                connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.Reader, StopwatchCompatible.GetElapsedTicks(startedAt));
            }
            catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
            {
                connection.Metrics.IncrementBatchReaderFallbackToNonQuery();
                connection.Metrics.IncrementBatchNonQueryCommand();
                connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.FallbackNonQuery}{command.CommandType}");
                await BatchPhaseExecutionTelemetry.ExecuteAsync(
                    connection,
                    BatchMetricKeys.Phases.FallbackNonQuery,
                    () => command.ExecuteNonQueryAsync(cancellationToken))
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                connection.Metrics.IncrementBatchPhaseCancellation(BatchMetricKeys.Phases.Reader);
                connection.Metrics.IncrementBatchCancellation();
                throw;
            }
            catch
            {
                connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.Reader);
                connection.Metrics.IncrementBatchException();
                throw;
            }

            return;
        }

        try
        {
            using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
            BatchReaderResultCollector.CollectAllResultSetsWithoutStats(reader, tables);
        }
        catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

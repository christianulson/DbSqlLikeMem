namespace DbSqlLikeMem;

/// <summary>
/// EN: Formats runtime query debug traces into readable text output.
/// PT: Formata traces de debug de query em runtime em uma saida textual legivel.
/// </summary>
public static class QueryDebugTraceFormatter
{
    /// <summary>
    /// EN: Formats one runtime query debug trace as readable text.
    /// PT: Formata um trace de debug de query em runtime como texto legivel.
    /// </summary>
    /// <param name="trace">EN: Trace to format. PT: Trace a formatar.</param>
    /// <returns>EN: Formatted text. PT: Texto formatado.</returns>
    public static string Format(QueryDebugTrace trace)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(trace, nameof(trace));
        LogIfTraceTotalLooksInconsistent(trace);

        var sb = new StringBuilder();
        sb.AppendLine("Query Debug Trace");
        AppendLine(sb, "QueryType", trace.QueryType);
        AppendLine(sb, "StatementIndex", trace.StatementIndex);
        AppendOptionalLine(sb, "SqlText", trace.SqlText);
        AppendLine(sb, "Steps", trace.Steps.Count);
        AppendMillisecondsLine(sb, "TotalElapsedMs", trace.TotalExecutionTime.TotalMilliseconds);
        AppendLine(sb, "MaxInputRows", trace.MaxInputRows);
        AppendLine(sb, "MaxOutputRows", trace.MaxOutputRows);
        AppendOptionalLine(sb, "Operators", trace.OperatorSignature);
        AppendOptionalLine(sb, "FirstOperator", trace.FirstOperator);
        AppendOptionalLine(sb, "LastOperator", trace.LastOperator);
        AppendOptionalLine(sb, "OperatorCounts", trace.OperatorCounts);
        AppendOptionalLine(sb, "SlowestOperator", trace.SlowestOperator);
        AppendOptionalLine(sb, "SlowestStepIndex", trace.SlowestStepIndex);
        AppendOptionalLine(sb, "SlowestStepDetails", trace.SlowestStepDetails);
        AppendOptionalLine(sb, "FastestOperator", trace.FastestOperator);
        AppendOptionalLine(sb, "FastestStepIndex", trace.FastestStepIndex);
        AppendOptionalLine(sb, "FastestStepDetails", trace.FastestStepDetails);
        AppendOptionalLine(sb, "WidestOperator", trace.WidestOperator);
        AppendOptionalLine(sb, "WidestStepIndex", trace.WidestStepIndex);
        AppendOptionalLine(sb, "WidestStepDetails", trace.WidestStepDetails);
        AppendOptionalLine(sb, "NarrowestOperator", trace.NarrowestOperator);
        AppendOptionalLine(sb, "NarrowestStepIndex", trace.NarrowestStepIndex);
        AppendOptionalLine(sb, "NarrowestStepDetails", trace.NarrowestStepDetails);
        AppendStepLines(sb, trace.Steps);

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// EN: Formats a batch of runtime query debug traces as readable text.
    /// PT: Formata um lote de traces de debug de query em runtime como texto legivel.
    /// </summary>
    /// <param name="traces">EN: Traces to format. PT: Traces a formatar.</param>
    /// <returns>EN: Formatted text for the trace batch. PT: Texto formatado para o lote de traces.</returns>
    public static string FormatBatch(IReadOnlyList<QueryDebugTrace> traces)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(traces, nameof(traces));

        var summary = BuildBatchSummary(traces);

        var sb = new StringBuilder();
        sb.AppendLine("Query Debug Trace Batch");
        AppendLine(sb, "TraceCount", traces.Count);
        AppendLine(sb, "TotalStepCount", summary.TotalStepCount);
        AppendMillisecondsLine(sb, "TotalElapsedMs", summary.TotalElapsedMs);
        AppendLine(sb, "MaxInputRows", summary.MaxInputRows);
        AppendLine(sb, "MaxOutputRows", summary.MaxOutputRows);
        AppendOptionalLine(sb, "Operators", summary.OperatorSignature);
        AppendOptionalLine(sb, "QueryTypes", summary.QueryTypeCounts);
        AppendOptionalLine(sb, "OperatorCounts", summary.OperatorCounts);
        AppendOptionalStatementLine(sb, "SlowestStatementIndex", "SlowestStatementSql", summary.SlowestStatement);
        AppendOptionalStatementLine(sb, "FastestStatementIndex", "FastestStatementSql", summary.FastestStatement);
        AppendOptionalStatementLine(sb, "WidestStatementIndex", "WidestStatementSql", summary.WidestStatement);
        AppendOptionalStatementLine(sb, "NarrowestStatementIndex", "NarrowestStatementSql", summary.NarrowestStatement);

        for (var i = 0; i < traces.Count; i++)
        {
            if (i > 0)
                sb.AppendLine("---");

            sb.AppendLine(Format(traces[i]));
        }

        return sb.ToString().TrimEnd();
    }

    private static void AppendStepLines(StringBuilder sb, IReadOnlyList<QueryDebugTraceStep> steps)
    {
        for (var i = 0; i < steps.Count; i++)
        {
            var step = steps[i];
            sb.AppendLine($"  - Step[{i + 1}]: {step.Operator}");
            sb.AppendLine($"    InputRows: {step.InputRows}");
            sb.AppendLine($"    OutputRows: {step.OutputRows}");
            sb.AppendLine($"    ElapsedMs: {step.ExecutionTime.TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture)}");
            if (!string.IsNullOrWhiteSpace(step.Details))
                sb.AppendLine($"    Details: {step.Details}");
        }
    }

    private static void AppendOptionalStatementLine(
        StringBuilder sb,
        string indexLabel,
        string sqlLabel,
        QueryDebugTrace? statement)
    {
        if (statement is null)
            return;

        AppendLine(sb, indexLabel, statement.StatementIndex);
        AppendOptionalLine(sb, sqlLabel, statement.SqlText);
    }

    private static void AppendMillisecondsLine(StringBuilder sb, string label, double value)
        => AppendLine(sb, label, value.ToString("F3", CultureInfo.InvariantCulture));

    private static void AppendOptionalLine(StringBuilder sb, string label, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
            AppendLine(sb, label, value!);
    }

    private static void AppendOptionalLine(StringBuilder sb, string label, int value)
    {
        if (value >= 0)
            AppendLine(sb, label, value);
    }

    private static void AppendLine(StringBuilder sb, string label, object value)
        => sb.AppendLine($"- {label}: {value}");

    /// <summary>
    /// EN: Formats one runtime query debug trace as structured JSON.
    /// PT: Formata um trace de debug de query em runtime como JSON estruturado.
    /// </summary>
    /// <param name="trace">EN: Trace to format. PT: Trace a formatar.</param>
    /// <returns>EN: JSON payload. PT: Payload JSON.</returns>
    public static string FormatJson(QueryDebugTrace trace)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(trace, nameof(trace));
        LogIfTraceTotalLooksInconsistent(trace);

        var payload = new Dictionary<string, object?>
        {
            ["queryType"] = trace.QueryType,
            ["statementIndex"] = trace.StatementIndex,
            ["sqlText"] = trace.SqlText,
            ["stepCount"] = trace.Steps.Count,
            ["totalElapsedMs"] = trace.TotalExecutionTime.TotalMilliseconds,
            ["maxInputRows"] = trace.MaxInputRows,
            ["maxOutputRows"] = trace.MaxOutputRows,
            ["operators"] = trace.OperatorSignature,
            ["firstOperator"] = trace.FirstOperator,
            ["lastOperator"] = trace.LastOperator,
            ["operatorCounts"] = trace.OperatorCounts,
            ["slowestOperator"] = trace.SlowestOperator,
            ["slowestStepIndex"] = trace.SlowestStepIndex,
            ["slowestStepDetails"] = trace.SlowestStepDetails,
            ["fastestOperator"] = trace.FastestOperator,
            ["fastestStepIndex"] = trace.FastestStepIndex,
            ["fastestStepDetails"] = trace.FastestStepDetails,
            ["widestOperator"] = trace.WidestOperator,
            ["widestStepIndex"] = trace.WidestStepIndex,
            ["widestStepDetails"] = trace.WidestStepDetails,
            ["narrowestOperator"] = trace.NarrowestOperator,
            ["narrowestStepIndex"] = trace.NarrowestStepIndex,
            ["narrowestStepDetails"] = trace.NarrowestStepDetails,
            ["steps"] = trace.Steps.Select(static step => new Dictionary<string, object?>
            {
                ["operator"] = step.Operator,
                ["inputRows"] = step.InputRows,
                ["outputRows"] = step.OutputRows,
                ["elapsedMs"] = step.ExecutionTime.TotalMilliseconds,
                ["details"] = step.Details
            }).ToArray()
        };

        return JsonSerializer.Serialize(payload);
    }

    /// <summary>
    /// EN: Formats a batch of runtime query debug traces as structured JSON.
    /// PT: Formata um lote de traces de debug de query em runtime como JSON estruturado.
    /// </summary>
    /// <param name="traces">EN: Traces to format. PT: Traces a formatar.</param>
    /// <returns>EN: JSON payload for the trace batch. PT: Payload JSON para o lote de traces.</returns>
    public static string FormatBatchJson(IReadOnlyList<QueryDebugTrace> traces)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(traces, nameof(traces));

        var summary = BuildBatchSummary(traces);

        var payload = new Dictionary<string, object?>
        {
            ["traceCount"] = traces.Count,
            ["totalStepCount"] = summary.TotalStepCount,
            ["totalElapsedMs"] = summary.TotalElapsedMs,
            ["maxInputRows"] = summary.MaxInputRows,
            ["maxOutputRows"] = summary.MaxOutputRows,
            ["operators"] = summary.OperatorSignature,
            ["queryTypes"] = summary.QueryTypeCounts,
            ["operatorCounts"] = summary.OperatorCounts,
            ["slowestStatementIndex"] = summary.SlowestStatement?.StatementIndex ?? -1,
            ["slowestStatementSql"] = summary.SlowestStatement?.SqlText,
            ["fastestStatementIndex"] = summary.FastestStatement?.StatementIndex ?? -1,
            ["fastestStatementSql"] = summary.FastestStatement?.SqlText,
            ["widestStatementIndex"] = summary.WidestStatement?.StatementIndex ?? -1,
            ["widestStatementSql"] = summary.WidestStatement?.SqlText,
            ["narrowestStatementIndex"] = summary.NarrowestStatement?.StatementIndex ?? -1,
            ["narrowestStatementSql"] = summary.NarrowestStatement?.SqlText,
            ["traces"] = traces.Select(AsJsonPayload).ToArray()
        };

        return JsonSerializer.Serialize(payload);
    }

    private static Dictionary<string, object?> AsJsonPayload(QueryDebugTrace trace)
    {
        LogIfTraceTotalLooksInconsistent(trace);
        return new()
        {
            ["queryType"] = trace.QueryType,
            ["statementIndex"] = trace.StatementIndex,
            ["sqlText"] = trace.SqlText,
            ["stepCount"] = trace.Steps.Count,
            ["totalElapsedMs"] = trace.TotalExecutionTime.TotalMilliseconds,
            ["maxInputRows"] = trace.MaxInputRows,
            ["maxOutputRows"] = trace.MaxOutputRows,
            ["operators"] = trace.OperatorSignature,
            ["firstOperator"] = trace.FirstOperator,
            ["lastOperator"] = trace.LastOperator,
            ["operatorCounts"] = trace.OperatorCounts,
            ["slowestOperator"] = trace.SlowestOperator,
            ["slowestStepIndex"] = trace.SlowestStepIndex,
            ["slowestStepDetails"] = trace.SlowestStepDetails,
            ["fastestOperator"] = trace.FastestOperator,
            ["fastestStepIndex"] = trace.FastestStepIndex,
            ["fastestStepDetails"] = trace.FastestStepDetails,
            ["widestOperator"] = trace.WidestOperator,
            ["widestStepIndex"] = trace.WidestStepIndex,
            ["widestStepDetails"] = trace.WidestStepDetails,
            ["narrowestOperator"] = trace.NarrowestOperator,
            ["narrowestStepIndex"] = trace.NarrowestStepIndex,
            ["narrowestStepDetails"] = trace.NarrowestStepDetails,
            ["steps"] = trace.Steps.Select(static step => new Dictionary<string, object?>
            {
                ["operator"] = step.Operator,
                ["inputRows"] = step.InputRows,
                ["outputRows"] = step.OutputRows,
                ["elapsedMs"] = step.ExecutionTime.TotalMilliseconds,
                ["details"] = step.Details
            }).ToArray()
        };
    }

    private static void LogIfTraceTotalLooksInconsistent(QueryDebugTrace trace)
    {
        if (trace.Steps.Count == 0)
            return;

        var stepsTotalMs = trace.Steps.Sum(static step => step.ExecutionTime.TotalMilliseconds);
        var traceTotalMs = trace.TotalExecutionTime.TotalMilliseconds;
        if (Math.Abs(stepsTotalMs - traceTotalMs) <= 0.0001d)
            return;

        Console.WriteLine(
            $"[QUERY-TRACE-TOTAL-MISMATCH] QueryType='{trace.QueryType}' StatementIndex={trace.StatementIndex} " +
            $"StepsTotalMs={stepsTotalMs.ToString("F3", CultureInfo.InvariantCulture)} " +
            $"TraceTotalMs={traceTotalMs.ToString("F3", CultureInfo.InvariantCulture)} " +
            $"Steps=[{string.Join(", ", trace.Steps.Select(static step => step.ExecutionTime.TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture)))}]");
    }

    private static string FormatCountMap(IReadOnlyDictionary<string, int> counts)
    {
        if (counts.Count == 0)
            return string.Empty;

        return string.Join(";", counts
            .OrderBy(static entry => entry.Key, StringComparer.Ordinal)
            .Select(static entry => $"{entry.Key}:{entry.Value}"));
    }

    private static BatchSummary BuildBatchSummary(IReadOnlyList<QueryDebugTrace> traces)
    {
        var totalStepCount = 0;
        var totalElapsedMs = 0d;
        var maxInputRows = 0;
        var maxOutputRows = 0;
        var queryTypeCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        var operatorCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        QueryDebugTrace? slowestStatement = null;
        QueryDebugTrace? fastestStatement = null;
        QueryDebugTrace? widestStatement = null;
        QueryDebugTrace? narrowestStatement = null;

        for (var i = 0; i < traces.Count; i++)
        {
            var trace = traces[i];
            totalStepCount += trace.Steps.Count;
            totalElapsedMs += trace.TotalExecutionTime.TotalMilliseconds;
            if (trace.MaxInputRows > maxInputRows)
                maxInputRows = trace.MaxInputRows;
            if (trace.MaxOutputRows > maxOutputRows)
                maxOutputRows = trace.MaxOutputRows;

            queryTypeCounts.TryGetValue(trace.QueryType, out var queryTypeCount);
            queryTypeCounts[trace.QueryType] = queryTypeCount + 1;

            if (slowestStatement is null || IsBetterElapsedCandidate(trace, slowestStatement, descending: true))
                slowestStatement = trace;
            if (fastestStatement is null || IsBetterElapsedCandidate(trace, fastestStatement, descending: false))
                fastestStatement = trace;
            if (widestStatement is null || IsBetterVolumeCandidate(trace, widestStatement, descending: true))
                widestStatement = trace;
            if (narrowestStatement is null || IsBetterVolumeCandidate(trace, narrowestStatement, descending: false))
                narrowestStatement = trace;

            for (var stepIndex = 0; stepIndex < trace.Steps.Count; stepIndex++)
            {
                var operatorName = trace.Steps[stepIndex].Operator;
                operatorCounts.TryGetValue(operatorName, out var operatorCount);
                operatorCounts[operatorName] = operatorCount + 1;
            }
        }

        return new BatchSummary(
            totalStepCount,
            totalElapsedMs,
            maxInputRows,
            maxOutputRows,
            FormatCountMap(queryTypeCounts),
            FormatCountMap(operatorCounts),
            FormatOperatorSignature(operatorCounts.Keys),
            slowestStatement,
            fastestStatement,
            widestStatement,
            narrowestStatement);
    }

    private static string FormatOperatorSignature(IEnumerable<string> operatorNames)
        => string.Join(" -> ", operatorNames.OrderBy(static name => name, StringComparer.Ordinal));

    private static bool IsBetterElapsedCandidate(QueryDebugTrace candidate, QueryDebugTrace current, bool descending)
    {
        var candidateMs = Math.Round(candidate.TotalExecutionTime.TotalMilliseconds, 3, MidpointRounding.AwayFromZero);
        var currentMs = Math.Round(current.TotalExecutionTime.TotalMilliseconds, 3, MidpointRounding.AwayFromZero);
        var comparison = candidateMs.CompareTo(currentMs);
        if (comparison != 0)
            return descending ? comparison > 0 : comparison < 0;

        return candidate.StatementIndex < current.StatementIndex;
    }

    private static bool IsBetterVolumeCandidate(QueryDebugTrace candidate, QueryDebugTrace current, bool descending)
    {
        if (candidate.MaxOutputRows != current.MaxOutputRows)
            return descending
                ? candidate.MaxOutputRows > current.MaxOutputRows
                : candidate.MaxOutputRows < current.MaxOutputRows;

        if (candidate.MaxInputRows != current.MaxInputRows)
            return descending
                ? candidate.MaxInputRows > current.MaxInputRows
                : candidate.MaxInputRows < current.MaxInputRows;

        return candidate.StatementIndex < current.StatementIndex;
    }

    private sealed record BatchSummary(
        int TotalStepCount,
        double TotalElapsedMs,
        int MaxInputRows,
        int MaxOutputRows,
        string QueryTypeCounts,
        string OperatorCounts,
        string OperatorSignature,
        QueryDebugTrace? SlowestStatement,
        QueryDebugTrace? FastestStatement,
        QueryDebugTrace? WidestStatement,
        QueryDebugTrace? NarrowestStatement);
}

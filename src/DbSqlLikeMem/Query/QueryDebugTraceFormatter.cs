using System.Text;
using System.Text.Json;

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

        var sb = new StringBuilder();
        sb.AppendLine("Query Debug Trace");
        sb.AppendLine($"- QueryType: {trace.QueryType}");
        sb.AppendLine($"- StatementIndex: {trace.StatementIndex}");

        if (!string.IsNullOrWhiteSpace(trace.SqlText))
            sb.AppendLine($"- SqlText: {trace.SqlText}");

        sb.AppendLine($"- Steps: {trace.Steps.Count}");
        sb.AppendLine($"- TotalElapsedMs: {trace.TotalExecutionTime.TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture)}");
        sb.AppendLine($"- MaxInputRows: {trace.MaxInputRows}");
        sb.AppendLine($"- MaxOutputRows: {trace.MaxOutputRows}");
        if (!string.IsNullOrWhiteSpace(trace.OperatorSignature))
            sb.AppendLine($"- Operators: {trace.OperatorSignature}");
        if (!string.IsNullOrWhiteSpace(trace.FirstOperator))
            sb.AppendLine($"- FirstOperator: {trace.FirstOperator}");
        if (!string.IsNullOrWhiteSpace(trace.LastOperator))
            sb.AppendLine($"- LastOperator: {trace.LastOperator}");
        if (!string.IsNullOrWhiteSpace(trace.OperatorCounts))
            sb.AppendLine($"- OperatorCounts: {trace.OperatorCounts}");
        if (!string.IsNullOrWhiteSpace(trace.SlowestOperator))
            sb.AppendLine($"- SlowestOperator: {trace.SlowestOperator}");
        if (trace.SlowestStepIndex >= 0)
            sb.AppendLine($"- SlowestStepIndex: {trace.SlowestStepIndex}");
        if (!string.IsNullOrWhiteSpace(trace.SlowestStepDetails))
            sb.AppendLine($"- SlowestStepDetails: {trace.SlowestStepDetails}");
        if (!string.IsNullOrWhiteSpace(trace.FastestOperator))
            sb.AppendLine($"- FastestOperator: {trace.FastestOperator}");
        if (trace.FastestStepIndex >= 0)
            sb.AppendLine($"- FastestStepIndex: {trace.FastestStepIndex}");
        if (!string.IsNullOrWhiteSpace(trace.FastestStepDetails))
            sb.AppendLine($"- FastestStepDetails: {trace.FastestStepDetails}");
        if (!string.IsNullOrWhiteSpace(trace.WidestOperator))
            sb.AppendLine($"- WidestOperator: {trace.WidestOperator}");
        if (trace.WidestStepIndex >= 0)
            sb.AppendLine($"- WidestStepIndex: {trace.WidestStepIndex}");
        if (!string.IsNullOrWhiteSpace(trace.WidestStepDetails))
            sb.AppendLine($"- WidestStepDetails: {trace.WidestStepDetails}");
        if (!string.IsNullOrWhiteSpace(trace.NarrowestOperator))
            sb.AppendLine($"- NarrowestOperator: {trace.NarrowestOperator}");
        if (trace.NarrowestStepIndex >= 0)
            sb.AppendLine($"- NarrowestStepIndex: {trace.NarrowestStepIndex}");
        if (!string.IsNullOrWhiteSpace(trace.NarrowestStepDetails))
            sb.AppendLine($"- NarrowestStepDetails: {trace.NarrowestStepDetails}");

        for (var i = 0; i < trace.Steps.Count; i++)
        {
            var step = trace.Steps[i];
            sb.AppendLine($"  - Step[{i + 1}]: {step.Operator}");
            sb.AppendLine($"    InputRows: {step.InputRows}");
            sb.AppendLine($"    OutputRows: {step.OutputRows}");
            sb.AppendLine($"    ElapsedMs: {step.ExecutionTime.TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture)}");
            if (!string.IsNullOrWhiteSpace(step.Details))
                sb.AppendLine($"    Details: {step.Details}");
        }

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
        sb.AppendLine($"- TraceCount: {traces.Count}");
        sb.AppendLine($"- TotalStepCount: {summary.TotalStepCount}");
        sb.AppendLine($"- TotalElapsedMs: {summary.TotalElapsedMs.ToString("F3", CultureInfo.InvariantCulture)}");
        sb.AppendLine($"- MaxInputRows: {summary.MaxInputRows}");
        sb.AppendLine($"- MaxOutputRows: {summary.MaxOutputRows}");
        if (!string.IsNullOrWhiteSpace(summary.OperatorSignature))
            sb.AppendLine($"- Operators: {summary.OperatorSignature}");
        if (!string.IsNullOrWhiteSpace(summary.QueryTypeCounts))
            sb.AppendLine($"- QueryTypes: {summary.QueryTypeCounts}");
        if (!string.IsNullOrWhiteSpace(summary.OperatorCounts))
            sb.AppendLine($"- OperatorCounts: {summary.OperatorCounts}");
        if (summary.SlowestStatement is not null)
            sb.AppendLine($"- SlowestStatementIndex: {summary.SlowestStatement.StatementIndex}");
        if (!string.IsNullOrWhiteSpace(summary.SlowestStatement?.SqlText))
            sb.AppendLine($"- SlowestStatementSql: {summary.SlowestStatement.SqlText}");
        if (summary.FastestStatement is not null)
            sb.AppendLine($"- FastestStatementIndex: {summary.FastestStatement.StatementIndex}");
        if (!string.IsNullOrWhiteSpace(summary.FastestStatement?.SqlText))
            sb.AppendLine($"- FastestStatementSql: {summary.FastestStatement.SqlText}");
        if (summary.WidestStatement is not null)
            sb.AppendLine($"- WidestStatementIndex: {summary.WidestStatement.StatementIndex}");
        if (!string.IsNullOrWhiteSpace(summary.WidestStatement?.SqlText))
            sb.AppendLine($"- WidestStatementSql: {summary.WidestStatement.SqlText}");
        if (summary.NarrowestStatement is not null)
            sb.AppendLine($"- NarrowestStatementIndex: {summary.NarrowestStatement.StatementIndex}");
        if (!string.IsNullOrWhiteSpace(summary.NarrowestStatement?.SqlText))
            sb.AppendLine($"- NarrowestStatementSql: {summary.NarrowestStatement.SqlText}");

        for (var i = 0; i < traces.Count; i++)
        {
            if (i > 0)
                sb.AppendLine("---");

            sb.AppendLine(Format(traces[i]));
        }

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// EN: Formats one runtime query debug trace as structured JSON.
    /// PT: Formata um trace de debug de query em runtime como JSON estruturado.
    /// </summary>
    /// <param name="trace">EN: Trace to format. PT: Trace a formatar.</param>
    /// <returns>EN: JSON payload. PT: Payload JSON.</returns>
    public static string FormatJson(QueryDebugTrace trace)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(trace, nameof(trace));

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
        => new()
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
        var comparison = candidate.TotalExecutionTime.CompareTo(current.TotalExecutionTime);
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

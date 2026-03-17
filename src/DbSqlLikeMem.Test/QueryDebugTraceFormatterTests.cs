namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Verifies textual formatting for runtime query debug traces.
/// PT: Verifica a formatacao textual dos traces de debug de query em runtime.
/// </summary>
public sealed class QueryDebugTraceFormatterTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private static TimeSpan Ms(decimal milliseconds)
        => TimeSpan.FromTicks((long)(milliseconds * TimeSpan.TicksPerMillisecond));

    /// <summary>
    /// EN: Ensures the formatter renders statement context and ordered runtime steps.
    /// PT: Garante que o formatter renderize o contexto do statement e os passos de runtime em ordem.
    /// </summary>
    [Fact]
    public void Format_ShouldRenderStatementContext_AndOrderedSteps()
    {
        var trace = new QueryDebugTrace(
            "SELECT",
            0,
            "SELECT Id FROM users",
            [
                new QueryDebugTraceStep("TableScan", 3, 3, Ms(1.25m), "users"),
                new QueryDebugTraceStep("Project", 3, 3, Ms(0.2m), "columns=1"),
                new QueryDebugTraceStep("Project", 3, 3, Ms(0.4m), "columns=1")
            ]);
        LogTrace("single", trace);

        var text = QueryDebugTraceFormatter.Format(trace);

        text.Should().Contain("Query Debug Trace");
        text.Should().Contain("- QueryType: SELECT");
        text.Should().Contain("- StatementIndex: 0");
        text.Should().Contain("- SqlText: SELECT Id FROM users");
        text.Should().Contain("- Steps: 3");
        text.Should().Contain("- TotalElapsedMs: 1.850");
        text.Should().Contain("- MaxInputRows: 3");
        text.Should().Contain("- MaxOutputRows: 3");
        text.Should().Contain("- Operators: TableScan -> Project -> Project");
        text.Should().Contain("- OperatorCounts: Project:2;TableScan:1");
        text.Should().Contain("- FirstOperator: TableScan");
        text.Should().Contain("- LastOperator: Project");
        text.Should().Contain("- SlowestOperator: TableScan");
        text.Should().Contain("- SlowestStepIndex: 0");
        text.Should().Contain("- SlowestStepDetails: users");
        text.Should().Contain("- FastestOperator: Project");
        text.Should().Contain("- FastestStepIndex: 1");
        text.Should().Contain("- FastestStepDetails: columns=1");
        text.Should().Contain("- WidestOperator: TableScan");
        text.Should().Contain("- WidestStepIndex: 0");
        text.Should().Contain("- WidestStepDetails: users");
        text.Should().Contain("- NarrowestOperator: TableScan");
        text.Should().Contain("- NarrowestStepIndex: 0");
        text.Should().Contain("- NarrowestStepDetails: users");
        text.Should().Contain("  - Step[1]: TableScan");
        text.Should().Contain("    InputRows: 3");
        text.Should().Contain("    OutputRows: 3");
        text.Should().Contain("    ElapsedMs: 1.250");
        text.Should().Contain("    Details: users");
        text.Should().Contain("  - Step[2]: Project");
        text.Should().Contain("  - Step[3]: Project");
    }

    /// <summary>
    /// EN: Ensures batch formatting separates traces and preserves per-statement context.
    /// PT: Garante que a formatacao em lote separe os traces e preserve o contexto por statement.
    /// </summary>
    [Fact]
    public void FormatBatch_ShouldSeparateTraces_AndPreserveStatementContext()
    {
        List<QueryDebugTrace> traces =
        [
            new QueryDebugTrace(
                "SELECT",
                0,
                "SELECT 1",
                [new QueryDebugTraceStep("Project", 1, 1, Ms(0.1m))]),
            new QueryDebugTrace(
                "UNION",
                1,
                "SELECT 2",
                [
                    new QueryDebugTraceStep("UnionInputs", 2, 2, Ms(1.2m)),
                    new QueryDebugTraceStep("Project", 1, 1, Ms(0.3m))
                ])
        ];
        LogBatch("text-batch", traces);

        var text = QueryDebugTraceFormatter.FormatBatch(traces);

        text.Should().Contain("Query Debug Trace");
        text.Should().Contain("Query Debug Trace Batch");
        text.Should().Contain("TraceCount: 2");
        text.Should().Contain("TotalStepCount: 3");
        text.Should().Contain("TotalElapsedMs: 1.600");
        text.Should().Contain("MaxInputRows: 2");
        text.Should().Contain("MaxOutputRows: 2");
        text.Should().Contain("Operators: Project -> UnionInputs");
        text.Should().Contain("QueryTypes: SELECT:1;UNION:1");
        text.Should().Contain("OperatorCounts: Project:2;UnionInputs:1");
        text.Should().Contain("SlowestStatementIndex: 1");
        text.Should().Contain("SlowestStatementSql: SELECT 2");
        text.Should().Contain("FastestStatementIndex: 0");
        text.Should().Contain("FastestStatementSql: SELECT 1");
        text.Should().Contain("WidestStatementIndex: 1");
        text.Should().Contain("WidestStatementSql: SELECT 2");
        text.Should().Contain("NarrowestStatementIndex: 0");
        text.Should().Contain("NarrowestStatementSql: SELECT 1");
        text.Should().Contain("StatementIndex: 0");
        text.Should().Contain("StatementIndex: 1");
        text.Should().Contain("SqlText: SELECT 1");
        text.Should().Contain("SqlText: SELECT 2");
    }

    /// <summary>
    /// EN: Ensures JSON formatting emits a stable structured payload for one trace.
    /// PT: Garante que a formatacao JSON emita um payload estruturado estavel para um trace.
    /// </summary>
    [Fact]
    public void FormatJson_ShouldEmitStableStructuredPayload()
    {
        var trace = new QueryDebugTrace(
            "SELECT",
            2,
            "SELECT Id FROM users",
            [new QueryDebugTraceStep("Project", 3, 3, Ms(0.4m), "columns=1")]);
        LogTrace("json-single", trace);

        var json = QueryDebugTraceFormatter.FormatJson(trace);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        root.GetProperty("queryType").GetString().Should().Be("SELECT");
        root.GetProperty("statementIndex").GetInt32().Should().Be(2);
        root.GetProperty("sqlText").GetString().Should().Be("SELECT Id FROM users");
        root.GetProperty("stepCount").GetInt32().Should().Be(1);
        root.GetProperty("totalElapsedMs").GetDouble().Should().BeApproximately(0.4d, 0.0001d);
        root.GetProperty("maxInputRows").GetInt32().Should().Be(3);
        root.GetProperty("maxOutputRows").GetInt32().Should().Be(3);
        root.GetProperty("operators").GetString().Should().Be("Project");
        root.GetProperty("operatorCounts").GetString().Should().Be("Project:1");
        root.GetProperty("firstOperator").GetString().Should().Be("Project");
        root.GetProperty("lastOperator").GetString().Should().Be("Project");
        root.GetProperty("slowestOperator").GetString().Should().Be("Project");
        root.GetProperty("slowestStepIndex").GetInt32().Should().Be(0);
        root.GetProperty("slowestStepDetails").GetString().Should().Be("columns=1");
        root.GetProperty("fastestOperator").GetString().Should().Be("Project");
        root.GetProperty("fastestStepIndex").GetInt32().Should().Be(0);
        root.GetProperty("fastestStepDetails").GetString().Should().Be("columns=1");
        root.GetProperty("widestOperator").GetString().Should().Be("Project");
        root.GetProperty("widestStepIndex").GetInt32().Should().Be(0);
        root.GetProperty("widestStepDetails").GetString().Should().Be("columns=1");
        root.GetProperty("narrowestOperator").GetString().Should().Be("Project");
        root.GetProperty("narrowestStepIndex").GetInt32().Should().Be(0);
        root.GetProperty("narrowestStepDetails").GetString().Should().Be("columns=1");
        var step = root.GetProperty("steps")[0];
        step.GetProperty("operator").GetString().Should().Be("Project");
        step.GetProperty("inputRows").GetInt32().Should().Be(3);
        step.GetProperty("outputRows").GetInt32().Should().Be(3);
        step.GetProperty("elapsedMs").GetDouble().Should().BeApproximately(0.4d, 0.0001d);
        step.GetProperty("details").GetString().Should().Be("columns=1");
    }

    /// <summary>
    /// EN: Ensures trace aggregates keep the earliest step when elapsed time or volume ties occur.
    /// PT: Garante que os agregados do trace mantenham o primeiro passo quando houver empate de tempo ou volume.
    /// </summary>
    [Fact]
    public void QueryDebugTrace_ShouldKeepEarliestStep_WhenMetricsTie()
    {
        var trace = new QueryDebugTrace(
            "SELECT",
            0,
            "SELECT 1",
            [
                new QueryDebugTraceStep("Filter", 5, 2, Ms(0.5m), "first"),
                new QueryDebugTraceStep("Project", 5, 2, Ms(0.5m), "second"),
                new QueryDebugTraceStep("Limit", 2, 1, Ms(0.2m), "third")
            ]);

        trace.SlowestOperator.Should().Be("Filter");
        trace.SlowestStepIndex.Should().Be(0);
        trace.SlowestStepDetails.Should().Be("first");
        trace.FastestOperator.Should().Be("Limit");
        trace.FastestStepIndex.Should().Be(2);
        trace.WidestOperator.Should().Be("Filter");
        trace.WidestStepIndex.Should().Be(0);
        trace.WidestStepDetails.Should().Be("first");
        trace.NarrowestOperator.Should().Be("Limit");
        trace.NarrowestStepIndex.Should().Be(2);
    }

    /// <summary>
    /// EN: Ensures batch JSON formatting preserves order and statement metadata.
    /// PT: Garante que a formatacao JSON em lote preserve a ordem e os metadados dos statements.
    /// </summary>
    [Fact]
    public void FormatBatchJson_ShouldPreserveOrder_AndStatementMetadata()
    {
        List<QueryDebugTrace> traces =
        [
            new QueryDebugTrace("SELECT", 0, "SELECT 1", [new QueryDebugTraceStep("Project", 1, 1, Ms(0.1m))]),
            new QueryDebugTrace(
                "UNION",
                1,
                "SELECT 2",
                [
                    new QueryDebugTraceStep("UnionInputs", 2, 2, Ms(1.2m)),
                    new QueryDebugTraceStep("Project", 1, 1, Ms(0.3m))
                ])
        ];
        LogBatch("json-batch", traces);

        var json = QueryDebugTraceFormatter.FormatBatchJson(traces);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        root.GetProperty("traceCount").GetInt32().Should().Be(2);
        root.GetProperty("totalStepCount").GetInt32().Should().Be(3);
        root.GetProperty("totalElapsedMs").GetDouble().Should().BeApproximately(1.6d, 0.0001d);
        root.GetProperty("maxInputRows").GetInt32().Should().Be(2);
        root.GetProperty("maxOutputRows").GetInt32().Should().Be(2);
        root.GetProperty("operators").GetString().Should().Be("Project -> UnionInputs");
        root.GetProperty("queryTypes").GetString().Should().Be("SELECT:1;UNION:1");
        root.GetProperty("operatorCounts").GetString().Should().Be("Project:2;UnionInputs:1");
        root.GetProperty("slowestStatementIndex").GetInt32().Should().Be(1);
        root.GetProperty("slowestStatementSql").GetString().Should().Be("SELECT 2");
        root.GetProperty("fastestStatementIndex").GetInt32().Should().Be(0);
        root.GetProperty("fastestStatementSql").GetString().Should().Be("SELECT 1");
        root.GetProperty("widestStatementIndex").GetInt32().Should().Be(1);
        root.GetProperty("widestStatementSql").GetString().Should().Be("SELECT 2");
        root.GetProperty("narrowestStatementIndex").GetInt32().Should().Be(0);
        root.GetProperty("narrowestStatementSql").GetString().Should().Be("SELECT 1");
        var items = root.GetProperty("traces");
        items[0].GetProperty("statementIndex").GetInt32().Should().Be(0);
        items[0].GetProperty("sqlText").GetString().Should().Be("SELECT 1");
        items[1].GetProperty("statementIndex").GetInt32().Should().Be(1);
        items[1].GetProperty("sqlText").GetString().Should().Be("SELECT 2");
    }

    private static void LogBatch(string label, IReadOnlyList<QueryDebugTrace> traces)
    {
        Console.WriteLine($"[TEST-TRACE-BATCH] Label={label} Count={traces.Count}");
        for (var i = 0; i < traces.Count; i++)
            LogTrace($"{label}[{i}]", traces[i]);
    }

    private static void LogTrace(string label, QueryDebugTrace trace)
    {
        Console.WriteLine(
            $"[TEST-TRACE] Label={label} QueryType={trace.QueryType} StatementIndex={trace.StatementIndex} " +
            $"TotalMs={trace.TotalExecutionTime.TotalMilliseconds:F3} " +
            $"StepMs=[{string.Join(", ", trace.Steps.Select(static step => step.ExecutionTime.TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture)))}]");
    }

    /// <summary>
    /// EN: Ensures batch summaries keep the earliest statement when batch-level metrics tie.
    /// PT: Garante que os resumos em lote mantenham o primeiro statement quando houver empate de metricas no batch.
    /// </summary>
    [Fact]
    public void FormatBatch_ShouldKeepEarliestStatement_WhenMetricsTie()
    {
        List<QueryDebugTrace> traces =
        [
            new QueryDebugTrace("SELECT", 0, "SELECT 1", [new QueryDebugTraceStep("Project", 2, 2, Ms(0.5m))]),
            new QueryDebugTrace("SELECT", 1, "SELECT 2", [new QueryDebugTraceStep("Project", 2, 2, Ms(0.5m))])
        ];

        var text = QueryDebugTraceFormatter.FormatBatch(traces);
        var json = QueryDebugTraceFormatter.FormatBatchJson(traces);

        text.Should().Contain("SlowestStatementIndex: 0");
        text.Should().Contain("FastestStatementIndex: 0");
        text.Should().Contain("WidestStatementIndex: 0");
        text.Should().Contain("NarrowestStatementIndex: 0");

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        root.GetProperty("slowestStatementIndex").GetInt32().Should().Be(0);
        root.GetProperty("fastestStatementIndex").GetInt32().Should().Be(0);
        root.GetProperty("widestStatementIndex").GetInt32().Should().Be(0);
        root.GetProperty("narrowestStatementIndex").GetInt32().Should().Be(0);
    }
}

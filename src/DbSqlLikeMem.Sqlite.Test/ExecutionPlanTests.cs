using System.Text.Json;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Execution plan coverage tests for Sqlite mock commands.
/// PT: Testes de cobertura de plano de execução para comandos simulado Sqlite.
/// </summary>
public sealed class ExecutionPlanTests : XUnitTestBase
{
    /// <summary>
    /// EN: Initializes execution plan tests with xUnit output integration.
    /// PT: Inicializa os testes de plano de execução com integração de saída do xUnit.
    /// </summary>
    /// <param name="helper">EN: xUnit output helper. PT: Helper de saída do xUnit.</param>
    public ExecutionPlanTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    /// <summary>
    /// EN: Ensures command execution generates a readable execution plan in test output.
    /// PT: Garante que a execução do comando gere um plano de execução legível na saída do teste.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldGenerateExecutionPlan_AndPrintOnTestOutput()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        ids.Should().Equal(1, 3);
        cnn.LastExecutionPlan.Should().NotBeNullOrWhiteSpace();
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.QueryTypeLabel()}: SELECT");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.EstimatedCostLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.InputTablesLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.EstimatedRowsReadLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.SelectivityPctLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.RowsPerMsLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.PerformanceDisclaimerLabel()}:");
        cnn.LastExecutionPlan.Should().Contain(SqlExecutionPlanMessages.PerformanceDisclaimerMessage());
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.ActualRowsLabel()}: 2");

        Console.WriteLine("[ExecutionPlan][Sqlite]\n" + cnn.LastExecutionPlan);
    }

    /// <summary>
    /// EN: Ensures INSERT non-query execution also generates a readable execution plan.
    /// PT: Garante que a execucao non-query de INSERT tambem gere um plano de execucao legivel.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteNonQuery_Insert_ShouldGenerateExecutionPlan()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<string>("users", "Name");

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO users (Id, Name) VALUES (1, 'Ana'), (2, 'Bia')"
        };

        cmd.ExecuteNonQuery().Should().Be(2);
        cnn.LastExecutionPlan.Should().NotBeNullOrWhiteSpace();
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.QueryTypeLabel()}: INSERT");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.TableLabel()}: users");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.ActualRowsLabel()}: 2");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.PerformanceDisclaimerLabel()}:");
    }

    /// <summary>
    /// EN: Ensures UPDATE non-query execution also generates a readable execution plan with target and filter details.
    /// PT: Garante que a execucao non-query de UPDATE tambem gere um plano de execucao legivel com detalhes de alvo e filtro.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteNonQuery_Update_ShouldGenerateExecutionPlan()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 0],
            [2, 0],
            [3, 1]);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "UPDATE users SET Active = 1 WHERE Id <= 2"
        };

        cmd.ExecuteNonQuery().Should().Be(2);
        cnn.LastExecutionPlan.Should().NotBeNullOrWhiteSpace();
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.QueryTypeLabel()}: UPDATE");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.TableLabel()}: users");
        cnn.LastExecutionPlan.Should().Contain("- SET: 1 item(s)");
        cnn.LastExecutionPlan.Should().Contain("- WHERE:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.ActualRowsLabel()}: 2");
    }

    /// <summary>
    /// EN: Ensures DELETE non-query execution also generates a readable execution plan with target and filter details.
    /// PT: Garante que a execucao non-query de DELETE tambem gere um plano de execucao legivel com detalhes de alvo e filtro.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteNonQuery_Delete_ShouldGenerateExecutionPlan()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 0]);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "DELETE FROM users WHERE Active = 0"
        };

        cmd.ExecuteNonQuery().Should().Be(2);
        cnn.LastExecutionPlan.Should().NotBeNullOrWhiteSpace();
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.QueryTypeLabel()}: DELETE");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.TableLabel()}: users");
        cnn.LastExecutionPlan.Should().Contain("- WHERE:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.ActualRowsLabel()}: 2");
    }

    /// <summary>
    /// EN: Ensures DebugSql captures a lightweight runtime operator trace for SELECT execution.
    /// PT: Garante que DebugSql capture um trace leve de operadores em runtime para execucao de SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSql_ShouldCaptureRuntimeOperatorTrace()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        var trace = cnn.DebugSql("SELECT Id FROM users WHERE Active = 1 ORDER BY Id LIMIT 1");

        trace.QueryType.Should().Be(SqlConst.SELECT);
        trace.StatementIndex.Should().Be(0);
        trace.SqlText.Should().Be("SELECT Id FROM users WHERE Active = 1 ORDER BY Id LIMIT 1");
        trace.Steps.Should().NotBeEmpty();
        trace.Steps.Select(step => step.Operator).Should().Contain(["TableScan", "Filter", "Project", "Sort", "Limit"]);
        trace.Steps.First(step => step.Operator == "TableScan").Details.Should().Contain("users");
        trace.Steps.First(step => step.Operator == "Filter").Details.Should().Contain("Active");
        trace.Steps.First(step => step.Operator == "Project").Details.Should().Contain("items=Id");
        trace.Steps.First(step => step.Operator == "Sort").Details.Should().Contain("items=Id");
        trace.Steps.First(step => step.Operator == "Limit").Details.Should().Be("count=1");
        trace.Steps.First(step => step.Operator == "Limit").OutputRows.Should().Be(1);
    }

    /// <summary>
    /// EN: Ensures DebugSql keeps the captured trace available even when it auto-opens and auto-closes the connection.
    /// PT: Garante que DebugSql mantenha o trace capturado disponivel mesmo quando abre e fecha a conexao automaticamente.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSql_ShouldRetainTraceAfterAutoClose()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Seed("users", null,
            [1],
            [2]);

        cnn.State.Should().Be(ConnectionState.Closed);

        var trace = cnn.DebugSql("SELECT Id FROM users ORDER BY Id LIMIT 1");

        cnn.State.Should().Be(ConnectionState.Closed);
        trace.Should().BeSameAs(cnn.LastDebugTrace);
        cnn.LastDebugTraces.Should().ContainSingle().Which.Should().BeSameAs(trace);
    }

    /// <summary>
    /// EN: Ensures DebugSql captures a single UNION trace without leaking internal SELECT traces into the public result.
    /// PT: Garante que DebugSql capture um unico trace de UNION sem vazar traces internos de SELECT no resultado publico.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSql_ShouldCaptureSingleUnionTrace()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        var trace = cnn.DebugSql("""
            SELECT Id FROM users WHERE Active = 1
            UNION
            SELECT Id FROM users WHERE Active = 0
            ORDER BY Id
            LIMIT 2
            """);

        trace.QueryType.Should().Be(SqlConst.UNION);
        trace.Steps.Select(step => step.Operator).Should().Contain(["UnionInputs", "UnionCombine", "Sort", "Limit"]);
        trace.Steps.First(step => step.Operator == "UnionInputs").Details.Should().Contain("parts=2");
        trace.Steps.First(step => step.Operator == "UnionCombine").Details.Should().Contain("mode=UNION DISTINCT");
        trace.Steps.First(step => step.Operator == "Sort").Details.Should().Contain("items=Id ASC");
        cnn.LastDebugTraces.Should().ContainSingle().Which.Should().BeSameAs(trace);
    }

    /// <summary>
    /// EN: Ensures grouped debug traces expose diagnostic details for grouping, having, projection and distinct steps.
    /// PT: Garante que traces agrupados exponham detalhes diagnosticos para agrupamento, having, projecao e distinct.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSql_ShouldCaptureGroupedTraceDetails()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        var trace = cnn.DebugSql("""
            SELECT DISTINCT Active, COUNT(*) AS Total
            FROM users
            GROUP BY Active
            HAVING COUNT(*) >= 1
            ORDER BY Active
            """);

        trace.Steps.Select(step => step.Operator).Should().Contain(["Group", "Having", "Project", "Distinct", "Sort"]);
        trace.Steps.First(step => step.Operator == "Group").Details.Should().Contain("items=Active");
        trace.Steps.First(step => step.Operator == "Having").Details.Should().Contain("COUNT");
        trace.Steps.First(step => step.Operator == "Project").Details.Should().Contain("COUNT(*) AS Total");
        trace.Steps.First(step => step.Operator == "Distinct").Details.Should().Be("columns=2");
        trace.Steps.First(step => step.Operator == "Sort").Details.Should().Contain("items=Active ASC");
    }

    /// <summary>
    /// EN: Ensures batch debug returns all traces captured for a multi-statement reader execution.
    /// PT: Garante que o debug em lote retorne todos os traces capturados para uma execucao reader com multiplos statements.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSqlBatch_ShouldReturnAllCapturedTraces()
    {
        using var cnn = new SqliteConnectionMock();

        var traces = cnn.DebugSqlBatch("SELECT 1 AS Id; SELECT 2 AS Id;");

        traces.Should().HaveCount(2);
        traces.Select(trace => trace.QueryType).Should().Equal(SqlConst.SELECT, SqlConst.SELECT);
        traces.Select(trace => trace.StatementIndex).Should().Equal(0, 1);
        traces.Select(trace => trace.SqlText).Should().Equal("SELECT 1 AS Id", "SELECT 2 AS Id");
        cnn.LastDebugTraces.Should().HaveCount(2);
        cnn.LastDebugTraces.Should().Equal(traces);
        cnn.LastDebugTrace.Should().BeSameAs(traces[1]);
    }

    /// <summary>
    /// EN: Ensures debug trace retention can be capped without losing the latest captured trace.
    /// PT: Garante que a retencao de traces de debug possa ser limitada sem perder o ultimo trace capturado.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSqlBatch_ShouldRespectDebugTraceRetentionLimit()
    {
        using var cnn = new SqliteConnectionMock
        {
            DebugTraceRetentionLimit = 1
        };

        var traces = cnn.DebugSqlBatch("SELECT 1 AS Id; SELECT 2 AS Id;");

        traces.Should().HaveCount(1);
        traces[0].StatementIndex.Should().Be(1);
        traces[0].SqlText.Should().Be("SELECT 2 AS Id");
        cnn.LastDebugTrace.Should().BeSameAs(traces[0]);
    }

    /// <summary>
    /// EN: Ensures a new debug execution replaces prior trace history instead of accumulating across calls.
    /// PT: Garante que uma nova execucao de debug substitua o historico anterior em vez de acumular entre chamadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSqlBatch_ShouldResetPreviousTraceHistory_OnNewCapture()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.DebugSqlBatch("SELECT 1 AS Id; SELECT 2 AS Id;");

        var traces = cnn.DebugSqlBatch("SELECT 3 AS Id;");

        traces.Should().HaveCount(1);
        traces[0].StatementIndex.Should().Be(0);
        traces[0].SqlText.Should().Be("SELECT 3 AS Id");
        cnn.LastDebugTraces.Should().HaveCount(1);
        cnn.LastDebugTrace.Should().BeSameAs(traces[0]);
    }

    /// <summary>
    /// EN: Ensures captured debug traces can be cleared explicitly without affecting execution plans.
    /// PT: Garante que os traces de debug capturados possam ser limpos explicitamente sem afetar os planos de execucao.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ClearDebugTraces_ShouldResetOnlyDebugTraceHistory()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.DebugSql("SELECT 1 AS Id");

        cnn.LastDebugTrace.Should().NotBeNull();
        cnn.LastDebugTraces.Should().NotBeEmpty();

        cnn.ClearDebugTraces();

        cnn.LastDebugTrace.Should().BeNull();
        cnn.LastDebugTraces.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Ensures the current debug trace snapshot can be exported without reexecuting SQL.
    /// PT: Garante que o snapshot atual de traces de debug possa ser exportado sem reexecutar SQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void GetDebugTraceSnapshot_ShouldExportCurrentCapturedState()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.DebugSqlBatch("SELECT 1 AS Id; SELECT 2 AS Id;");

        var snapshot = cnn.GetDebugTraceSnapshot();
        var text = cnn.GetDebugTraceSnapshotText();
        var json = cnn.GetDebugTraceSnapshotJson();

        snapshot.Should().HaveCount(2);
        snapshot.Select(trace => trace.SqlText).Should().Equal("SELECT 1 AS Id", "SELECT 2 AS Id");
        text.Should().Contain("Query Debug Trace Batch");
        text.Should().Contain("SqlText: SELECT 1 AS Id");
        text.Should().Contain("SqlText: SELECT 2 AS Id");

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        root.GetProperty("traceCount").GetInt32().Should().Be(2);
        root.GetProperty("traces")[1].GetProperty("sqlText").GetString().Should().Be("SELECT 2 AS Id");
    }

    /// <summary>
    /// EN: Ensures the last retained debug trace can be exported directly without reexecuting SQL.
    /// PT: Garante que o ultimo trace de debug retido possa ser exportado diretamente sem reexecutar SQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void GetLastDebugTraceSnapshot_ShouldExportCurrentLastTrace()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.DebugSqlBatch("SELECT 1 AS Id; SELECT 2 AS Id;");

        var trace = cnn.GetLastDebugTraceSnapshot();
        var text = cnn.GetLastDebugTraceSnapshotText();
        var json = cnn.GetLastDebugTraceSnapshotJson();

        trace.Should().NotBeNull();
        trace!.StatementIndex.Should().Be(1);
        trace.SqlText.Should().Be("SELECT 2 AS Id");
        text.Should().Contain("Query Debug Trace");
        text.Should().Contain("- StatementIndex: 1");
        text.Should().Contain("- SqlText: SELECT 2 AS Id");

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        root.GetProperty("statementIndex").GetInt32().Should().Be(1);
        root.GetProperty("sqlText").GetString().Should().Be("SELECT 2 AS Id");
    }

    /// <summary>
    /// EN: Ensures the non-throwing last-trace API reports availability explicitly.
    /// PT: Garante que a API sem excecao para o ultimo trace informe explicitamente a disponibilidade.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void TryGetLastDebugTraceSnapshot_ShouldReportAvailability()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.TryGetLastDebugTraceSnapshot(out var emptyTrace).Should().BeFalse();
        emptyTrace.Should().BeNull();

        cnn.DebugSql("SELECT 1 AS Id");

        cnn.TryGetLastDebugTraceSnapshot(out var trace).Should().BeTrue();
        trace.Should().NotBeNull();
        trace!.SqlText.Should().Be("SELECT 1 AS Id");
    }

    /// <summary>
    /// EN: Ensures snapshot APIs behave predictably when no debug trace is retained.
    /// PT: Garante que as APIs de snapshot se comportem de forma previsivel quando nenhum trace de debug estiver retido.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugTraceSnapshotApis_ShouldHandleEmptyState()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.GetDebugTraceSnapshot().Should().BeEmpty();
        cnn.GetLastDebugTraceSnapshot().Should().BeNull();
        cnn.GetDebugTraceSnapshotText().Should().Contain("TraceCount: 0");

        using (var batchDoc = JsonDocument.Parse(cnn.GetDebugTraceSnapshotJson()))
        {
            batchDoc.RootElement.GetProperty("traceCount").GetInt32().Should().Be(0);
        }

        var textAction = () => cnn.GetLastDebugTraceSnapshotText();
        var jsonAction = () => cnn.GetLastDebugTraceSnapshotJson();

        textAction.Should().Throw<InvalidOperationException>()
            .WithMessage("No runtime debug trace is currently retained.");
        jsonAction.Should().Throw<InvalidOperationException>()
            .WithMessage("No runtime debug trace is currently retained.");
    }

    /// <summary>
    /// EN: Ensures the connection can return a formatted debug trace directly for ad-hoc inspection.
    /// PT: Garante que a conexao consiga retornar diretamente um trace de debug formatado para inspecao ad-hoc.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSqlText_ShouldReturnFormattedTrace()
    {
        using var cnn = new SqliteConnectionMock();

        var text = cnn.DebugSqlText("SELECT 1 AS Id");

        text.Should().Contain("Query Debug Trace");
        text.Should().Contain("- QueryType: SELECT");
        text.Should().Contain("- SqlText: SELECT 1 AS Id");
        text.Should().Contain("- Steps:");
    }

    /// <summary>
    /// EN: Ensures the connection can return a structured JSON debug trace directly for automation.
    /// PT: Garante que a conexao consiga retornar diretamente um trace de debug JSON estruturado para automacao.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSqlJson_ShouldReturnStructuredTrace()
    {
        using var cnn = new SqliteConnectionMock();

        var json = cnn.DebugSqlJson("SELECT 1 AS Id");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        root.GetProperty("queryType").GetString().Should().Be(SqlConst.SELECT);
        root.GetProperty("statementIndex").GetInt32().Should().Be(0);
        root.GetProperty("sqlText").GetString().Should().Be("SELECT 1 AS Id");
        root.GetProperty("stepCount").GetInt32().Should().BeGreaterThan(0);
    }

    /// <summary>
    /// EN: Ensures the connection can return formatted batch debug text with aggregated batch metadata.
    /// PT: Garante que a conexao consiga retornar texto de debug em lote com metadados agregados do batch.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSqlBatchText_ShouldReturnFormattedBatchTrace()
    {
        using var cnn = new SqliteConnectionMock();

        var text = cnn.DebugSqlBatchText("SELECT 1 AS Id; SELECT 2 AS Id;");

        text.Should().Contain("Query Debug Trace Batch");
        text.Should().Contain("TraceCount: 2");
        text.Should().Contain("FastestStatementIndex:");
        text.Should().MatchRegex(@"FastestStatementIndex: [01]\r?\n- FastestStatementSql: SELECT [12] AS Id");
        text.Should().Contain("NarrowestStatementIndex: 0");
        text.Should().Contain("SqlText: SELECT 1 AS Id");
        text.Should().Contain("SqlText: SELECT 2 AS Id");
    }

    /// <summary>
    /// EN: Ensures the connection can return structured batch debug JSON with aggregated batch metadata.
    /// PT: Garante que a conexao consiga retornar JSON estruturado de debug em lote com metadados agregados do batch.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void DebugSqlBatchJson_ShouldReturnStructuredBatchTrace()
    {
        using var cnn = new SqliteConnectionMock();

        var json = cnn.DebugSqlBatchJson("SELECT 1 AS Id; SELECT 2 AS Id;");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        root.GetProperty("traceCount").GetInt32().Should().Be(2);
        var fastestStatementIndex = root.GetProperty("fastestStatementIndex").GetInt32();
        var fastestStatementSql = root.GetProperty("fastestStatementSql").GetString();
        fastestStatementIndex.Should().BeOneOf(0, 1);
        fastestStatementSql.Should().Be(fastestStatementIndex == 0 ? "SELECT 1 AS Id" : "SELECT 2 AS Id");
        root.GetProperty("narrowestStatementIndex").GetInt32().Should().Be(0);
        root.GetProperty("narrowestStatementSql").GetString().Should().Be("SELECT 1 AS Id");
        root.GetProperty("traces")[1].GetProperty("sqlText").GetString().Should().Be("SELECT 2 AS Id");
    }

    /// <summary>
    /// EN: Ensures execution plan suggests missing index for filter/sort columns.
    /// PT: Garante que o plano de execução sugira índice ausente para colunas de filtro/ordenação.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldSuggestMissingIndex_WhenNoMatchingIndexExists()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.IndexRecommendationsLabel()}:");
        cnn.LastExecutionPlan.Should().Contain("CREATE INDEX IX_users_Active_Id ON users (Active, Id);");
    }



    /// <summary>
    /// EN: Ensures index recommendations include estimated before/after and gain metrics.
    /// PT: Garante que recomendações de índice incluam métricas estimadas de antes/depois e ganho.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldIncludeEstimatedGainMetrics_WhenRecommendingIndex()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().MatchRegex(@"(EstimatedRowsReadBefore|LinhasEstimadasLidasAntes):");
        cnn.LastExecutionPlan.Should().MatchRegex(@"(EstimatedRowsReadAfter|LinhasEstimadasLidasDepois):");
        cnn.LastExecutionPlan.Should().MatchRegex(@"(EstimatedGainPct|GanhoEstimadoPct):");
    }



    /// <summary>
    /// EN: Ensures advisor skips recommendation for tiny scans to reduce noise.
    /// PT: Garante que o advisor não recomende índice para scans muito pequenos, reduzindo ruído.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotSuggestMissingIndex_WhenEstimatedRowsReadIsTooLow()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0]);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"hasIndexRecommendations:false");
    }

    /// <summary>
    /// EN: Ensures execution plan does not suggest index when a matching index already exists.
    /// PT: Garante que o plano não sugira índice quando já existe índice aderente.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotSuggestMissingIndex_WhenMatchingIndexAlreadyExists()
    {
        using var cnn = new SqliteConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.DefineTable("users").Index("ix_users_active_id", ["Active", "Id"]);
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"hasIndexRecommendations:false");
    }

}

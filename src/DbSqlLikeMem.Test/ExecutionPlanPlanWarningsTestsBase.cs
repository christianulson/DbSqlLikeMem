using System.Data.Common;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Provides reusable execution-plan warning assertions across provider-specific test suites.
/// PT: Fornece asserções reutilizáveis de alertas de plano de execução entre suítes específicas de provedores.
/// </summary>
/// <param name="helper">EN: xUnit output helper for diagnostics. PT: Helper de saída do xUnit para diagnósticos.</param>
public abstract class ExecutionPlanPlanWarningsTestsBase(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Creates the provider-specific connection used in each scenario.
    /// PT: Cria a conexão específica do provedor usada em cada cenário.
    /// </summary>
    protected abstract DbConnectionMockBase CreateConnection();
    /// <summary>
    /// EN: Creates a provider-specific command for the given SQL text.
    /// PT: Cria um comando específico do provedor para o texto SQL informado.
    /// </summary>
    protected abstract DbCommand CreateCommand(DbConnectionMockBase connection, string commandText);
    /// <summary>
    /// EN: Gets ORDER BY SQL that also applies row limiting for the provider.
    /// PT: Obtém SQL com ORDER BY que também aplica limitação de linhas para o provedor.
    /// </summary>
    protected abstract string SelectOrderByWithLimitSql { get; }
    /// <summary>
    /// EN: Verifies PW001 is emitted for ORDER BY without row limit and high reads.
    /// PT: Verifica que PW001 é emitido para ORDER BY sem limite de linhas e com alta leitura.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW001_WhenOrderByHasNoLimitAndHighRead()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT Id FROM users ORDER BY Id");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW001");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.MessageLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.ReasonLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.SuggestedActionLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.SeverityLabel()}: {SqlExecutionPlanMessages.SeverityHighValue()}");
    }
    /// <summary>
    /// EN: Verifies PW001 is not emitted when row limit is present.
    /// PT: Verifica que PW001 não é emitido quando há limite de linhas.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarningPW001_WhenLimitIsPresent()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, SelectOrderByWithLimitSql);
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain($"{SqlExecutionPlanMessages.CodeLabel()}: PW001");
    }
    /// <summary>
    /// EN: Verifies PW002 is emitted for low-selectivity predicates with high reads.
    /// PT: Verifica que PW002 é emitido para predicados de baixa seletividade com alta leitura.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW002_WhenSelectivityIsLowAndHighRead()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT Id FROM users WHERE Active = 1");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW002");
    }
    /// <summary>
    /// EN: Verifies PW002 is not emitted when predicate selectivity is high.
    /// PT: Verifica que PW002 não é emitido quando a seletividade do predicado é alta.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarningPW002_WhenSelectivityIsHigh()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, i => i == 1 ? 1 : 0);

        using var cmd = CreateCommand(cnn, "SELECT Id FROM users WHERE Active = 1");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain($"{SqlExecutionPlanMessages.CodeLabel()}: PW002");
    }
    /// <summary>
    /// EN: Verifies PW003 is emitted for SELECT * under high-read conditions.
    /// PT: Verifica que PW003 é emitido para SELECT * sob condição de alta leitura.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW003_WhenSelectStarHasHighRead()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT * FROM users");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW003");
    }
    /// <summary>
    /// EN: Verifies PW003 is not emitted when projection is explicit.
    /// PT: Verifica que PW003 não é emitido quando a projeção é explícita.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarningPW003_WhenProjectionIsExplicit()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT Id FROM users");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain($"{SqlExecutionPlanMessages.CodeLabel()}: PW003");
    }
    /// <summary>
    /// EN: Verifies warning metadata appears in stable key order.
    /// PT: Verifica que os metadados de alerta aparecem em ordem estável de chaves.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldKeepWarningMetadataInStableOrder()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT DISTINCT Id FROM users");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        var plan = cnn.LastExecutionPlan;
        plan.Should().NotBeNullOrWhiteSpace();

        var idxCode = plan!.IndexOf($"{SqlExecutionPlanMessages.CodeLabel()}: PW005", StringComparison.Ordinal);
        var idxMessage = plan.IndexOf($"{SqlExecutionPlanMessages.MessageLabel()}:", idxCode, StringComparison.Ordinal);
        var idxReason = plan.IndexOf($"{SqlExecutionPlanMessages.ReasonLabel()}:", idxMessage, StringComparison.Ordinal);
        var idxAction = plan.IndexOf($"{SqlExecutionPlanMessages.SuggestedActionLabel()}:", idxReason, StringComparison.Ordinal);
        var idxSeverity = plan.IndexOf($"{SqlExecutionPlanMessages.SeverityLabel()}:", idxAction, StringComparison.Ordinal);
        var idxMetric = plan.IndexOf($"{SqlExecutionPlanMessages.MetricNameLabel()}:", idxSeverity, StringComparison.Ordinal);
        var idxObserved = plan.IndexOf($"{SqlExecutionPlanMessages.ObservedValueLabel()}:", idxMetric, StringComparison.Ordinal);
        var idxThreshold = plan.IndexOf($"{SqlExecutionPlanMessages.ThresholdLabel()}:", idxObserved, StringComparison.Ordinal);

        idxCode.Should().BeGreaterThan(-1);
        idxMessage.Should().BeGreaterThan(idxCode);
        idxReason.Should().BeGreaterThan(idxMessage);
        idxAction.Should().BeGreaterThan(idxReason);
        idxSeverity.Should().BeGreaterThan(idxAction);
        idxMetric.Should().BeGreaterThan(idxSeverity);
        idxObserved.Should().BeGreaterThan(idxMetric);
        idxThreshold.Should().BeGreaterThan(idxObserved);
    }
    /// <summary>
    /// EN: Verifies threshold values follow the expected technical pattern.
    /// PT: Verifica que valores de threshold seguem o padrão técnico esperado.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldFormatThresholdsInTechnicalPattern()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 5000, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT DISTINCT * FROM users ORDER BY Id");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        var thresholds = cnn.LastExecutionPlan!
            .Split(new[] { Environment.NewLine }, StringSplitOptions.None)
            .Select(static line => line.Trim())
            .Where(static line => line.StartsWith($"{SqlExecutionPlanMessages.ThresholdLabel()}:", StringComparison.Ordinal))
            .Select(static line => line[$"{SqlExecutionPlanMessages.ThresholdLabel()}:".Length..].Trim())
            .ToList();

        thresholds.Should().NotBeEmpty();
        var pattern = new Regex(@"^[a-zA-Z]+:\d+(\.\d+)?(?:;[a-zA-Z]+:\d+(\.\d+)?)*$", RegexOptions.CultureInvariant);
        thresholds.Should().OnlyContain(t => pattern.IsMatch(t));
    }
    /// <summary>
    /// EN: Verifies PW004 is suppressed when DISTINCT already explains high read without WHERE.
    /// PT: Verifica que PW004 é suprimido quando DISTINCT já explica alta leitura sem WHERE.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldSuppressPW004_WhenDistinctAlreadyExplainsHighReadNoWhere()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT DISTINCT Id FROM users");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW005");
        cnn.LastExecutionPlan.Should().NotContain($"{SqlExecutionPlanMessages.CodeLabel()}: PW004");
    }
    /// <summary>
    /// EN: Verifies PW004 remains when query has no WHERE and no DISTINCT.
    /// PT: Verifica que PW004 permanece quando a consulta não tem WHERE nem DISTINCT.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldKeepPW004_WhenNoWhereAndNotDistinct()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT Id FROM users");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW004");
    }
    /// <summary>
    /// EN: Verifies PW005 is kept and PW004 is suppressed when WHERE and DISTINCT coexist.
    /// PT: Verifica que PW005 é mantido e PW004 suprimido quando WHERE e DISTINCT coexistem.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldKeepPW005AndSuppressPW004_WhenWhereAndDistinct()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT DISTINCT Id FROM users WHERE Active = 1");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW005");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW002");
        cnn.LastExecutionPlan.Should().NotContain($"{SqlExecutionPlanMessages.CodeLabel()}: PW004");
    }
    /// <summary>
    /// EN: Verifies PW005 is not emitted when DISTINCT is absent.
    /// PT: Verifica que PW005 não é emitido quando DISTINCT está ausente.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPW005_WhenNoDistinct()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT Id FROM users");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain($"{SqlExecutionPlanMessages.CodeLabel()}: PW005");
    }
    /// <summary>
    /// EN: Verifies PW002 emits stable technical threshold metadata.
    /// PT: Verifica que PW002 emite metadados técnicos de threshold estáveis.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitStableTechnicalThresholdMetadata_ForPW002()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT Id FROM users WHERE Active = 1");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        var warningBlock = ExtractWarningBlock(cnn.LastExecutionPlan!, "PW002");
        warningBlock.Should().Contain($"{SqlExecutionPlanMessages.MetricNameLabel()}: SelectivityPct");
        warningBlock.Should().Contain($"{SqlExecutionPlanMessages.ThresholdLabel()}: gte:60;highImpactGte:85");
    }
    /// <summary>
    /// EN: Verifies PW004 and PW005 emit stable technical threshold metadata.
    /// PT: Verifica que PW004 e PW005 emitem metadados técnicos de threshold estáveis.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitStableTechnicalThresholdMetadata_ForPW004AndPW005()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var pw004Cmd = CreateCommand(cnn, "SELECT Id FROM users");
        using var pw004Reader = pw004Cmd.ExecuteReader();
        while (pw004Reader.Read()) { }

        var pw004Block = ExtractWarningBlock(cnn.LastExecutionPlan!, "PW004");
        pw004Block.Should().Contain($"{SqlExecutionPlanMessages.MetricNameLabel()}: EstimatedRowsRead");
        pw004Block.Should().Contain($"{SqlExecutionPlanMessages.ThresholdLabel()}: gte:100;highGte:5000");

        using var pw005Cmd = CreateCommand(cnn, "SELECT DISTINCT Id FROM users");
        using var pw005Reader = pw005Cmd.ExecuteReader();
        while (pw005Reader.Read()) { }

        var pw005Block = ExtractWarningBlock(cnn.LastExecutionPlan!, "PW005");
        pw005Block.Should().Contain($"{SqlExecutionPlanMessages.MetricNameLabel()}: EstimatedRowsRead");
        pw005Block.Should().Contain($"{SqlExecutionPlanMessages.ThresholdLabel()}: gte:100;highGte:5000");
    }
    /// <summary>
    /// EN: Verifies index recommendations are preserved when warnings are present.
    /// PT: Verifica que recomendações de índice são preservadas quando há alertas.
    /// </summary>


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldKeepIndexRecommendations_WhenPlanWarningsArePresent()
    {
        using var cnn = CreateConnection();
        SeedUsers(cnn, 120, _ => 1);

        using var cmd = CreateCommand(cnn, "SELECT Id FROM users WHERE Active = 1 ORDER BY Id");
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW001");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.CodeLabel()}: PW002");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.IndexRecommendationsLabel()}:");
    }

    private static string[] ExtractWarningBlock(string plan, string code)
    {
        var lines = plan
            .Split(new[] { Environment.NewLine }, StringSplitOptions.None)
            .Select(static line => line.Trim())
            .ToArray();

        var start = Array.FindIndex(lines, line => line == $"- {SqlExecutionPlanMessages.CodeLabel()}: {code}");
        start.Should().BeGreaterThanOrEqualTo(0);

        var nextCodeOffset = -1;
        for (var i = start + 1; i < lines.Length; i++)
        {
            if (lines[i].StartsWith($"- {SqlExecutionPlanMessages.CodeLabel()}:", StringComparison.Ordinal))
            {
                nextCodeOffset = i - start;
                break;
            }
        }

        if (nextCodeOffset >= 0)
            return lines.Skip(start).Take(nextCodeOffset).ToArray();

        return lines.Skip(start).ToArray();
    }

    /// <summary>
    /// EN: Seeds the users table with deterministic Active values for warning scenarios.
    /// PT: Popula a tabela users com valores determinísticos de Active para cenários de alerta.
    /// </summary>
    protected static void SeedUsers(DbConnectionMockBase cnn, int totalRows, Func<int, int> activeSelector)
    {
        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");

        var rows = new object?[totalRows][];
        for (var i = 1; i <= totalRows; i++)
            rows[i - 1] = [i, activeSelector(i)];

        cnn.Seed("users", null, rows);
    }
}

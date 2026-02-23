using System.Data.Common;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem.Test;

public abstract class ExecutionPlanPlanWarningsTestsBase(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    protected abstract DbConnectionMockBase CreateConnection();
    protected abstract DbCommand CreateCommand(DbConnectionMockBase connection, string commandText);
    protected abstract string SelectOrderByWithLimitSql { get; }

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

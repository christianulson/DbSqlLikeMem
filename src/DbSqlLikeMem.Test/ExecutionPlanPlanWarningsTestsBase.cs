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

        cnn.LastExecutionPlan.Should().Contain("Code: PW001");
        cnn.LastExecutionPlan.Should().Contain("Message:");
        cnn.LastExecutionPlan.Should().Contain("Reason:");
        cnn.LastExecutionPlan.Should().Contain("SuggestedAction:");
        cnn.LastExecutionPlan.Should().Contain("Severity: High");
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

        cnn.LastExecutionPlan.Should().NotContain("Code: PW001");
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

        cnn.LastExecutionPlan.Should().Contain("Code: PW002");
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

        cnn.LastExecutionPlan.Should().NotContain("Code: PW002");
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

        cnn.LastExecutionPlan.Should().Contain("Code: PW003");
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

        cnn.LastExecutionPlan.Should().NotContain("Code: PW003");
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

        var idxCode = plan!.IndexOf("Code: PW005", StringComparison.Ordinal);
        var idxMessage = plan.IndexOf("Message:", idxCode, StringComparison.Ordinal);
        var idxReason = plan.IndexOf("Reason:", idxMessage, StringComparison.Ordinal);
        var idxAction = plan.IndexOf("SuggestedAction:", idxReason, StringComparison.Ordinal);
        var idxSeverity = plan.IndexOf("Severity:", idxAction, StringComparison.Ordinal);
        var idxMetric = plan.IndexOf("MetricName:", idxSeverity, StringComparison.Ordinal);
        var idxObserved = plan.IndexOf("ObservedValue:", idxMetric, StringComparison.Ordinal);
        var idxThreshold = plan.IndexOf("Threshold:", idxObserved, StringComparison.Ordinal);

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
            .Where(static line => line.StartsWith("Threshold:", StringComparison.Ordinal))
            .Select(static line => line["Threshold:".Length..].Trim())
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

        cnn.LastExecutionPlan.Should().Contain("Code: PW005");
        cnn.LastExecutionPlan.Should().NotContain("Code: PW004");
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

        cnn.LastExecutionPlan.Should().Contain("Code: PW004");
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

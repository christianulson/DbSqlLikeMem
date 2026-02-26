using System.Xml.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Validates execution-plan warning formatting and i18n resource consistency.
/// PT: Valida a formatação de alertas do plano de execução e a consistência de recursos de i18n.
/// </summary>
public sealed class ExecutionPlanFormattingAndI18nTests
{
    private static readonly Regex TechnicalThresholdPattern = new(
        @"^[a-zA-Z]+:\d+(\.\d+)?(?:;[a-zA-Z]+:\d+(\.\d+)?)*$",
        RegexOptions.CultureInvariant);

    /// <summary>
    /// EN: Verifies warning metadata is rendered in deterministic key order.
    /// PT: Verifica que os metadados de alerta são renderizados em ordem determinística de chaves.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldPrintPlanWarningMetadataInStableOrder()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warning = new SqlPlanWarning(
            "PWX",
            "message",
            "reason",
            "action",
            SqlPlanWarningSeverity.Warning,
            "EstimatedRowsRead",
            "120",
            "gte:100;highGte:5000");

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, [warning]);
        var lines = plan.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

        var warningStart = Array.FindIndex(lines, l => l.Contains($"{SqlExecutionPlanMessages.CodeLabel()}: PWX", StringComparison.Ordinal));
        warningStart.Should().BeGreaterThan(-1);

        var warningBlock = lines
            .Skip(warningStart)
            .Take(8)
            .Select(static line => line.Trim())
            .ToArray();

        warningBlock.Should().Equal(
            $"- {SqlExecutionPlanMessages.CodeLabel()}: PWX",
            $"{SqlExecutionPlanMessages.MessageLabel()}: message",
            $"{SqlExecutionPlanMessages.ReasonLabel()}: reason",
            $"{SqlExecutionPlanMessages.SuggestedActionLabel()}: action",
            $"{SqlExecutionPlanMessages.SeverityLabel()}: {SqlExecutionPlanMessages.SeverityWarningValue()}",
            $"{SqlExecutionPlanMessages.MetricNameLabel()}: EstimatedRowsRead",
            $"{SqlExecutionPlanMessages.ObservedValueLabel()}: 120",
            $"{SqlExecutionPlanMessages.ThresholdLabel()}: gte:100;highGte:5000");
    }

    /// <summary>
    /// EN: Verifies localized execution-plan resources keep base keys and canonical SQL keywords.
    /// PT: Verifica que recursos localizados de plano de execução mantêm chaves base e palavras-chave SQL canônicas.
    /// </summary>
    [Fact]
    public void SqlExecutionPlanMessages_AllLocalizedResxShouldContainBaseKeys_AndKeepCanonicalSqlKeywords()
    {
        var basePath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "DbSqlLikeMem", "Resources");
        var baseResx = Path.Combine(basePath, "SqlExecutionPlanMessages.resx");

        var baseEntries = LoadResxEntries(baseResx);
        var messageKeysUsedByCode = typeof(SqlExecutionPlanMessages)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Select(static method => method.Name)
            .ToHashSet(StringComparer.Ordinal);

        baseEntries.Keys.Should().Contain(messageKeysUsedByCode);
        var localizedFiles = Directory
            .EnumerateFiles(basePath, "SqlExecutionPlanMessages.*.resx", SearchOption.TopDirectoryOnly)
            .Where(path => !path.EndsWith("SqlExecutionPlanMessages.resx", StringComparison.Ordinal))
            .ToArray();

        localizedFiles.Should().NotBeEmpty("localized execution plan resources must exist");

        foreach (var localized in localizedFiles)
        {
            var localizedEntries = LoadResxEntries(localized);
            localizedEntries.Keys.Should().Contain(baseEntries.Keys, $"Missing keys in {Path.GetFileName(localized)}");

            var canonicalKeywordKeys = new Dictionary<string, string[]>(StringComparer.Ordinal)
            {
                ["WarningOrderByWithoutLimitMessage"] = ["ORDER BY", "LIMIT", "TOP", "FETCH"],
                ["WarningOrderByWithoutLimitAction"] = ["LIMIT", "TOP", "FETCH"],
                ["WarningNoWhereHighReadMessage"] = ["WHERE"],
                ["WarningNoWhereHighReadHighImpactMessage"] = ["WHERE"],
                ["WarningNoWhereHighReadAction"] = ["WHERE"],
                ["WarningDistinctHighReadMessage"] = ["DISTINCT"],
                ["WarningDistinctHighReadHighImpactMessage"] = ["DISTINCT"],
                ["WarningDistinctHighReadAction"] = ["DISTINCT"],
                ["WarningSelectStarMessage"] = ["SELECT *"],
                ["WarningSelectStarHighImpactMessage"] = ["SELECT *"],
                ["WarningSelectStarCriticalImpactMessage"] = ["SELECT *"],
                ["WarningSelectStarAction"] = ["SELECT *"]
            };

            foreach (var pair in canonicalKeywordKeys)
            {
                var key = pair.Key;
                var expectedTokens = pair.Value;
                var value = localizedEntries[key];
                expectedTokens.Any(token => value.IndexOf(token, StringComparison.Ordinal) >= 0).Should().BeTrue($"{key} should preserve canonical SQL keyword tokens");
            }
        }
    }

    /// <summary>
    /// EN: Verifies threshold metadata stays in stable machine-parseable format.
    /// PT: Verifica que metadados de threshold permanecem em formato estável legível por máquina.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldKeepThresholdInTechnicalParseablePattern()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning, "EstimatedRowsRead", "120", "gte:100;highGte:5000"),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning, "SelectivityPct", "85.00", "gte:60;highImpactGte:85")
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        var thresholds = plan
            .Split(new[] { Environment.NewLine }, StringSplitOptions.None)
            .Where(line => line.Contains($"{SqlExecutionPlanMessages.ThresholdLabel()}:", StringComparison.Ordinal))
            .Select(line => line[(line.IndexOf(':') + 1)..].Trim())
            .ToList();

        thresholds.Should().NotBeEmpty();
        thresholds.Should().OnlyContain(value => TechnicalThresholdPattern.IsMatch(value));
    }

    private static Dictionary<string, string> LoadResxEntries(string path)
    {
        var doc = XDocument.Load(path);
        return doc.Root!
            .Elements("data")
            .Where(static d => d.Attribute("name") is not null)
            .ToDictionary(
                d => d.Attribute("name")!.Value,
                d => d.Element("value")?.Value ?? string.Empty,
                StringComparer.Ordinal);
    }
}

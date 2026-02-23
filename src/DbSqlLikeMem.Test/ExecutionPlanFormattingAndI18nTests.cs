using System.Xml.Linq;

namespace DbSqlLikeMem.Test;

public sealed class ExecutionPlanFormattingAndI18nTests
{
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

        var warningLines = lines.Where(l => l.IndexOf(':') >= 0).ToList();
        warningLines.Should().ContainInConsecutiveOrder(
            warningLines.Single(l => l.Contains("Code:", StringComparison.Ordinal)),
            warningLines.Single(l => l.Contains("Message:", StringComparison.Ordinal)),
            warningLines.Single(l => l.Contains("Reason:", StringComparison.Ordinal)),
            warningLines.Single(l => l.Contains("SuggestedAction:", StringComparison.Ordinal)),
            warningLines.Single(l => l.Contains("Severity:", StringComparison.Ordinal)),
            warningLines.Single(l => l.Contains("MetricName:", StringComparison.Ordinal)),
            warningLines.Single(l => l.Contains("ObservedValue:", StringComparison.Ordinal)),
            warningLines.Single(l => l.Contains("Threshold:", StringComparison.Ordinal)));
    }

    [Fact]
    public void SqlExecutionPlanMessages_AllLocalizedResxShouldContainBaseKeys_AndKeepCanonicalSqlKeywords()
    {
        var basePath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "DbSqlLikeMem", "Resources");
        var baseResx = Path.Combine(basePath, "SqlExecutionPlanMessages.resx");

        var baseEntries = LoadResxEntries(baseResx);
        var localizedFiles = new[] { "de", "es", "fr", "it", "pt" }
            .Select(c => Path.Combine(basePath, $"SqlExecutionPlanMessages.{c}.resx"));

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

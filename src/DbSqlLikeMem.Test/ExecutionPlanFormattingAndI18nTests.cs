using System.Xml.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Validates execution-plan warning formatting and i18n resource consistency.
/// PT: Valida a formatação de alertas do plano de execução e a consistência de recursos de i18n.
/// </summary>
public sealed class ExecutionPlanFormattingAndI18nTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private static readonly Regex TechnicalThresholdPattern = new(
        @"^[a-zA-Z]+:\d+(\.\d+)?(?:;[a-zA-Z]+:\d+(\.\d+)?)*$",
        RegexOptions.CultureInvariant);

    private static readonly Regex CorrelationIdPattern = new(
        @"^[0-9a-f]{32}$",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

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
        var basePath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "DbSqlLikeMem", "Resources");
        var baseResx = Path.Combine(basePath, "SqlExecutionPlanMessages.resx");

        var baseEntries = LoadResxEntries(baseResx);
        var messageKeysUsedByCode = new HashSet<string>( typeof(SqlExecutionPlanMessages)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Select(static method => method.Name), StringComparer.Ordinal);

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
                ["WarningOrderByWithoutLimitMessage"] = ["ORDER BY", SqlConst.LIMIT, SqlConst.TOP, SqlConst.FETCH],
                ["WarningOrderByWithoutLimitAction"] = [SqlConst.LIMIT, SqlConst.TOP, SqlConst.FETCH],
                ["WarningNoWhereHighReadMessage"] = [SqlConst.WHERE],
                ["WarningNoWhereHighReadHighImpactMessage"] = [SqlConst.WHERE],
                ["WarningNoWhereHighReadAction"] = [SqlConst.WHERE],
                ["WarningDistinctHighReadMessage"] = [SqlConst.DISTINCT],
                ["WarningDistinctHighReadHighImpactMessage"] = [SqlConst.DISTINCT],
                ["WarningDistinctHighReadAction"] = [SqlConst.DISTINCT],
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


    /// <summary>
    /// EN: Verifies plan risk score is emitted and capped at 100 when warnings are present.
    /// PT: Verifica que o score de risco do plano é emitido e limitado em 100 quando há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanRiskScore_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Warning)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanRiskScore: 100");
    }

    /// <summary>
    /// EN: Verifies plan risk score is omitted when warnings are absent.
    /// PT: Verifica que o score de risco do plano é omitido quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanRiskScore_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanRiskScore:");
    }


    /// <summary>
    /// EN: Verifies quality grade is emitted when warnings are present.
    /// PT: Verifica que a nota qualitativa do plano é emitida quando há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanQualityGrade_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanQualityGrade: C");
    }

    /// <summary>
    /// EN: Verifies quality grade is omitted when warnings are absent.
    /// PT: Verifica que a nota qualitativa do plano é omitida quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanQualityGrade_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanQualityGrade:");
    }

    /// <summary>
    /// EN: Verifies schema-qualified table-valued sources are rendered with the schema name in the execution plan.
    /// PT: Verifica se fontes tabulares qualificadas por schema sao renderizadas com o nome do schema no plano de execucao.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderSchemaQualifiedTableFunctionSource()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("j.value", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "j",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("OPENJSON", [new IdentifierExpr("payload")]))
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- FROM: dbo.OPENJSON(...) AS j");
    }

    /// <summary>
    /// EN: Verifies OPENJSON sources with explicit WITH schema keep that shape in the execution plan.
    /// PT: Verifica se fontes OPENJSON com schema explicito em WITH mantem esse shape no plano de execucao.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderOpenJsonWithClauseInTableFunctionSource()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("data.Name", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "data",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("OPENJSON", [new IdentifierExpr("payload")]),
                OpenJsonWithClause: new SqlOpenJsonWithClause(
                [
                    new SqlOpenJsonWithColumn("Name", "NVARCHAR(20)", DbType.String, "$.Name", false)
                ]))
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- FROM: dbo.OPENJSON(...) WITH (...) AS data");
    }

    /// <summary>
    /// EN: Verifies STRING_SPLIT sources with enable_ordinal keep that shape in the execution plan.
    /// PT: Verifica se fontes STRING_SPLIT com enable_ordinal mantem esse shape no plano de execucao.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderStringSplitEnableOrdinalInTableFunctionSource()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("part.value", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "part",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("STRING_SPLIT", [new IdentifierExpr("payload"), new LiteralExpr(","), new LiteralExpr(1)]))
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- FROM: dbo.STRING_SPLIT(..., ..., enable_ordinal) AS part");
    }

    /// <summary>
    /// EN: Verifies OPENJSON sources with an explicit path keep that shape in the execution plan.
    /// PT: Verifica se fontes OPENJSON com path explicito mantem esse shape no plano de execucao.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderOpenJsonPathInTableFunctionSource()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("j.value", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "j",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("OPENJSON", [new IdentifierExpr("payload"), new LiteralExpr("strict $.items[1]")]))
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- FROM: dbo.OPENJSON(..., strict path) AS j");
    }

    /// <summary>
    /// EN: Verifies OPENJSON sources keep strict-path and WITH shape together in the execution plan.
    /// PT: Verifica se fontes OPENJSON mantem juntos os detalhes de strict-path e WITH no plano de execucao.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderOpenJsonStrictPathWithClauseInTableFunctionSource()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("data.Name", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "data",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("OPENJSON", [new IdentifierExpr("payload"), new LiteralExpr("strict $.items[1]")]),
                OpenJsonWithClause: new SqlOpenJsonWithClause(
                [
                    new SqlOpenJsonWithColumn("Name", "NVARCHAR(20)", DbType.String, "$.Name", false)
                ]))
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- FROM: dbo.OPENJSON(..., strict path) WITH (...) AS data");
    }

    /// <summary>
    /// EN: Verifies JSON_TABLE sources keep the row-path and COLUMNS shape in the execution plan.
    /// PT: Verifica se fontes JSON_TABLE mantem o shape de row-path e COLUMNS no plano de execucao.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderJsonTableColumnsShapeInTableFunctionSource()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("jt.Name", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "jt",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr(SqlConst.JSON_TABLE, [new IdentifierExpr("payload"), new LiteralExpr("strict $.items[*]")]),
                JsonTableClause: new SqlJsonTableClause(
                [
                    new SqlJsonTableColumn("ord", "BIGINT", DbType.Int64, null, true),
                    new SqlJsonTableColumn("Name", "VARCHAR(20)", DbType.String, "$.name", false)
                ]))
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- FROM: dbo.JSON_TABLE(..., strict path) COLUMNS (...) AS jt");
    }

    /// <summary>
    /// EN: Verifies nested JSON_TABLE sources keep the row-path and nested COLUMNS shape in the execution plan.
    /// PT: Verifica se fontes JSON_TABLE aninhadas mantem o shape de row-path e COLUMNS aninhados no plano de execucao.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderNestedJsonTableColumnsShapeInTableFunctionSource()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("jt.TagName", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "jt",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr(SqlConst.JSON_TABLE, [new IdentifierExpr("payload"), new LiteralExpr("$[*]")]),
                JsonTableClause: new SqlJsonTableClause(
                [
                    new SqlJsonTableColumn("Id", "INT", DbType.Int32, "$.id", false),
                    new SqlJsonTableNestedPath(
                        "$.tags[*]",
                        new SqlJsonTableClause(
                        [
                            new SqlJsonTableColumn("TagOrd", "BIGINT", DbType.Int64, null, true),
                            new SqlJsonTableColumn("TagName", "VARCHAR(20)", DbType.String, "$.name", false)
                        ]))
                ]))
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- FROM: dbo.JSON_TABLE(..., path) COLUMNS (..., NESTED PATH (...)) AS jt");
    }

    /// <summary>
    /// EN: Verifies CROSS APPLY join lines preserve strict-path and WITH details for OPENJSON sources.
    /// PT: Verifica se linhas de CROSS APPLY preservam os detalhes de strict-path e WITH para fontes OPENJSON.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderCrossApplyOpenJsonStrictPathWithClauseInJoinLine()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var join = new SqlJoin(
            SqlJoinType.CrossApply,
            new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "data",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("OPENJSON", [new IdentifierExpr("payload"), new LiteralExpr("strict $.items[1]")]),
                OpenJsonWithClause: new SqlOpenJsonWithClause(
                [
                    new SqlOpenJsonWithColumn("Name", "NVARCHAR(20)", DbType.String, "$.Name", false)
                ])),
            new LiteralExpr(true));

        var query = new SqlSelectQuery([], false, [new SqlSelectItem("data.Name", null)], [join], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- JOIN: CROSS APPLY dbo.OPENJSON(..., strict path) WITH (...) AS data");
    }

    /// <summary>
    /// EN: Verifies CROSS APPLY join lines preserve enable_ordinal details for STRING_SPLIT sources.
    /// PT: Verifica se linhas de CROSS APPLY preservam os detalhes de enable_ordinal para fontes STRING_SPLIT.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderCrossApplyStringSplitEnableOrdinalInJoinLine()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var join = new SqlJoin(
            SqlJoinType.CrossApply,
            new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "part",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("STRING_SPLIT", [new IdentifierExpr("payload"), new LiteralExpr(","), new LiteralExpr(1)])),
            new LiteralExpr(true));

        var query = new SqlSelectQuery([], false, [new SqlSelectItem("part.value", null)], [join], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- JOIN: CROSS APPLY dbo.STRING_SPLIT(..., ..., enable_ordinal) AS part");
    }

    /// <summary>
    /// EN: Verifies OUTER APPLY join lines preserve the shared STRING_SPLIT source shape in the execution plan.
    /// PT: Verifica se linhas de OUTER APPLY preservam o shape compartilhado da fonte STRING_SPLIT no plano de execucao.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderOuterApplyStringSplitInJoinLine()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var join = new SqlJoin(
            SqlJoinType.OuterApply,
            new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "part",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("STRING_SPLIT", [new IdentifierExpr("payload"), new LiteralExpr(",")])),
            new LiteralExpr(true));

        var query = new SqlSelectQuery([], false, [new SqlSelectItem("part.value", null)], [join], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- JOIN: OUTER APPLY dbo.STRING_SPLIT(...) AS part");
    }

    /// <summary>
    /// EN: Verifies OUTER APPLY join lines preserve strict-path and WITH details for OPENJSON sources.
    /// PT: Verifica se linhas de OUTER APPLY preservam os detalhes de strict-path e WITH para fontes OPENJSON.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderOuterApplyOpenJsonStrictPathWithClauseInJoinLine()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var join = new SqlJoin(
            SqlJoinType.OuterApply,
            new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "data",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("OPENJSON", [new IdentifierExpr("payload"), new LiteralExpr("strict $.items[1]")]),
                OpenJsonWithClause: new SqlOpenJsonWithClause(
                [
                    new SqlOpenJsonWithColumn("Name", "NVARCHAR(20)", DbType.String, "$.Name", false)
                ])),
            new LiteralExpr(true));

        var query = new SqlSelectQuery([], false, [new SqlSelectItem("data.Name", null)], [join], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- JOIN: OUTER APPLY dbo.OPENJSON(..., strict path) WITH (...) AS data");
    }

    /// <summary>
    /// EN: Verifies OUTER APPLY join lines preserve enable_ordinal details for STRING_SPLIT sources.
    /// PT: Verifica se linhas de OUTER APPLY preservam os detalhes de enable_ordinal para fontes STRING_SPLIT.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldRenderOuterApplyStringSplitEnableOrdinalInJoinLine()
    {
        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var join = new SqlJoin(
            SqlJoinType.OuterApply,
            new SqlTableSource(
                DbName: "dbo",
                Name: null,
                Alias: "part",
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                TableFunction: new FunctionCallExpr("STRING_SPLIT", [new IdentifierExpr("payload"), new LiteralExpr(","), new LiteralExpr(1)])),
            new LiteralExpr(true));

        var query = new SqlSelectQuery([], false, [new SqlSelectItem("part.value", null)], [join], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);
        plan.Should().Contain("- JOIN: OUTER APPLY dbo.STRING_SPLIT(..., ..., enable_ordinal) AS part");
    }


    /// <summary>
    /// EN: Verifies quality grade thresholds from risk score and performance band remain stable.
    /// PT: Verifica que os thresholds da nota qualitativa por risco e performance permanecem estáveis.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanQualityGrade_WithStableThresholdRules()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var infoWarning = new[] { new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Info) };
        var warningOnly = new[] { new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning) };
        var highWarning = new[] { new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High) };

        var gradeA = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 5), null, infoWarning);
        var gradeB = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 5), null, warningOnly);
        var gradeC = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 6), null, warningOnly);
        var gradeD = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 31), null, warningOnly);
        var gradeBFromHighRiskFastBand = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 5), null, highWarning);

        gradeA.Should().Contain("- PlanQualityGrade: A");
        gradeB.Should().Contain("- PlanQualityGrade: B");
        gradeC.Should().Contain("- PlanQualityGrade: C");
        gradeD.Should().Contain("- PlanQualityGrade: D");
        gradeBFromHighRiskFastBand.Should().Contain("- PlanQualityGrade: B");
    }


    /// <summary>
    /// EN: Verifies risk-score boundary mapping for quality grade on fast band.
    /// PT: Verifica o mapeamento dos limites de score de risco para nota qualitativa na banda Fast.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldMapPlanQualityGrade_ByRiskScoreBoundaries_OnFastBand()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var fastMetrics = new SqlPlanRuntimeMetrics(1, 1, 1, 5);

        var gradeAWarnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Info),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Info)
        };

        var gradeBWarnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Info),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Info)
        };

        var gradeCWarnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning)
        };

        var gradeDWarnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Info),
            new SqlPlanWarning("PW4", "m4", "r4", "a4", SqlPlanWarningSeverity.Info)
        };

        SqlExecutionPlanFormatter.FormatSelect(query, fastMetrics, null, gradeAWarnings)
            .Should().Contain("- PlanRiskScore: 20")
            .And.Contain("- PlanQualityGrade: A");

        SqlExecutionPlanFormatter.FormatSelect(query, fastMetrics, null, gradeBWarnings)
            .Should().Contain("- PlanRiskScore: 50")
            .And.Contain("- PlanQualityGrade: B");

        SqlExecutionPlanFormatter.FormatSelect(query, fastMetrics, null, gradeCWarnings)
            .Should().Contain("- PlanRiskScore: 80")
            .And.Contain("- PlanQualityGrade: C");

        SqlExecutionPlanFormatter.FormatSelect(query, fastMetrics, null, gradeDWarnings)
            .Should().Contain("- PlanRiskScore: 100")
            .And.Contain("- PlanQualityGrade: D");
    }



    /// <summary>
    /// EN: Verifies warning summary is emitted in deterministic severity/code order.
    /// PT: Verifica que o resumo de warnings é emitido em ordem determinística por severidade/código.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanWarningSummary_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Warning)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanWarningSummary: PW1:High;PW2:Warning;PW3:Warning");
    }

    /// <summary>
    /// EN: Verifies warning summary is omitted when warnings are absent.
    /// PT: Verifica que o resumo de warnings é omitido quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanWarningSummary_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanWarningSummary:");
    }


    /// <summary>
    /// EN: Verifies primary warning is emitted using deterministic severity/code priority.
    /// PT: Verifica que o warning primário é emitido com prioridade determinística por severidade/código.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanPrimaryWarning_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW0", "m0", "r0", "a0", SqlPlanWarningSeverity.High)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanPrimaryWarning: PW0:High");
    }

    /// <summary>
    /// EN: Verifies primary warning is omitted when warnings are absent.
    /// PT: Verifica que o warning primário é omitido quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanPrimaryWarning_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanPrimaryWarning:");
    }


    /// <summary>
    /// EN: Verifies primary cause group is emitted from primary warning mapping.
    /// PT: Verifica que o grupo de causa primária é emitido a partir do mapeamento do warning primário.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanPrimaryCauseGroup_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW002", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanPrimaryCauseGroup: SortWithoutLimit");
    }

    /// <summary>
    /// EN: Verifies primary cause group is omitted when warnings are absent.
    /// PT: Verifica que o grupo de causa primária é omitido quando não há warnings.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanPrimaryCauseGroup_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanPrimaryCauseGroup:");
    }


    /// <summary>
    /// EN: Verifies index recommendation summary is emitted in parseable format.
    /// PT: Verifica que o resumo de recomendação de índice é emitido em formato parseável.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitIndexRecommendationSummary_WhenRecommendationsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60),
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Id ON users (Id);", "reason", 60, 120, 30)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, []);
        plan.Should().Contain($"- IndexRecommendationSummary: count:2;avgConfidence:{70:N2};maxGainPct:{75:N2}");
    }

    /// <summary>
    /// EN: Verifies index recommendation summary is omitted when recommendations are absent.
    /// PT: Verifica que o resumo de recomendação de índice é omitido quando não há recomendações.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitIndexRecommendationSummary_WhenNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().NotContain("IndexRecommendationSummary:");
    }


    /// <summary>
    /// EN: Verifies index recommendation evidence is emitted in parseable deterministic format.
    /// PT: Verifica que a evidência de recomendação de índice é emitida em formato parseável e determinístico.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitIndexRecommendationEvidence_WhenRecommendationsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active_Id ON users (Active, Id);", "reason", 80, 120, 60)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, []);
        plan.Should().Contain($"- IndexRecommendationEvidence: table:users;indexCols:Active,Id;confidence:80;gainPct:{50:N2}");
    }

    /// <summary>
    /// EN: Verifies index recommendation evidence is omitted when recommendations are absent.
    /// PT: Verifica que a evidência de recomendação de índice é omitida quando não há recomendações.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitIndexRecommendationEvidence_WhenNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().NotContain("IndexRecommendationEvidence:");
    }


    /// <summary>
    /// EN: Verifies warning counts are emitted in parseable fixed-key format.
    /// PT: Verifica que as contagens de warning são emitidas em formato parseável de chaves fixas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanWarningCounts_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW4", "m4", "r4", "a4", SqlPlanWarningSeverity.Info)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanWarningCounts: high:1;warning:2;info:1");
    }

    /// <summary>
    /// EN: Verifies warning counts are omitted when warnings are absent.
    /// PT: Verifica que as contagens de warning são omitidas quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanWarningCounts_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanWarningCounts:");
    }


    /// <summary>
    /// EN: Verifies noise score is emitted when warnings are present.
    /// PT: Verifica que o score de ruído é emitido quando há warnings.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanNoiseScore_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW004", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning, "EstimatedRowsRead", "120", "gte:100;highGte:5000"),
            new SqlPlanWarning("PW005", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning, "EstimatedRowsRead", "120", "gte:100;highGte:5000")
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);

        plan.Should().Contain("- PlanNoiseScore: 50");
    }

    /// <summary>
    /// EN: Verifies noise score is omitted when warnings are absent.
    /// PT: Verifica que o score de ruído é omitido quando não há warnings.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanNoiseScore_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanNoiseScore:");
    }


    /// <summary>
    /// EN: Verifies top actions are emitted in deterministic priority order and capped at 3.
    /// PT: Verifica que as ações prioritárias são emitidas em ordem determinística e limitadas a 3.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanTopActions_WithDeterministicOrderAndCap()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW003", "m3", "r3", "a3", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW005", "m5", "r5", "a5", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW004", "m4", "r4", "a4", SqlPlanWarningSeverity.Info)
        };
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, warnings);
        plan.Should().Contain("- PlanTopActions: PW001:AddSelectiveFilter;PW003:ReduceProjectionColumns;PW005:CreateDistinctCoveringIndex");
    }

    /// <summary>
    /// EN: Verifies top actions include index recommendation action when warnings are absent.
    /// PT: Verifica que as ações incluem recomendação de índice quando não há warnings.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanTopActions_FromIndexRecommendations_WhenWarningsAreAbsent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, []);

        plan.Should().Contain("- PlanTopActions: IDX:CreateSuggestedIndex");
    }

    /// <summary>
    /// EN: Verifies top actions are omitted when warnings and recommendations are absent.
    /// PT: Verifica que as ações são omitidas quando não há warnings e nem recomendações.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanTopActions_WhenNoWarningsAndNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().NotContain("PlanTopActions:");
    }


    /// <summary>
    /// EN: Verifies plan delta is emitted when previous metrics are provided.
    /// PT: Verifica que o delta do plano é emitido quando métricas anteriores são fornecidas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanDelta_WhenPreviousMetricsAreProvided()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var current = new SqlPlanRuntimeMetrics(1, 120, 12, 31);
        var previous = new SqlPlanRuntimeMetrics(1, 80, 10, 20);
        var warnings = new[]
        {
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High)
        };
        var previousWarnings = new[]
        {
            new SqlPlanWarning("PW002", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, current, [], warnings, previous, previousWarnings);
        plan.Should().Contain($"- {SqlExecutionPlanMessages.PlanDeltaLabel()}: riskDelta:+20;elapsedMsDelta:+11");
    }

    /// <summary>
    /// EN: Verifies severity hint context can be overridden and reflected in output.
    /// PT: Verifica que o contexto do hint de severidade pode ser sobrescrito e refletido no output.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanSeverityHint_WithContextOverride()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW002", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning)
        };

        var devPlan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], warnings, null, null, SqlPlanSeverityHintContext.Dev);
        var prodPlan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], warnings, null, null, SqlPlanSeverityHintContext.Prod);

        devPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanSeverityHintLabel()}: context:dev;level:Info");
        prodPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanSeverityHintLabel()}: context:prod;level:Warning");
    }

    /// <summary>
    /// EN: Verifies the formatted execution plan includes the current metadata version marker.
    /// PT: Verifica que o plano de execução formatado inclui o marcador da versão atual de metadados.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanMetadataVersion()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().Contain("- PlanMetadataVersion: 1");
    }


    /// <summary>
    /// EN: Verifies plan correlation id is emitted with stable technical format.
    /// PT: Verifica que o correlation id do plano é emitido em formato técnico estável.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanCorrelationId_WithStableFormat()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        var correlationId = plan
            .Split(new[] { Environment.NewLine }, StringSplitOptions.None)
            .First(line => line.StartsWith("- PlanCorrelationId:", StringComparison.Ordinal))
            .Split([':'], 2)[1]
            .Trim();

        CorrelationIdPattern.IsMatch(correlationId).Should().BeTrue();
    }

    /// <summary>
    /// EN: Verifies SELECT estimated cost increases when projection includes window-function expressions.
    /// PT: Verifica que o custo estimado de SELECT aumenta quando a projeção inclui expressões de função de janela.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithWindowProjectionComplexity()
    {
        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var windowQuery = new SqlSelectQuery(
            [],
            false,
            [
                new SqlSelectItem("id", null),
                new SqlSelectItem("RANK() OVER (ORDER BY tenantid)", "rk")
            ],
            [],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 10, 3, 1);
        var basePlan = SqlExecutionPlanFormatter.FormatSelect(baseQuery, metrics, [], []);
        var windowPlan = SqlExecutionPlanFormatter.FormatSelect(windowQuery, metrics, [], []);

        ExtractEstimatedCost(basePlan).Should().BeLessThan(ExtractEstimatedCost(windowPlan));
    }

    /// <summary>
    /// EN: Verifies SELECT estimated cost includes source complexity for derived subqueries.
    /// PT: Verifica que o custo estimado de SELECT inclui complexidade de fonte para subconsultas derivadas.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithDerivedSourceComplexity()
    {
        var simpleQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var derivedInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var derivedQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, null, "d", derivedInner, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 10, 3, 1);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleQuery, metrics, [], []);
        var derivedPlan = SqlExecutionPlanFormatter.FormatSelect(derivedQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(derivedPlan));
    }

    /// <summary>
    /// EN: Verifies SELECT estimated cost applies higher join-type complexity for LEFT joins versus INNER joins.
    /// PT: Verifica que o custo estimado de SELECT aplica maior complexidade por tipo de join para LEFT em relação a INNER.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithOuterJoinTypeComplexity()
    {
        var innerJoinQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new LiteralExpr(true))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var leftJoinQuery = innerJoinQuery with
        {
            Joins = [new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), new LiteralExpr(true))]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 12, 3);
        var innerPlan = SqlExecutionPlanFormatter.FormatSelect(innerJoinQuery, metrics, [], []);
        var leftPlan = SqlExecutionPlanFormatter.FormatSelect(leftJoinQuery, metrics, [], []);

        ExtractEstimatedCost(innerPlan).Should().BeLessThan(ExtractEstimatedCost(leftPlan));
    }

    /// <summary>
    /// EN: Verifies ORDER BY without row-limit carries additional estimated spill/sort risk cost.
    /// PT: Verifica que ORDER BY sem limite de linhas carrega custo adicional estimado de risco de sort/spill.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForOrderByWithoutLimitRisk()
    {
        var noLimitQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withLimitQuery = noLimitQuery with { RowLimit = new SqlLimitOffset(new LiteralExpr(10), null) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(noLimitQuery, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withLimitQuery, metrics, [], []);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }

    /// <summary>
    /// EN: Verifies predicate complexity contributes to estimated cost for WHERE filters.
    /// PT: Verifica que a complexidade de predicados contribui para o custo estimado em filtros WHERE.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithWherePredicateComplexity()
    {
        var simpleWhere = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var complexWhere = simpleWhere with
        {
            Where = new BinaryExpr(
                SqlBinaryOp.And,
                new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
                new BinaryExpr(SqlBinaryOp.Or,
                    new LikeExpr(new IdentifierExpr("name"), new LiteralExpr("J%")),
                    new BinaryExpr(SqlBinaryOp.GreaterOrEqual, new IdentifierExpr("tenantid"), new LiteralExpr(10))))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleWhere, metrics, [], []);
        var complexPlan = SqlExecutionPlanFormatter.FormatSelect(complexWhere, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(complexPlan));
    }

    /// <summary>
    /// EN: Verifies join ON predicate complexity contributes to estimated cost.
    /// PT: Verifica que a complexidade do predicado ON do join contribui para o custo estimado.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithJoinPredicateComplexity()
    {
        var simpleJoin = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new LiteralExpr(true))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var complexJoin = simpleJoin with
        {
            Joins =
            [
                new SqlJoin(
                    SqlJoinType.Inner,
                    new SqlTableSource(null, "orders", "o", null, null, null, null),
                    new BinaryExpr(
                        SqlBinaryOp.And,
                        new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid")),
                        new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("o.amount"), new LiteralExpr(0))))
            ]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleJoin, metrics, [], []);
        var complexPlan = SqlExecutionPlanFormatter.FormatSelect(complexJoin, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(complexPlan));
    }

    /// <summary>
    /// EN: Verifies GROUP BY with HAVING carries higher estimated cost than equivalent non-aggregated query.
    /// PT: Verifica que GROUP BY com HAVING tem custo estimado maior do que consulta equivalente sem agregação.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithGroupByAndHaving()
    {
        var nonAggregatedQuery = new SqlSelectQuery([], false, [new SqlSelectItem("tenantid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var aggregatedQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [],
            null,
            ["tenantid"],
            new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(1)))
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var nonAggregatedPlan = SqlExecutionPlanFormatter.FormatSelect(nonAggregatedQuery, metrics, [], []);
        var aggregatedPlan = SqlExecutionPlanFormatter.FormatSelect(aggregatedQuery, metrics, [], []);

        ExtractEstimatedCost(nonAggregatedPlan).Should().BeLessThan(ExtractEstimatedCost(aggregatedPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT with ORDER BY and no limit carries higher estimated cost than ORDER BY alone.
    /// PT: Verifica que DISTINCT com ORDER BY sem limite tem custo estimado maior do que apenas ORDER BY.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithDistinctAndOrderByWithoutLimit()
    {
        var orderByOnlyQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var distinctOrderByQuery = orderByOnlyQuery with { Distinct = true };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var orderByOnlyPlan = SqlExecutionPlanFormatter.FormatSelect(orderByOnlyQuery, metrics, [], []);
        var distinctOrderByPlan = SqlExecutionPlanFormatter.FormatSelect(distinctOrderByQuery, metrics, [], []);

        ExtractEstimatedCost(orderByOnlyPlan).Should().BeLessThan(ExtractEstimatedCost(distinctOrderByPlan));
    }

    /// <summary>
    /// EN: Verifies UNION DISTINCT carries higher estimated cost than equivalent UNION ALL.
    /// PT: Verifica que UNION DISTINCT tem custo estimado maior do que UNION ALL equivalente.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseForUnionDistinctComparedToUnionAll()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var unionAllPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [], null, metrics);
        var unionDistinctPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [false], [], null, metrics);

        ExtractEstimatedCost(unionAllPlan).Should().BeLessThan(ExtractEstimatedCost(unionDistinctPlan));
    }

    /// <summary>
    /// EN: Verifies UNION text plan includes localized performance disclaimer.
    /// PT: Verifica que o plano textual de UNION inclui disclaimer de performance localizado.
    /// </summary>
    [Fact]
    public void FormatUnion_ShouldIncludePerformanceDisclaimer()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var plan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [], null, metrics);

        plan.Should().Contain($"- {SqlExecutionPlanMessages.PerformanceDisclaimerLabel()}: {SqlExecutionPlanMessages.PerformanceDisclaimerMessage()}");
    }

    /// <summary>
    /// EN: Verifies UNION estimated cost increases when ALL/DISTINCT operators alternate more often even with the same DISTINCT operator count.
    /// PT: Verifica que o custo estimado de UNION aumenta quando operadores ALL/DISTINCT alternam com maior frequência mesmo com a mesma contagem de operadores DISTINCT.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseForAlternatingSetOperatorTransitionsWithSameDistinctCount()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var part3 = new SqlSelectQuery([], false, [new SqlSelectItem("tenantid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part4 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(4, 400, 40, 8);

        var clusteredTransitionsPlan = SqlExecutionPlanFormatter.FormatUnion(
            [part1, part2, part3, part4],
            [false, false, true],
            [],
            null,
            metrics);

        var alternatingTransitionsPlan = SqlExecutionPlanFormatter.FormatUnion(
            [part1, part2, part3, part4],
            [false, true, false],
            [],
            null,
            metrics);

        ExtractEstimatedCost(clusteredTransitionsPlan).Should().BeLessThan(ExtractEstimatedCost(alternatingTransitionsPlan));
    }

    /// <summary>
    /// EN: Verifies UNION ORDER BY without row limit carries higher estimated cost than equivalent UNION ORDER BY with row limit.
    /// PT: Verifica que UNION com ORDER BY sem limite de linhas tem custo estimado maior do que UNION equivalente com limite.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseForOrderByWithoutLimitRisk()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], null, metrics);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(10), null), metrics);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }


    /// <summary>
    /// EN: Verifies EXISTS/Subquery predicates increase estimated cost compared to a simple scalar predicate.
    /// PT: Verifica que predicados EXISTS/Subquery aumentam o custo estimado em relação a predicado escalar simples.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithExistsAndSubqueryPredicates()
    {
        var baseQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var subquerySelect = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", "o", null, null, null, null)
        };

        var subqueryExpr = new SubqueryExpr("SELECT userid FROM orders o", subquerySelect);
        var existsQuery = baseQuery with
        {
            Where = new ExistsExpr(subqueryExpr)
        };

        var scalarSubqueryQuery = baseQuery with
        {
            Where = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("id"), subqueryExpr)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var basePlan = SqlExecutionPlanFormatter.FormatSelect(baseQuery, metrics, [], []);
        var existsPlan = SqlExecutionPlanFormatter.FormatSelect(existsQuery, metrics, [], []);
        var scalarSubqueryPlan = SqlExecutionPlanFormatter.FormatSelect(scalarSubqueryQuery, metrics, [], []);

        ExtractEstimatedCost(basePlan).Should().BeLessThan(ExtractEstimatedCost(existsPlan));
        ExtractEstimatedCost(basePlan).Should().BeLessThan(ExtractEstimatedCost(scalarSubqueryPlan));
    }


    /// <summary>
    /// EN: Verifies GROUP BY + HAVING coupling penalty keeps estimated cost above equivalent GROUP BY-only shape.
    /// PT: Verifica que a penalidade de acoplamento GROUP BY + HAVING mantém custo estimado acima do formato equivalente só com GROUP BY.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWhenGroupingAndHavingAreCombined()
    {
        var groupByOnlyQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var groupByHavingQuery = groupByOnlyQuery with
        {
            Having = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(10))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var groupByOnlyPlan = SqlExecutionPlanFormatter.FormatSelect(groupByOnlyQuery, metrics, [], []);
        var groupByHavingPlan = SqlExecutionPlanFormatter.FormatSelect(groupByHavingQuery, metrics, [], []);

        ExtractEstimatedCost(groupByOnlyPlan).Should().BeLessThan(ExtractEstimatedCost(groupByHavingPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT + ORDER BY no-limit coupling penalty is reduced when a row limit is present.
    /// PT: Verifica que a penalidade de acoplamento DISTINCT + ORDER BY sem limite é reduzida quando há limite de linhas.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldDecreaseForDistinctOrderByWhenLimitIsPresent()
    {
        var noLimitQuery = new SqlSelectQuery([], true, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withLimitQuery = noLimitQuery with { RowLimit = new SqlLimitOffset(new LiteralExpr(10), null) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(noLimitQuery, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withLimitQuery, metrics, [], []);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }

    /// <summary>
    /// EN: Verifies UNION ORDER BY merge uplift grows with additional UNION parts (fan-in), not only with ORDER BY presence.
    /// PT: Verifica que o uplift de merge em UNION ORDER BY cresce com partes adicionais de UNION (fan-in), e não apenas com a presença de ORDER BY.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseOrderByMergeUpliftWithAdditionalUnionParts()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var part3 = new SqlSelectQuery([], false, [new SqlSelectItem("tenantid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part4 = new SqlSelectQuery([], false, [new SqlSelectItem("orderid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(4, 400, 40, 8);

        var twoPartsNoOrderPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [], null, metrics);
        var twoPartsWithOrderPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], null, metrics);

        var fourPartsNoOrderPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2, part3, part4], [true, true, true], [], null, metrics);
        var fourPartsWithOrderPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2, part3, part4], [true, true, true], [new SqlOrderByItem("id", false)], null, metrics);

        var twoPartsOrderUplift = ExtractEstimatedCost(twoPartsWithOrderPlan) - ExtractEstimatedCost(twoPartsNoOrderPlan);
        var fourPartsOrderUplift = ExtractEstimatedCost(fourPartsWithOrderPlan) - ExtractEstimatedCost(fourPartsNoOrderPlan);
        twoPartsOrderUplift.Should().BeLessThan(fourPartsOrderUplift);
    }

    /// <summary>
    /// EN: Verifies UNION ORDER BY fan-in uplift is stronger with complex ORDER BY expressions than with simple key ordering.
    /// PT: Verifica que o uplift de fan-in em UNION ORDER BY é mais forte com expressões ORDER BY complexas do que com ordenação por chave simples.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseOrderByMergeFanInUpliftForComplexOrderExpression()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var part3 = new SqlSelectQuery([], false, [new SqlSelectItem("tenantid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part4 = new SqlSelectQuery([], false, [new SqlSelectItem("orderid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(4, 400, 40, 8);

        var simpleOrderByPlan = SqlExecutionPlanFormatter.FormatUnion(
            [part1, part2, part3, part4],
            [true, true, true],
            [new SqlOrderByItem("id", false)],
            null,
            metrics);

        var complexOrderByPlan = SqlExecutionPlanFormatter.FormatUnion(
            [part1, part2, part3, part4],
            [true, true, true],
            [new SqlOrderByItem("CASE WHEN id > 0 THEN JSON_VALUE(payload, '$.tenant') ELSE id END", false)],
            null,
            metrics);

        ExtractEstimatedCost(simpleOrderByPlan).Should().BeLessThan(ExtractEstimatedCost(complexOrderByPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT + GROUP BY + ORDER BY without row limit applies an additional coupling penalty beyond DISTINCT baseline.
    /// PT: Verifica que DISTINCT + GROUP BY + ORDER BY sem limite de linhas aplica penalidade adicional de acoplamento além da linha de base de DISTINCT.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldApplyAdditionalPenaltyForDistinctGroupByOrderByMixWithoutLimit()
    {
        var groupedOrderedQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("tenantid", false)],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var distinctGroupedOrderedQuery = groupedOrderedQuery with { Distinct = true };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var groupedOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(groupedOrderedQuery, metrics, [], []);
        var distinctGroupedOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(distinctGroupedOrderedQuery, metrics, [], []);

        var groupedOrderedCost = ExtractEstimatedCost(groupedOrderedPlan);
        var distinctGroupedOrderedCost = ExtractEstimatedCost(distinctGroupedOrderedPlan);
        (distinctGroupedOrderedCost - groupedOrderedCost).Should().BeGreaterThanOrEqualTo(20);
    }

    /// <summary>
    /// EN: Verifies row-limit reduces DISTINCT + GROUP BY + ORDER BY coupling pressure compared with the same no-limit shape.
    /// PT: Verifica que limite de linhas reduz a pressão de acoplamento de DISTINCT + GROUP BY + ORDER BY em comparação ao mesmo formato sem limite.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldReduceDistinctGroupByOrderByCouplingWhenLimitIsPresent()
    {
        var noLimitQuery = new SqlSelectQuery(
            [],
            true,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("tenantid", false)],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withLimitQuery = noLimitQuery with { RowLimit = new SqlLimitOffset(new LiteralExpr(10), null) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(noLimitQuery, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withLimitQuery, metrics, [], []);

        var noLimitCost = ExtractEstimatedCost(noLimitPlan);
        var withLimitCost = ExtractEstimatedCost(withLimitPlan);
        (noLimitCost - withLimitCost).Should().BeGreaterThanOrEqualTo(15);
    }

    /// <summary>
    /// EN: Verifies LIMIT with moderate OFFSET still adds noticeable DISTINCT + GROUP BY + ORDER BY coupling pressure compared with the same LIMIT without OFFSET.
    /// PT: Verifica que LIMIT com OFFSET moderado ainda adiciona pressão perceptível de acoplamento DISTINCT + GROUP BY + ORDER BY em comparação ao mesmo LIMIT sem OFFSET.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseDistinctGroupByOrderByCouplingForLimitWithOffset()
    {
        var noOffsetQuery = new SqlSelectQuery(
            [],
            true,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("tenantid", false)],
            new SqlLimitOffset(new LiteralExpr(10), null),
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withOffsetQuery = noOffsetQuery with
        {
            RowLimit = new SqlLimitOffset(new LiteralExpr(10), new LiteralExpr(50))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(noOffsetQuery, metrics, [], []);
        var withOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(withOffsetQuery, metrics, [], []);

        (ExtractEstimatedCost(withOffsetPlan) - ExtractEstimatedCost(noOffsetPlan)).Should().BeGreaterThanOrEqualTo(2);
    }

    /// <summary>
    /// EN: Verifies DISTINCT + GROUP BY + ORDER BY coupling uplift is stronger when grouping/ordering expressions are complex (CASE/JSON markers) than for simple key expressions.
    /// PT: Verifica que o uplift de acoplamento DISTINCT + GROUP BY + ORDER BY é mais forte quando expressões de agrupamento/ordenação são complexas (marcadores CASE/JSON) do que para chaves simples.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseDistinctGroupByOrderByCouplingForComplexExpressions()
    {
        var simpleGroupedOrderedQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("tenantid", false)],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var complexGroupedOrderedQuery = simpleGroupedOrderedQuery with
        {
            GroupBy = ["CASE WHEN status = 'A' THEN JSON_VALUE(payload, '$.tenant') ELSE tenantid END"],
            OrderBy = [new SqlOrderByItem("CASE WHEN status = 'A' THEN JSON_VALUE(payload, '$.tenant') ELSE payload->>'tenant' END", false)]
        };

        var simpleDistinctQuery = simpleGroupedOrderedQuery with { Distinct = true };
        var complexDistinctQuery = complexGroupedOrderedQuery with { Distinct = true };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simpleGroupedOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(simpleGroupedOrderedQuery, metrics, [], []);
        var complexGroupedOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(complexGroupedOrderedQuery, metrics, [], []);
        var simpleDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(simpleDistinctQuery, metrics, [], []);
        var complexDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(complexDistinctQuery, metrics, [], []);

        var simpleDistinctUplift = ExtractEstimatedCost(simpleDistinctPlan) - ExtractEstimatedCost(simpleGroupedOrderedPlan);
        var complexDistinctUplift = ExtractEstimatedCost(complexDistinctPlan) - ExtractEstimatedCost(complexGroupedOrderedPlan);
        simpleDistinctUplift.Should().BeLessThan(complexDistinctUplift);
    }

    /// <summary>
    /// EN: Verifies DISTINCT + GROUP BY + ORDER BY coupling uplift grows when expansion-risk joins are present.
    /// PT: Verifica que o uplift de acoplamento DISTINCT + GROUP BY + ORDER BY cresce quando há joins com risco de expansão.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseDistinctGroupByOrderByCouplingWhenExpansionRiskJoinsArePresent()
    {
        var noJoinBaseQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("u.tenantid", false)],
            null,
            ["u.tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var withExpansionRiskJoinsBaseQuery = noJoinBaseQuery with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Cross, new SqlTableSource(null, "payments", "p", null, null, null, null), new LiteralExpr(true))
            ]
        };

        var noJoinDistinctQuery = noJoinBaseQuery with { Distinct = true };
        var withExpansionRiskJoinsDistinctQuery = withExpansionRiskJoinsBaseQuery with { Distinct = true };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var noJoinBasePlan = SqlExecutionPlanFormatter.FormatSelect(noJoinBaseQuery, metrics, [], []);
        var noJoinDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(noJoinDistinctQuery, metrics, [], []);
        var withJoinsBasePlan = SqlExecutionPlanFormatter.FormatSelect(withExpansionRiskJoinsBaseQuery, metrics, [], []);
        var withJoinsDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(withExpansionRiskJoinsDistinctQuery, metrics, [], []);

        var noJoinDistinctUplift = ExtractEstimatedCost(noJoinDistinctPlan) - ExtractEstimatedCost(noJoinBasePlan);
        var withJoinsDistinctUplift = ExtractEstimatedCost(withJoinsDistinctPlan) - ExtractEstimatedCost(withJoinsBasePlan);
        noJoinDistinctUplift.Should().BeLessThan(withJoinsDistinctUplift);
    }

    /// <summary>
    /// EN: Verifies HAVING adds stronger extra cost when DISTINCT + GROUP BY + ORDER BY are already present than in the equivalent non-distinct grouped/ordered shape.
    /// PT: Verifica que HAVING adiciona custo extra mais forte quando DISTINCT + GROUP BY + ORDER BY já estão presentes do que no formato equivalente agrupado/ordenado sem DISTINCT.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseHavingCouplingForDistinctGroupByOrderByShape()
    {
        var groupedOrderedQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("tenantid", false)],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var groupedOrderedHavingQuery = groupedOrderedQuery with
        {
            Having = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(10))
        };

        var distinctGroupedOrderedQuery = groupedOrderedQuery with
        {
            Distinct = true
        };

        var distinctGroupedOrderedHavingQuery = distinctGroupedOrderedQuery with
        {
            Having = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(10))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var groupedOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(groupedOrderedQuery, metrics, [], []);
        var groupedOrderedHavingPlan = SqlExecutionPlanFormatter.FormatSelect(groupedOrderedHavingQuery, metrics, [], []);
        var distinctGroupedOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(distinctGroupedOrderedQuery, metrics, [], []);
        var distinctGroupedOrderedHavingPlan = SqlExecutionPlanFormatter.FormatSelect(distinctGroupedOrderedHavingQuery, metrics, [], []);

        var nonDistinctHavingUplift = ExtractEstimatedCost(groupedOrderedHavingPlan) - ExtractEstimatedCost(groupedOrderedPlan);
        var distinctHavingUplift = ExtractEstimatedCost(distinctGroupedOrderedHavingPlan) - ExtractEstimatedCost(distinctGroupedOrderedPlan);
        nonDistinctHavingUplift.Should().BeLessThan(distinctHavingUplift);
    }

    /// <summary>
    /// EN: Verifies HAVING coupling uplift for DISTINCT + GROUP BY + ORDER BY grows when expansion-risk joins are present.
    /// PT: Verifica que o uplift de acoplamento de HAVING para DISTINCT + GROUP BY + ORDER BY cresce quando há joins com risco de expansão.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseHavingCouplingForDistinctGroupByOrderByWithExpansionRiskJoins()
    {
        var noJoinQuery = new SqlSelectQuery(
            [],
            true,
            [new SqlSelectItem("u.tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("u.tenantid", false)],
            null,
            ["u.tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var withExpansionRiskJoinsQuery = noJoinQuery with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Cross, new SqlTableSource(null, "payments", "p", null, null, null, null), new LiteralExpr(true))
            ]
        };

        var noJoinWithHavingQuery = noJoinQuery with
        {
            Having = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(10))
        };

        var withJoinsHavingQuery = withExpansionRiskJoinsQuery with
        {
            Having = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(10))
        };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var noJoinPlan = SqlExecutionPlanFormatter.FormatSelect(noJoinQuery, metrics, [], []);
        var noJoinHavingPlan = SqlExecutionPlanFormatter.FormatSelect(noJoinWithHavingQuery, metrics, [], []);
        var withJoinPlan = SqlExecutionPlanFormatter.FormatSelect(withExpansionRiskJoinsQuery, metrics, [], []);
        var withJoinHavingPlan = SqlExecutionPlanFormatter.FormatSelect(withJoinsHavingQuery, metrics, [], []);

        var noJoinHavingUplift = ExtractEstimatedCost(noJoinHavingPlan) - ExtractEstimatedCost(noJoinPlan);
        var withJoinHavingUplift = ExtractEstimatedCost(withJoinHavingPlan) - ExtractEstimatedCost(withJoinPlan);
        noJoinHavingUplift.Should().BeLessThan(withJoinHavingUplift);
    }

    /// <summary>
    /// EN: Verifies HAVING coupling uplift for DISTINCT + GROUP BY + ORDER BY with expansion-risk joins is stronger when JOIN predicates are deeply nested and mixed with CASE/JSON logic.
    /// PT: Verifica que o uplift de acoplamento de HAVING para DISTINCT + GROUP BY + ORDER BY com joins de risco de expansão é mais forte quando predicados de JOIN são profundamente aninhados e mistos com lógica CASE/JSON.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseHavingJoinCouplingForDistinctGroupByOrderByWithComplexJoinPredicates()
    {
        var simpleJoinOn = new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"));
        var complexJoinOn = new BinaryExpr(
            SqlBinaryOp.Or,
            new BinaryExpr(
                SqlBinaryOp.And,
                new BinaryExpr(
                    SqlBinaryOp.Eq,
                    new CaseExpr(
                        null,
                        [new CaseWhenThen(new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("o.total"), new LiteralExpr(1000)), new LiteralExpr("vip"))],
                        new LiteralExpr("std")),
                    new LiteralExpr("vip")),
                new BinaryExpr(
                    SqlBinaryOp.Eq,
                    new FunctionCallExpr("JSON_VALUE", [new IdentifierExpr("o.payload"), new LiteralExpr("$.tier")]),
                    new LiteralExpr("gold"))),
            new BinaryExpr(
                SqlBinaryOp.And,
                new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.tenantid"), new IdentifierExpr("o.tenantid")),
                new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("o.createdat"), new LiteralExpr("2025-01-01"))));

        var withSimpleJoinPredicatesQuery = new SqlSelectQuery(
            [],
            true,
            [new SqlSelectItem("u.tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [
                new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), simpleJoinOn),
                new SqlJoin(SqlJoinType.Cross, new SqlTableSource(null, "payments", "p", null, null, null, null), new LiteralExpr(true))
            ],
            null,
            [new SqlOrderByItem("u.tenantid", false)],
            null,
            ["u.tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var withComplexJoinPredicatesQuery = withSimpleJoinPredicatesQuery with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), complexJoinOn),
                new SqlJoin(SqlJoinType.Cross, new SqlTableSource(null, "payments", "p", null, null, null, null), new LiteralExpr(true))
            ]
        };

        var withSimpleJoinHavingQuery = withSimpleJoinPredicatesQuery with
        {
            Having = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(10))
        };

        var withComplexJoinHavingQuery = withComplexJoinPredicatesQuery with
        {
            Having = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(10))
        };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var simpleNoHavingPlan = SqlExecutionPlanFormatter.FormatSelect(withSimpleJoinPredicatesQuery, metrics, [], []);
        var simpleHavingPlan = SqlExecutionPlanFormatter.FormatSelect(withSimpleJoinHavingQuery, metrics, [], []);
        var complexNoHavingPlan = SqlExecutionPlanFormatter.FormatSelect(withComplexJoinPredicatesQuery, metrics, [], []);
        var complexHavingPlan = SqlExecutionPlanFormatter.FormatSelect(withComplexJoinHavingQuery, metrics, [], []);

        var simpleHavingUplift = ExtractEstimatedCost(simpleHavingPlan) - ExtractEstimatedCost(simpleNoHavingPlan);
        var complexHavingUplift = ExtractEstimatedCost(complexHavingPlan) - ExtractEstimatedCost(complexNoHavingPlan);
        simpleHavingUplift.Should().BeLessThan(complexHavingUplift);
    }


    /// <summary>
    /// EN: Verifies estimated cost increases with additional GROUP BY keys.
    /// PT: Verifica que o custo estimado aumenta com chaves adicionais de GROUP BY.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalGroupByKeys()
    {
        var oneKeyGroupByQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoKeyGroupByQuery = oneKeyGroupByQuery with
        {
            GroupBy = ["tenantid", "status"]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneKeyPlan = SqlExecutionPlanFormatter.FormatSelect(oneKeyGroupByQuery, metrics, [], []);
        var twoKeyPlan = SqlExecutionPlanFormatter.FormatSelect(twoKeyGroupByQuery, metrics, [], []);

        ExtractEstimatedCost(oneKeyPlan).Should().BeLessThan(ExtractEstimatedCost(twoKeyPlan));
    }

    /// <summary>
    /// EN: Verifies GROUP BY expression complexity increases estimated cost when grouping key uses CASE expression.
    /// PT: Verifica que a complexidade de expressão em GROUP BY aumenta o custo estimado quando a chave de agrupamento usa expressão CASE.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithComplexGroupByExpressions()
    {
        var simpleGroupByQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var complexGroupByQuery = simpleGroupByQuery with
        {
            GroupBy = ["CASE WHEN tenantid > 10 THEN 1 ELSE 0 END"]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleGroupByQuery, metrics, [], []);
        var complexPlan = SqlExecutionPlanFormatter.FormatSelect(complexGroupByQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(complexPlan));
    }

    /// <summary>
    /// EN: Verifies GROUP BY JSON-expression complexity increases estimated cost.
    /// PT: Verifica que a complexidade de expressão JSON em GROUP BY aumenta o custo estimado.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithJsonGroupByExpressions()
    {
        var simpleGroupByQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var jsonGroupByQuery = simpleGroupByQuery with
        {
            GroupBy = ["JSON_VALUE(payload, '$.tenant')"]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleGroupByQuery, metrics, [], []);
        var jsonPlan = SqlExecutionPlanFormatter.FormatSelect(jsonGroupByQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(jsonPlan));
    }

    /// <summary>
    /// EN: Verifies estimated cost increases with additional ORDER BY keys.
    /// PT: Verifica que o custo estimado aumenta com chaves adicionais de ORDER BY.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalOrderByKeys()
    {
        var oneOrderByKeyQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoOrderByKeysQuery = oneOrderByKeyQuery with
        {
            OrderBy = [new SqlOrderByItem("id", false), new SqlOrderByItem("tenantid", false)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneKeyPlan = SqlExecutionPlanFormatter.FormatSelect(oneOrderByKeyQuery, metrics, [], []);
        var twoKeyPlan = SqlExecutionPlanFormatter.FormatSelect(twoOrderByKeysQuery, metrics, [], []);

        ExtractEstimatedCost(oneKeyPlan).Should().BeLessThan(ExtractEstimatedCost(twoKeyPlan));
    }

    /// <summary>
    /// EN: Verifies ORDER BY expression complexity increases estimated cost when ordering key uses CASE expression.
    /// PT: Verifica que a complexidade de expressão em ORDER BY aumenta o custo estimado quando a chave de ordenação usa expressão CASE.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithComplexOrderByExpressions()
    {
        var simpleOrderByQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("tenantid", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var complexOrderByQuery = simpleOrderByQuery with
        {
            OrderBy = [new SqlOrderByItem("CASE WHEN tenantid > 10 THEN 1 ELSE 0 END", false)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleOrderByQuery, metrics, [], []);
        var complexPlan = SqlExecutionPlanFormatter.FormatSelect(complexOrderByQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(complexPlan));
    }

    /// <summary>
    /// EN: Verifies ORDER BY JSON-expression complexity increases estimated cost.
    /// PT: Verifica que a complexidade de expressão JSON em ORDER BY aumenta o custo estimado.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithJsonOrderByExpressions()
    {
        var simpleOrderByQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("tenantid", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var jsonOrderByQuery = simpleOrderByQuery with
        {
            OrderBy = [new SqlOrderByItem("JSON_VALUE(payload, '$.tenant')", false)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleOrderByQuery, metrics, [], []);
        var jsonPlan = SqlExecutionPlanFormatter.FormatSelect(jsonOrderByQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(jsonPlan));
    }

    /// <summary>
    /// EN: Verifies UNION estimated cost increases with additional ORDER BY keys.
    /// PT: Verifica que o custo estimado de UNION aumenta com chaves adicionais de ORDER BY.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseWithAdditionalOrderByKeys()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var oneKeyPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], null, metrics);
        var twoKeyPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false), new SqlOrderByItem("userid", false)], null, metrics);

        ExtractEstimatedCost(oneKeyPlan).Should().BeLessThan(ExtractEstimatedCost(twoKeyPlan));
    }

    /// <summary>
    /// EN: Verifies UNION ORDER BY expression complexity increases estimated cost when ordering key uses CASE expression.
    /// PT: Verifica que a complexidade de expressão em ORDER BY de UNION aumenta o custo estimado quando a chave usa expressão CASE.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseWithComplexOrderByExpressions()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var simplePlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], null, metrics);
        var complexPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("CASE WHEN id > 10 THEN 1 ELSE 0 END", false)], null, metrics);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(complexPlan));
    }


    /// <summary>
    /// EN: Verifies estimated cost increases with larger IN-list cardinality in WHERE predicate.
    /// PT: Verifica que o custo estimado aumenta com maior cardinalidade da lista IN no predicado WHERE.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithInListCardinality()
    {
        var shortInListQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new InExpr(new IdentifierExpr("id"), [new LiteralExpr(1), new LiteralExpr(2)]),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var longInListQuery = shortInListQuery with
        {
            Where = new InExpr(new IdentifierExpr("id"), [new LiteralExpr(1), new LiteralExpr(2), new LiteralExpr(3), new LiteralExpr(4), new LiteralExpr(5)])
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var shortPlan = SqlExecutionPlanFormatter.FormatSelect(shortInListQuery, metrics, [], []);
        var longPlan = SqlExecutionPlanFormatter.FormatSelect(longInListQuery, metrics, [], []);

        ExtractEstimatedCost(shortPlan).Should().BeLessThan(ExtractEstimatedCost(longPlan));
    }


    /// <summary>
    /// EN: Verifies estimated cost increases when query includes CTE definitions versus equivalent direct SELECT.
    /// PT: Verifica que o custo estimado aumenta quando a consulta inclui definições CTE em relação ao SELECT direto equivalente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithCteDefinitions()
    {
        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var cteInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withCteQuery = baseQuery with
        {
            Ctes = [new SqlCte("u", cteInner)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var basePlan = SqlExecutionPlanFormatter.FormatSelect(baseQuery, metrics, [], []);
        var withCtePlan = SqlExecutionPlanFormatter.FormatSelect(withCteQuery, metrics, [], []);

        ExtractEstimatedCost(basePlan).Should().BeLessThan(ExtractEstimatedCost(withCtePlan));
    }

    /// <summary>
    /// EN: Verifies estimated cost increases with additional CTE declarations.
    /// PT: Verifica que o custo estimado aumenta com declarações CTE adicionais.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalCtes()
    {
        var cteInner1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var cteInner2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var oneCteQuery = new SqlSelectQuery(
            [new SqlCte("u", cteInner1)],
            false,
            [new SqlSelectItem("id", null)],
            [],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoCtesQuery = oneCteQuery with
        {
            Ctes = [new SqlCte("u", cteInner1), new SqlCte("o", cteInner2)]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var oneCtePlan = SqlExecutionPlanFormatter.FormatSelect(oneCteQuery, metrics, [], []);
        var twoCtesPlan = SqlExecutionPlanFormatter.FormatSelect(twoCtesQuery, metrics, [], []);

        ExtractEstimatedCost(oneCtePlan).Should().BeLessThan(ExtractEstimatedCost(twoCtesPlan));
    }

    /// <summary>
    /// EN: Verifies CTE cost accounts for source complexity when CTE body reads from a derived subquery source.
    /// PT: Verifica que o custo de CTE considera complexidade da fonte quando o corpo da CTE lê de uma subconsulta derivada.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWhenCteBodyUsesDerivedSource()
    {
        var simpleCteInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var derivedInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var derivedCteInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, null, "d", derivedInner, null, null, null)
        };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withSimpleCte = baseQuery with
        {
            Ctes = [new SqlCte("u", simpleCteInner)]
        };

        var withDerivedCte = baseQuery with
        {
            Ctes = [new SqlCte("u", derivedCteInner)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simpleCtePlan = SqlExecutionPlanFormatter.FormatSelect(withSimpleCte, metrics, [], []);
        var derivedCtePlan = SqlExecutionPlanFormatter.FormatSelect(withDerivedCte, metrics, [], []);

        ExtractEstimatedCost(simpleCtePlan).Should().BeLessThan(ExtractEstimatedCost(derivedCtePlan));
    }

    /// <summary>
    /// EN: Verifies CTE cost reflects row-limit relief for derived UNION sources inside CTE body.
    /// PT: Verifica que o custo de CTE reflete alívio por limite de linhas para fontes UNION derivadas dentro do corpo da CTE.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldDecreaseWhenCteDerivedUnionHasRowLimit()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var unionPart2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var cteUnionNoLimit = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                null,
                null,
                "du",
                null,
                new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], null),
                "(SELECT id FROM users UNION ALL SELECT userid FROM orders)",
                null)
        };

        var cteUnionWithLimit = cteUnionNoLimit with
        {
            Table = cteUnionNoLimit.Table with
            {
                DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(10), null))
            }
        };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withNoLimitCte = baseQuery with
        {
            Ctes = [new SqlCte("u", cteUnionNoLimit)]
        };

        var withLimitCte = baseQuery with
        {
            Ctes = [new SqlCte("u", cteUnionWithLimit)]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withNoLimitCte, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withLimitCte, metrics, [], []);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }

    /// <summary>
    /// EN: Verifies CTE-body row-limit relief is stronger for tighter limits than loose limits.
    /// PT: Verifica que o alívio de limite de linhas no corpo da CTE é mais forte para limites mais restritos do que para limites largos.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldDecreaseMoreWhenCteBodyUsesTighterRowLimit()
    {
        var cteBodyLooseLimit = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(1000), null), [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var cteBodyTightLimit = cteBodyLooseLimit with
        {
            RowLimit = new SqlLimitOffset(new LiteralExpr(10), null)
        };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withLooseLimitCte = baseQuery with
        {
            Ctes = [new SqlCte("u", cteBodyLooseLimit)]
        };

        var withTightLimitCte = baseQuery with
        {
            Ctes = [new SqlCte("u", cteBodyTightLimit)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var loosePlan = SqlExecutionPlanFormatter.FormatSelect(withLooseLimitCte, metrics, [], []);
        var tightPlan = SqlExecutionPlanFormatter.FormatSelect(withTightLimitCte, metrics, [], []);

        ExtractEstimatedCost(tightPlan).Should().BeLessThan(ExtractEstimatedCost(loosePlan));
    }

    /// <summary>
    /// EN: Verifies CTE-body large OFFSET reduces row-limit relief and therefore increases estimated cost compared with zero OFFSET.
    /// PT: Verifica que OFFSET alto no corpo da CTE reduz o alívio de limite e, portanto, aumenta o custo estimado em comparação com OFFSET zero.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWhenCteBodyRowLimitUsesLargeOffset()
    {
        var cteBodyNoOffset = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(10), null), [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var cteBodyLargeOffset = cteBodyNoOffset with
        {
            RowLimit = new SqlLimitOffset(new LiteralExpr(10), new LiteralExpr(5000))
        };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withNoOffsetCte = baseQuery with
        {
            Ctes = [new SqlCte("u", cteBodyNoOffset)]
        };

        var withLargeOffsetCte = baseQuery with
        {
            Ctes = [new SqlCte("u", cteBodyLargeOffset)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(withNoOffsetCte, metrics, [], []);
        var largeOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(withLargeOffsetCte, metrics, [], []);

        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(largeOffsetPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT + GROUP BY + ORDER BY coupling inside CTE body grows more for complex expressions than for simple key expressions.
    /// PT: Verifica que o acoplamento DISTINCT + GROUP BY + ORDER BY dentro do corpo da CTE cresce mais para expressões complexas do que para chaves simples.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseCteBodyDistinctGroupByOrderByCouplingForComplexExpressions()
    {
        var simpleCteBody = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("tenantid", false)],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var complexCteBody = simpleCteBody with
        {
            GroupBy = ["CASE WHEN status = 'A' THEN JSON_VALUE(payload, '$.tenant') ELSE tenantid END"],
            OrderBy = [new SqlOrderByItem("CASE WHEN status = 'A' THEN JSON_VALUE(payload, '$.tenant') ELSE payload->>'tenant' END", false)]
        };

        var simpleCteBodyDistinct = simpleCteBody with { Distinct = true };
        var complexCteBodyDistinct = complexCteBody with { Distinct = true };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("tenantid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withSimpleCte = baseQuery with { Ctes = [new SqlCte("u", simpleCteBody)] };
        var withSimpleDistinctCte = baseQuery with { Ctes = [new SqlCte("u", simpleCteBodyDistinct)] };
        var withComplexCte = baseQuery with { Ctes = [new SqlCte("u", complexCteBody)] };
        var withComplexDistinctCte = baseQuery with { Ctes = [new SqlCte("u", complexCteBodyDistinct)] };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(withSimpleCte, metrics, [], []);
        var simpleDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(withSimpleDistinctCte, metrics, [], []);
        var complexPlan = SqlExecutionPlanFormatter.FormatSelect(withComplexCte, metrics, [], []);
        var complexDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(withComplexDistinctCte, metrics, [], []);

        var simpleDistinctUplift = ExtractEstimatedCost(simpleDistinctPlan) - ExtractEstimatedCost(simplePlan);
        var complexDistinctUplift = ExtractEstimatedCost(complexDistinctPlan) - ExtractEstimatedCost(complexPlan);
        simpleDistinctUplift.Should().BeLessThan(complexDistinctUplift);
    }

    /// <summary>
    /// EN: Verifies DISTINCT + GROUP BY + ORDER BY coupling uplift inside CTE body grows when expansion-risk joins are present.
    /// PT: Verifica que o uplift de acoplamento DISTINCT + GROUP BY + ORDER BY no corpo da CTE cresce quando há joins com risco de expansão.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseCteBodyDistinctGroupByOrderByCouplingWhenExpansionRiskJoinsArePresent()
    {
        var cteBodyNoJoins = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("u.tenantid", false)],
            null,
            ["u.tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var cteBodyWithExpansionRiskJoins = cteBodyNoJoins with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Cross, new SqlTableSource(null, "payments", "p", null, null, null, null), new LiteralExpr(true))
            ]
        };

        var cteBodyNoJoinsDistinct = cteBodyNoJoins with { Distinct = true };
        var cteBodyWithJoinsDistinct = cteBodyWithExpansionRiskJoins with { Distinct = true };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("tenantid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withNoJoinCte = baseQuery with { Ctes = [new SqlCte("u", cteBodyNoJoins)] };
        var withNoJoinDistinctCte = baseQuery with { Ctes = [new SqlCte("u", cteBodyNoJoinsDistinct)] };
        var withJoinCte = baseQuery with { Ctes = [new SqlCte("u", cteBodyWithExpansionRiskJoins)] };
        var withJoinDistinctCte = baseQuery with { Ctes = [new SqlCte("u", cteBodyWithJoinsDistinct)] };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var noJoinPlan = SqlExecutionPlanFormatter.FormatSelect(withNoJoinCte, metrics, [], []);
        var noJoinDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(withNoJoinDistinctCte, metrics, [], []);
        var withJoinPlan = SqlExecutionPlanFormatter.FormatSelect(withJoinCte, metrics, [], []);
        var withJoinDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(withJoinDistinctCte, metrics, [], []);

        var noJoinDistinctUplift = ExtractEstimatedCost(noJoinDistinctPlan) - ExtractEstimatedCost(noJoinPlan);
        var withJoinDistinctUplift = ExtractEstimatedCost(withJoinDistinctPlan) - ExtractEstimatedCost(withJoinPlan);
        noJoinDistinctUplift.Should().BeLessThan(withJoinDistinctUplift);
    }

    /// <summary>
    /// EN: Verifies CTE-body outer ORDER BY adds stronger cost when source is already an internally ordered derived UNION.
    /// PT: Verifica que ORDER BY externo no corpo da CTE adiciona custo mais forte quando a fonte já é um UNION derivado ordenado internamente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseCteBodyNestedOrderByCouplingWhenDerivedUnionIsAlreadyOrdered()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var unionPart2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var unorderedSource = new SqlTableSource(
            null,
            null,
            "du",
            null,
            new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [], null),
            "(SELECT id FROM users UNION ALL SELECT userid FROM orders)",
            null);

        var orderedSource = unorderedSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], null)
        };

        var unorderedWithoutOuterOrderBy = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = unorderedSource
        };

        var unorderedWithOuterOrderBy = unorderedWithoutOuterOrderBy with
        {
            OrderBy = [new SqlOrderByItem("id", false)]
        };

        var orderedWithoutOuterOrderBy = unorderedWithoutOuterOrderBy with
        {
            Table = orderedSource
        };

        var orderedWithOuterOrderBy = orderedWithoutOuterOrderBy with
        {
            OrderBy = [new SqlOrderByItem("id", false)]
        };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withUnorderedNoOuterOrderCte = baseQuery with { Ctes = [new SqlCte("u", unorderedWithoutOuterOrderBy)] };
        var withUnorderedOuterOrderCte = baseQuery with { Ctes = [new SqlCte("u", unorderedWithOuterOrderBy)] };
        var withOrderedNoOuterOrderCte = baseQuery with { Ctes = [new SqlCte("u", orderedWithoutOuterOrderBy)] };
        var withOrderedOuterOrderCte = baseQuery with { Ctes = [new SqlCte("u", orderedWithOuterOrderBy)] };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var unorderedNoOuterOrderPlan = SqlExecutionPlanFormatter.FormatSelect(withUnorderedNoOuterOrderCte, metrics, [], []);
        var unorderedOuterOrderPlan = SqlExecutionPlanFormatter.FormatSelect(withUnorderedOuterOrderCte, metrics, [], []);
        var orderedNoOuterOrderPlan = SqlExecutionPlanFormatter.FormatSelect(withOrderedNoOuterOrderCte, metrics, [], []);
        var orderedOuterOrderPlan = SqlExecutionPlanFormatter.FormatSelect(withOrderedOuterOrderCte, metrics, [], []);

        var unorderedOuterOrderUplift = ExtractEstimatedCost(unorderedOuterOrderPlan) - ExtractEstimatedCost(unorderedNoOuterOrderPlan);
        var orderedOuterOrderUplift = ExtractEstimatedCost(orderedOuterOrderPlan) - ExtractEstimatedCost(orderedNoOuterOrderPlan);
        unorderedOuterOrderUplift.Should().BeLessThan(orderedOuterOrderUplift);
    }


    /// <summary>
    /// EN: Verifies tighter SELECT row limits provide larger estimated-cost relief than loose limits.
    /// PT: Verifica que limites de linha mais restritos em SELECT proporcionam maior alívio de custo estimado que limites largos.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldDecreaseMoreWithTighterRowLimit()
    {
        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var looseLimitQuery = baseQuery with { RowLimit = new SqlLimitOffset(new LiteralExpr(1000), null) };
        var tightLimitQuery = baseQuery with { RowLimit = new SqlLimitOffset(new LiteralExpr(10), null) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var looseLimitPlan = SqlExecutionPlanFormatter.FormatSelect(looseLimitQuery, metrics, [], []);
        var tightLimitPlan = SqlExecutionPlanFormatter.FormatSelect(tightLimitQuery, metrics, [], []);

        ExtractEstimatedCost(tightLimitPlan).Should().BeLessThan(ExtractEstimatedCost(looseLimitPlan));
    }

    /// <summary>
    /// EN: Verifies tighter UNION row limits provide larger estimated-cost relief than loose limits.
    /// PT: Verifica que limites de linha mais restritos em UNION proporcionam maior alívio de custo estimado que limites largos.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldDecreaseMoreWithTighterRowLimit()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var looseLimitPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(1000), null), metrics);
        var tightLimitPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(10), null), metrics);

        ExtractEstimatedCost(tightLimitPlan).Should().BeLessThan(ExtractEstimatedCost(looseLimitPlan));
    }


    /// <summary>
    /// EN: Verifies estimated cost increases with additional joins due to join-graph fan-out overhead.
    /// PT: Verifica que o custo estimado aumenta com joins adicionais devido ao overhead de fan-out do grafo de joins.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalJoins()
    {
        var oneJoinQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid")))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var twoJoinsQuery = oneJoinQuery with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "payments", "p", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("o.id"), new IdentifierExpr("p.orderid")))
            ]
        };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var oneJoinPlan = SqlExecutionPlanFormatter.FormatSelect(oneJoinQuery, metrics, [], []);
        var twoJoinsPlan = SqlExecutionPlanFormatter.FormatSelect(twoJoinsQuery, metrics, [], []);

        ExtractEstimatedCost(oneJoinPlan).Should().BeLessThan(ExtractEstimatedCost(twoJoinsPlan));
    }

    /// <summary>
    /// EN: Verifies multiple expansion-risk joins (LEFT/CROSS/RIGHT) increase estimated cost versus equivalent all-inner joins.
    /// PT: Verifica que múltiplos joins de risco de expansão (LEFT/CROSS/RIGHT) aumentam o custo estimado em relação ao equivalente só com INNER.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithMultipleExpansionRiskJoins()
    {
        var allInnerQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [
                new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "payments", "p", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("o.id"), new IdentifierExpr("p.orderid")))
            ],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var expansionRiskQuery = allInnerQuery with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Cross, new SqlTableSource(null, "payments", "p", null, null, null, null), new LiteralExpr(true))
            ]
        };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var allInnerPlan = SqlExecutionPlanFormatter.FormatSelect(allInnerQuery, metrics, [], []);
        var expansionRiskPlan = SqlExecutionPlanFormatter.FormatSelect(expansionRiskQuery, metrics, [], []);

        ExtractEstimatedCost(allInnerPlan).Should().BeLessThan(ExtractEstimatedCost(expansionRiskPlan));
    }


    /// <summary>
    /// EN: Verifies SELECT row-limit relief is reduced when large offsets are present.
    /// PT: Verifica que o alívio de custo por limite de linhas em SELECT é reduzido quando há offsets grandes.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWhenLargeOffsetReducesLimitRelief()
    {
        var noOffsetQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(10), null), [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var largeOffsetQuery = noOffsetQuery with { RowLimit = new SqlLimitOffset(new LiteralExpr(10), new LiteralExpr(5000)) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(noOffsetQuery, metrics, [], []);
        var largeOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(largeOffsetQuery, metrics, [], []);

        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(largeOffsetPlan));
    }

    /// <summary>
    /// EN: Verifies UNION row-limit relief is reduced when large offsets are present.
    /// PT: Verifica que o alívio de custo por limite de linhas em UNION é reduzido quando há offsets grandes.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseWhenLargeOffsetReducesLimitRelief()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noOffsetPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(10), null), metrics);
        var largeOffsetPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(10), new LiteralExpr(5000)), metrics);

        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(largeOffsetPlan));
    }


    /// <summary>
    /// EN: Verifies estimated cost increases with additional projected columns due to projection-width overhead.
    /// PT: Verifica que o custo estimado aumenta com colunas projetadas adicionais devido ao overhead de largura da projeção.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithProjectionWidth()
    {
        var narrowProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var wideProjectionQuery = narrowProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("id", null), new SqlSelectItem("tenantid", null), new SqlSelectItem("status", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var narrowPlan = SqlExecutionPlanFormatter.FormatSelect(narrowProjectionQuery, metrics, [], []);
        var widePlan = SqlExecutionPlanFormatter.FormatSelect(wideProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(narrowPlan).Should().BeLessThan(ExtractEstimatedCost(widePlan));
    }


    /// <summary>
    /// EN: Verifies wildcard projection carries higher estimated cost than equivalent explicit narrow projection.
    /// PT: Verifica que projeção curinga tem custo estimado maior do que projeção explícita estreita equivalente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithWildcardProjection()
    {
        var explicitProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var wildcardProjectionQuery = explicitProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("*", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var explicitPlan = SqlExecutionPlanFormatter.FormatSelect(explicitProjectionQuery, metrics, [], []);
        var wildcardPlan = SqlExecutionPlanFormatter.FormatSelect(wildcardProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(explicitPlan).Should().BeLessThan(ExtractEstimatedCost(wildcardPlan));
    }


    /// <summary>
    /// EN: Verifies CASE expression in WHERE predicate increases estimated cost compared with simple scalar predicate.
    /// PT: Verifica que expressão CASE no predicado WHERE aumenta o custo estimado em comparação com predicado escalar simples.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithCasePredicateComplexity()
    {
        var simpleQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var caseQuery = simpleQuery with
        {
            Where = new CaseExpr(
                null,
                [
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr(true)),
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.LessOrEqual, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr(false))
                ],
                new LiteralExpr(false))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleQuery, metrics, [], []);
        var casePlan = SqlExecutionPlanFormatter.FormatSelect(caseQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(casePlan));
    }

    /// <summary>
    /// EN: Verifies JSON access predicates increase estimated cost compared with simple scalar predicate.
    /// PT: Verifica que predicados com acesso JSON aumentam o custo estimado em comparação com predicado escalar simples.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithJsonAccessPredicateComplexity()
    {
        var simpleQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var jsonQuery = simpleQuery with
        {
            Where = new BinaryExpr(
                SqlBinaryOp.Eq,
                new JsonAccessExpr(new IdentifierExpr("payload"), new LiteralExpr("$.customer.id"), false),
                new LiteralExpr("42"))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleQuery, metrics, [], []);
        var jsonPlan = SqlExecutionPlanFormatter.FormatSelect(jsonQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(jsonPlan));
    }

    /// <summary>
    /// EN: Verifies JSON SQL functions in predicate AST (FunctionCallExpr/CallExpr) carry higher estimated cost than non-JSON functions with equivalent argument shapes.
    /// PT: Verifica que funções SQL JSON no AST de predicado (FunctionCallExpr/CallExpr) carregam custo estimado maior que funções não-JSON com formato de argumentos equivalente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForJsonFunctionsInPredicateFunctionNodes()
    {
        var nonJsonFunctionPredicateQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(
                SqlBinaryOp.Eq,
                new FunctionCallExpr("COALESCE", [new IdentifierExpr("payload"), new LiteralExpr("$.customer.id")]),
                new LiteralExpr("42")),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var jsonFunctionCallPredicateQuery = nonJsonFunctionPredicateQuery with
        {
            Where = new BinaryExpr(
                SqlBinaryOp.Eq,
                new FunctionCallExpr("JSON_VALUE", [new IdentifierExpr("payload"), new LiteralExpr("$.customer.id")]),
                new LiteralExpr("42"))
        };

        var jsonCallExprPredicateQuery = nonJsonFunctionPredicateQuery with
        {
            Where = new BinaryExpr(
                SqlBinaryOp.Eq,
                new CallExpr("JSON_EXTRACT", [new IdentifierExpr("payload"), new LiteralExpr("$.customer.id")]),
                new LiteralExpr("42"))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var nonJsonPlan = SqlExecutionPlanFormatter.FormatSelect(nonJsonFunctionPredicateQuery, metrics, [], []);
        var jsonFunctionCallPlan = SqlExecutionPlanFormatter.FormatSelect(jsonFunctionCallPredicateQuery, metrics, [], []);
        var jsonCallExprPlan = SqlExecutionPlanFormatter.FormatSelect(jsonCallExprPredicateQuery, metrics, [], []);

        ExtractEstimatedCost(nonJsonPlan).Should().BeLessThan(ExtractEstimatedCost(jsonFunctionCallPlan));
        ExtractEstimatedCost(nonJsonPlan).Should().BeLessThan(ExtractEstimatedCost(jsonCallExprPlan));
    }

    /// <summary>
    /// EN: Verifies raw WHERE predicates with JSON functions increase estimated cost over equivalent non-JSON raw predicates.
    /// PT: Verifica que predicados WHERE raw com funções JSON aumentam o custo estimado sobre predicados raw equivalentes sem JSON.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForRawPredicateWithJsonFunctions()
    {
        var nonJsonRawPredicateQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new RawSqlExpr("COALESCE(payload, '$.tenant') = 'acme'"),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "events", null, null, null, null, null)
        };

        var jsonRawPredicateQuery = nonJsonRawPredicateQuery with
        {
            Where = new RawSqlExpr("JSON_VALUE(payload, '$.tenant') = 'acme'")
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var nonJsonPlan = SqlExecutionPlanFormatter.FormatSelect(nonJsonRawPredicateQuery, metrics, [], []);
        var jsonPlan = SqlExecutionPlanFormatter.FormatSelect(jsonRawPredicateQuery, metrics, [], []);

        ExtractEstimatedCost(nonJsonPlan).Should().BeLessThan(ExtractEstimatedCost(jsonPlan));
    }

    /// <summary>
    /// EN: Verifies deeply nested raw WHERE predicates with mixed AND/OR plus CASE/JSON tokens increase estimated cost over flatter raw predicates.
    /// PT: Verifica que predicados WHERE raw profundamente aninhados com AND/OR mistos e tokens CASE/JSON aumentam o custo estimado sobre predicados raw mais planos.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForDeepNestedRawPredicateWithCaseJsonAndMixedLogicalOperators()
    {
        var flatterRawPredicateQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new RawSqlExpr("CASE WHEN tenantid > 100 THEN JSON_VALUE(payload, '$.tier') ELSE 'standard' END = 'gold' AND status = 'active'"),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var deepNestedRawPredicateQuery = flatterRawPredicateQuery with
        {
            Where = new RawSqlExpr("((CASE WHEN tenantid > 100 THEN JSON_VALUE(payload, '$.tier') ELSE 'standard' END = 'gold' OR payload->>'region' = 'us') AND status = 'active') OR (SELECT MAX(userid) FROM orders) > 10")
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var flatterPlan = SqlExecutionPlanFormatter.FormatSelect(flatterRawPredicateQuery, metrics, [], []);
        var deepPlan = SqlExecutionPlanFormatter.FormatSelect(deepNestedRawPredicateQuery, metrics, [], []);

        ExtractEstimatedCost(flatterPlan).Should().BeLessThan(ExtractEstimatedCost(deepPlan));
    }

    /// <summary>
    /// EN: Verifies raw WHERE predicates with JSON operators (->, ->>) increase estimated cost over equivalent scalar raw predicates.
    /// PT: Verifica que predicados WHERE raw com operadores JSON (->, ->>) aumentam o custo estimado sobre predicados raw escalares equivalentes.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForRawPredicateWithJsonOperators()
    {
        var scalarRawPredicateQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new RawSqlExpr("payload = 'x'"),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "events", null, null, null, null, null)
        };

        var jsonOperatorRawPredicateQuery = scalarRawPredicateQuery with
        {
            Where = new RawSqlExpr("payload->>'tenant' = 'acme'")
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var scalarPlan = SqlExecutionPlanFormatter.FormatSelect(scalarRawPredicateQuery, metrics, [], []);
        var jsonOperatorPlan = SqlExecutionPlanFormatter.FormatSelect(jsonOperatorRawPredicateQuery, metrics, [], []);

        ExtractEstimatedCost(scalarPlan).Should().BeLessThan(ExtractEstimatedCost(jsonOperatorPlan));
    }

    /// <summary>
    /// EN: Verifies raw predicate cost increases when AND/OR transitions are more frequent even with the same logical operator counts.
    /// PT: Verifica que o custo do predicado raw aumenta quando transições AND/OR são mais frequentes mesmo com a mesma contagem de operadores lógicos.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForRawPredicateWithAdditionalLogicalOperatorTransitions()
    {
        var lowerTransitionQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new RawSqlExpr("((a = 1 AND b = 2) AND c = 3) OR d = 4"),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var higherTransitionQuery = lowerTransitionQuery with
        {
            Where = new RawSqlExpr("(a = 1 AND (b = 2 OR c = 3)) AND d = 4")
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var lowerTransitionPlan = SqlExecutionPlanFormatter.FormatSelect(lowerTransitionQuery, metrics, [], []);
        var higherTransitionPlan = SqlExecutionPlanFormatter.FormatSelect(higherTransitionQuery, metrics, [], []);

        ExtractEstimatedCost(lowerTransitionPlan).Should().BeLessThan(ExtractEstimatedCost(higherTransitionPlan));
    }

    /// <summary>
    /// EN: Verifies raw predicate cost increases with deeper logical nesting even when AND/OR counts and transitions remain equivalent.
    /// PT: Verifica que o custo de predicado raw aumenta com aninhamento lógico mais profundo mesmo quando contagens e transições de AND/OR permanecem equivalentes.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForRawPredicateWithGreaterLogicalDepthAndSameOperatorProfile()
    {
        var shallowerLogicalRawQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new RawSqlExpr("(a = 1 OR b = 2) AND (c = 3 OR d = 4)"),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var deeperLogicalRawQuery = shallowerLogicalRawQuery with
        {
            Where = new RawSqlExpr("((a = 1 OR b = 2) AND c = 3) OR d = 4")
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var shallowerPlan = SqlExecutionPlanFormatter.FormatSelect(shallowerLogicalRawQuery, metrics, [], []);
        var deeperPlan = SqlExecutionPlanFormatter.FormatSelect(deeperLogicalRawQuery, metrics, [], []);

        ExtractEstimatedCost(shallowerPlan).Should().BeLessThan(ExtractEstimatedCost(deeperPlan));
    }

    /// <summary>
    /// EN: Verifies deeply nested logical predicates with mixed CASE/JSON leaves carry higher estimated cost than flatter logical shapes with equivalent leaves.
    /// PT: Verifica que predicados lógicos profundamente aninhados com folhas mistas de CASE/JSON carregam custo estimado maior que formatos lógicos mais planos com folhas equivalentes.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForDeepNestedLogicalPredicatesWithCaseAndJsonLeaves()
    {
        var casePredicate = new BinaryExpr(
            SqlBinaryOp.Eq,
            new CaseExpr(
                null,
                [
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr("high")),
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.LessOrEqual, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr("normal"))
                ],
                new LiteralExpr("normal")),
            new LiteralExpr("high"));

        var jsonPredicate = new BinaryExpr(
            SqlBinaryOp.Eq,
            new JsonAccessExpr(new IdentifierExpr("payload"), new LiteralExpr("$.customer.tier"), false),
            new LiteralExpr("gold"));

        var statusPredicate = new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("status"), new LiteralExpr("active"));
        var namePredicate = new LikeExpr(new IdentifierExpr("name"), new LiteralExpr("J%"));

        var flatterPredicate = new BinaryExpr(
            SqlBinaryOp.And,
            new BinaryExpr(SqlBinaryOp.Or, casePredicate, jsonPredicate),
            new BinaryExpr(SqlBinaryOp.Or, statusPredicate, namePredicate));

        var deepNestedPredicate = new BinaryExpr(
            SqlBinaryOp.Or,
            new BinaryExpr(
                SqlBinaryOp.And,
                new BinaryExpr(SqlBinaryOp.Or, casePredicate, jsonPredicate),
                statusPredicate),
            namePredicate);

        var flatterQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], flatterPredicate, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var deepQuery = flatterQuery with
        {
            Where = deepNestedPredicate
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var flatterPlan = SqlExecutionPlanFormatter.FormatSelect(flatterQuery, metrics, [], []);
        var deepPlan = SqlExecutionPlanFormatter.FormatSelect(deepQuery, metrics, [], []);

        ExtractEstimatedCost(flatterPlan).Should().BeLessThan(ExtractEstimatedCost(deepPlan));
    }

    /// <summary>
    /// EN: Verifies mixed logical operators (AND/OR) over equivalent CASE/JSON leaves carry higher estimated cost than homogeneous logical chains.
    /// PT: Verifica que operadores lógicos mistos (AND/OR) sobre folhas CASE/JSON equivalentes carregam custo estimado maior que cadeias lógicas homogêneas.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForMixedLogicalOperatorsWithCaseAndJsonLeaves()
    {
        var casePredicate = new BinaryExpr(
            SqlBinaryOp.Eq,
            new CaseExpr(
                null,
                [
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr("high"))
                ],
                new LiteralExpr("normal")),
            new LiteralExpr("high"));

        var jsonPredicate = new BinaryExpr(
            SqlBinaryOp.Eq,
            new JsonAccessExpr(new IdentifierExpr("payload"), new LiteralExpr("$.customer.tier"), false),
            new LiteralExpr("gold"));

        var statusPredicate = new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("status"), new LiteralExpr("active"));

        var homogeneousPredicate = new BinaryExpr(
            SqlBinaryOp.And,
            new BinaryExpr(SqlBinaryOp.And, casePredicate, jsonPredicate),
            statusPredicate);

        var mixedPredicate = new BinaryExpr(
            SqlBinaryOp.Or,
            new BinaryExpr(SqlBinaryOp.And, casePredicate, jsonPredicate),
            statusPredicate);

        var homogeneousQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], homogeneousPredicate, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var mixedQuery = homogeneousQuery with
        {
            Where = mixedPredicate
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var homogeneousPlan = SqlExecutionPlanFormatter.FormatSelect(homogeneousQuery, metrics, [], []);
        var mixedPlan = SqlExecutionPlanFormatter.FormatSelect(mixedQuery, metrics, [], []);

        ExtractEstimatedCost(homogeneousPlan).Should().BeLessThan(ExtractEstimatedCost(mixedPlan));
    }

    /// <summary>
    /// EN: Verifies derived UNION source cost keeps monotonic behavior for ORDER BY with row-limit and large OFFSET.
    /// PT: Verifica que o custo de fonte UNION derivada mantém comportamento monotônico para ORDER BY com limite de linhas e OFFSET alto.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldRemainMonotonicForDerivedUnionOrderByAndLimitOffset()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var unionPart2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var noLimitSource = new SqlTableSource(
            null,
            null,
            "du",
            null,
            new SqlQueryParser.UnionChain(
                [unionPart1, unionPart2],
                [true],
                [new SqlOrderByItem("id", false)],
                null),
            "(SELECT id FROM users UNION ALL SELECT userid FROM orders)",
            null);

        var noOffsetSource = noLimitSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain(
                [unionPart1, unionPart2],
                [true],
                [new SqlOrderByItem("id", false)],
                new SqlLimitOffset(new LiteralExpr(10), null))
        };

        var largeOffsetSource = noLimitSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain(
                [unionPart1, unionPart2],
                [true],
                [new SqlOrderByItem("id", false)],
                new SqlLimitOffset(new LiteralExpr(10), new LiteralExpr(5000)))
        };

        var noLimitQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = noLimitSource
        };

        var noOffsetQuery = noLimitQuery with { Table = noOffsetSource };
        var largeOffsetQuery = noLimitQuery with { Table = largeOffsetSource };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(noLimitQuery, metrics, [], []);
        var noOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(noOffsetQuery, metrics, [], []);
        var largeOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(largeOffsetQuery, metrics, [], []);

        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(largeOffsetPlan));
    }

    /// <summary>
    /// EN: Verifies outer ORDER BY introduces stronger additional cost when source is already a derived UNION with internal ORDER BY (nested sort coupling).
    /// PT: Verifica que ORDER BY externo introduz custo adicional mais forte quando a fonte já é um UNION derivado com ORDER BY interno (acoplamento de sort aninhado).
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseNestedOrderByCouplingWhenDerivedUnionIsAlreadyOrdered()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var unionPart2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var unorderedDerivedUnionSource = new SqlTableSource(
            null,
            null,
            "du",
            null,
            new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [], null),
            "(SELECT id FROM users UNION ALL SELECT userid FROM orders)",
            null);

        var orderedDerivedUnionSource = unorderedDerivedUnionSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], null)
        };

        var unorderedWithoutOuterOrderBy = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = unorderedDerivedUnionSource
        };

        var unorderedWithOuterOrderBy = unorderedWithoutOuterOrderBy with
        {
            OrderBy = [new SqlOrderByItem("id", false)]
        };

        var orderedWithoutOuterOrderBy = unorderedWithoutOuterOrderBy with
        {
            Table = orderedDerivedUnionSource
        };

        var orderedWithOuterOrderBy = orderedWithoutOuterOrderBy with
        {
            OrderBy = [new SqlOrderByItem("id", false)]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var unorderedWithoutOuterOrderByPlan = SqlExecutionPlanFormatter.FormatSelect(unorderedWithoutOuterOrderBy, metrics, [], []);
        var unorderedWithOuterOrderByPlan = SqlExecutionPlanFormatter.FormatSelect(unorderedWithOuterOrderBy, metrics, [], []);
        var orderedWithoutOuterOrderByPlan = SqlExecutionPlanFormatter.FormatSelect(orderedWithoutOuterOrderBy, metrics, [], []);
        var orderedWithOuterOrderByPlan = SqlExecutionPlanFormatter.FormatSelect(orderedWithOuterOrderBy, metrics, [], []);

        var unorderedOuterOrderByUplift = ExtractEstimatedCost(unorderedWithOuterOrderByPlan) - ExtractEstimatedCost(unorderedWithoutOuterOrderByPlan);
        var orderedOuterOrderByUplift = ExtractEstimatedCost(orderedWithOuterOrderByPlan) - ExtractEstimatedCost(orderedWithoutOuterOrderByPlan);
        unorderedOuterOrderByUplift.Should().BeLessThan(orderedOuterOrderByUplift);
    }

    /// <summary>
    /// EN: Verifies outer ORDER BY uplift increases when a JOIN source is a derived UNION already ordered internally.
    /// PT: Verifica que o uplift de ORDER BY externo aumenta quando uma fonte de JOIN é um UNION derivado já ordenado internamente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseNestedOrderByCouplingForOrderedJoinedDerivedUnionSource()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var unionPart2 = unionPart1 with
        {
            SelectItems = [new SqlSelectItem("userid", null)]
        };

        var unorderedJoinSource = new SqlTableSource(
            null,
            null,
            "duj",
            null,
            new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [], null),
            "(SELECT id FROM orders UNION ALL SELECT userid FROM orders)",
            null);

        var orderedJoinSource = unorderedJoinSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], null)
        };

        var baseQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, unorderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var unorderedJoinedNoOuterOrderBy = baseQuery;
        var unorderedJoinedWithOuterOrderBy = baseQuery with { OrderBy = [new SqlOrderByItem("u.id", false)] };
        var orderedJoinedNoOuterOrderBy = baseQuery with { Joins = [new SqlJoin(SqlJoinType.Inner, orderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))] };
        var orderedJoinedWithOuterOrderBy = orderedJoinedNoOuterOrderBy with { OrderBy = [new SqlOrderByItem("u.id", false)] };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var unorderedNoOrderPlan = SqlExecutionPlanFormatter.FormatSelect(unorderedJoinedNoOuterOrderBy, metrics, [], []);
        var unorderedWithOrderPlan = SqlExecutionPlanFormatter.FormatSelect(unorderedJoinedWithOuterOrderBy, metrics, [], []);
        var orderedNoOrderPlan = SqlExecutionPlanFormatter.FormatSelect(orderedJoinedNoOuterOrderBy, metrics, [], []);
        var orderedWithOrderPlan = SqlExecutionPlanFormatter.FormatSelect(orderedJoinedWithOuterOrderBy, metrics, [], []);

        var unorderedUplift = ExtractEstimatedCost(unorderedWithOrderPlan) - ExtractEstimatedCost(unorderedNoOrderPlan);
        var orderedUplift = ExtractEstimatedCost(orderedWithOrderPlan) - ExtractEstimatedCost(orderedNoOrderPlan);
        unorderedUplift.Should().BeLessThan(orderedUplift);
    }

    /// <summary>
    /// EN: Verifies outer ORDER BY uplift increases when a JOIN source is a derived SELECT already ordered internally.
    /// PT: Verifica que o uplift de ORDER BY externo aumenta quando uma fonte de JOIN é um SELECT derivado já ordenado internamente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseNestedOrderByCouplingForOrderedJoinedDerivedSelectSource()
    {
        var unorderedDerivedSelect = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var orderedDerivedSelect = unorderedDerivedSelect with
        {
            OrderBy = [new SqlOrderByItem("id", false)]
        };

        var unorderedJoinSource = new SqlTableSource(null, null, "dsj", unorderedDerivedSelect, null, null, null);
        var orderedJoinSource = new SqlTableSource(null, null, "dsj", orderedDerivedSelect, null, null, null);

        var baseQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, unorderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("dsj.id")))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var unorderedJoinedNoOuterOrderBy = baseQuery;
        var unorderedJoinedWithOuterOrderBy = baseQuery with { OrderBy = [new SqlOrderByItem("u.id", false)] };
        var orderedJoinedNoOuterOrderBy = baseQuery with { Joins = [new SqlJoin(SqlJoinType.Inner, orderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("dsj.id")))] };
        var orderedJoinedWithOuterOrderBy = orderedJoinedNoOuterOrderBy with { OrderBy = [new SqlOrderByItem("u.id", false)] };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var unorderedNoOrderPlan = SqlExecutionPlanFormatter.FormatSelect(unorderedJoinedNoOuterOrderBy, metrics, [], []);
        var unorderedWithOrderPlan = SqlExecutionPlanFormatter.FormatSelect(unorderedJoinedWithOuterOrderBy, metrics, [], []);
        var orderedNoOrderPlan = SqlExecutionPlanFormatter.FormatSelect(orderedJoinedNoOuterOrderBy, metrics, [], []);
        var orderedWithOrderPlan = SqlExecutionPlanFormatter.FormatSelect(orderedJoinedWithOuterOrderBy, metrics, [], []);

        var unorderedUplift = ExtractEstimatedCost(unorderedWithOrderPlan) - ExtractEstimatedCost(unorderedNoOrderPlan);
        var orderedUplift = ExtractEstimatedCost(orderedWithOrderPlan) - ExtractEstimatedCost(orderedNoOrderPlan);
        unorderedUplift.Should().BeLessThan(orderedUplift);
    }

    /// <summary>
    /// EN: Verifies nested ORDER BY coupling delta between ordered and unordered joined sources is stronger when outer ORDER BY expression is complex.
    /// PT: Verifica que o delta de acoplamento de ORDER BY aninhado entre fontes de JOIN ordenadas e não ordenadas é mais forte quando a expressão externa de ORDER BY é complexa.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseJoinedNestedOrderByCouplingDeltaForComplexOuterOrderExpression()
    {
        var unorderedDerivedSelect = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var orderedDerivedSelect = unorderedDerivedSelect with
        {
            OrderBy = [new SqlOrderByItem("id", false)]
        };

        var unorderedJoinSource = new SqlTableSource(null, null, "dsj", unorderedDerivedSelect, null, null, null);
        var orderedJoinSource = new SqlTableSource(null, null, "dsj", orderedDerivedSelect, null, null, null);

        var baseQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, unorderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("dsj.id")))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var unorderedSimpleOuterOrder = baseQuery with
        {
            OrderBy = [new SqlOrderByItem("u.id", false)]
        };

        var orderedSimpleOuterOrder = unorderedSimpleOuterOrder with
        {
            Joins = [new SqlJoin(SqlJoinType.Inner, orderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("dsj.id")))]
        };

        var unorderedComplexOuterOrder = baseQuery with
        {
            OrderBy = [new SqlOrderByItem("CASE WHEN u.status = 'A' THEN JSON_VALUE(u.payload, '$.tenant') ELSE u.id END", false)]
        };

        var orderedComplexOuterOrder = unorderedComplexOuterOrder with
        {
            Joins = [new SqlJoin(SqlJoinType.Inner, orderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("dsj.id")))]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var unorderedSimplePlan = SqlExecutionPlanFormatter.FormatSelect(unorderedSimpleOuterOrder, metrics, [], []);
        var orderedSimplePlan = SqlExecutionPlanFormatter.FormatSelect(orderedSimpleOuterOrder, metrics, [], []);
        var unorderedComplexPlan = SqlExecutionPlanFormatter.FormatSelect(unorderedComplexOuterOrder, metrics, [], []);
        var orderedComplexPlan = SqlExecutionPlanFormatter.FormatSelect(orderedComplexOuterOrder, metrics, [], []);

        var simpleDelta = ExtractEstimatedCost(orderedSimplePlan) - ExtractEstimatedCost(unorderedSimplePlan);
        var complexDelta = ExtractEstimatedCost(orderedComplexPlan) - ExtractEstimatedCost(unorderedComplexPlan);
        simpleDelta.Should().BeLessThan(complexDelta);
    }

    /// <summary>
    /// EN: Verifies nested ORDER BY coupling delta between ordered and unordered joined UNION sources is stronger when inner ORDER BY expression is complex.
    /// PT: Verifica que o delta de acoplamento de ORDER BY aninhado entre fontes UNION em JOIN ordenadas e não ordenadas é mais forte quando a expressão interna de ORDER BY é complexa.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseJoinedNestedOrderByCouplingDeltaForComplexInnerOrderExpression()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var unionPart2 = unionPart1 with
        {
            SelectItems = [new SqlSelectItem("userid", null)]
        };

        var unorderedJoinSource = new SqlTableSource(
            null,
            null,
            "duj",
            null,
            new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [], null),
            "(SELECT id FROM orders UNION ALL SELECT userid FROM orders)",
            null);

        var simpleOrderedJoinSource = unorderedJoinSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], null)
        };

        var complexOrderedJoinSource = unorderedJoinSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("CASE WHEN id > 0 THEN JSON_VALUE(payload, '$.tenant') ELSE id END", false)], null)
        };

        var baseQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, unorderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))],
            null,
            [new SqlOrderByItem("u.id", false)],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var simpleOrderedSourceQuery = baseQuery with
        {
            Joins = [new SqlJoin(SqlJoinType.Inner, simpleOrderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))]
        };

        var complexOrderedSourceQuery = baseQuery with
        {
            Joins = [new SqlJoin(SqlJoinType.Inner, complexOrderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var unorderedPlan = SqlExecutionPlanFormatter.FormatSelect(baseQuery, metrics, [], []);
        var simpleOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(simpleOrderedSourceQuery, metrics, [], []);
        var complexOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(complexOrderedSourceQuery, metrics, [], []);

        var simpleDelta = ExtractEstimatedCost(simpleOrderedPlan) - ExtractEstimatedCost(unorderedPlan);
        var complexDelta = ExtractEstimatedCost(complexOrderedPlan) - ExtractEstimatedCost(unorderedPlan);
        simpleDelta.Should().BeLessThan(complexDelta);
    }

    /// <summary>
    /// EN: Verifies nested ORDER BY coupling grows when multiple JOIN sources are internally ordered derived sources.
    /// PT: Verifica que o acoplamento de ORDER BY aninhado cresce quando múltiplas fontes de JOIN são derivadas internamente ordenadas.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseNestedOrderByCouplingWithMultipleOrderedJoinedDerivedSources()
    {
        var orderedDerivedOrders = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [new SqlOrderByItem("userid", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var orderedDerivedPayments = new SqlSelectQuery([], false, [new SqlSelectItem("orderid", null)], [], null, [new SqlOrderByItem("orderid", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "payments", null, null, null, null, null)
        };

        var oneOrderedJoinSource = new SqlTableSource(null, null, "od1", orderedDerivedOrders, null, null, null);
        var twoOrderedJoinSource = new SqlTableSource(null, null, "od2", orderedDerivedPayments, null, null, null);

        var oneOrderedJoinQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, oneOrderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("od1.userid")))],
            null,
            [new SqlOrderByItem("u.id", false)],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var twoOrderedJoinsQuery = oneOrderedJoinQuery with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Inner, oneOrderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("od1.userid"))),
                new SqlJoin(SqlJoinType.Inner, twoOrderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("od1.userid"), new IdentifierExpr("od2.orderid")))
            ]
        };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var oneOrderedJoinPlan = SqlExecutionPlanFormatter.FormatSelect(oneOrderedJoinQuery, metrics, [], []);
        var twoOrderedJoinsPlan = SqlExecutionPlanFormatter.FormatSelect(twoOrderedJoinsQuery, metrics, [], []);

        ExtractEstimatedCost(oneOrderedJoinPlan).Should().BeLessThan(ExtractEstimatedCost(twoOrderedJoinsPlan));
    }

    /// <summary>
    /// EN: Verifies outer row-limit reduces nested ORDER BY coupling uplift for ordered JOIN derived sources.
    /// PT: Verifica que limite de linhas externo reduz o uplift de acoplamento de ORDER BY aninhado para fontes derivadas ordenadas em JOIN.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldReduceNestedOrderByCouplingForOrderedJoinedDerivedSourceWhenOuterLimitIsPresent()
    {
        var orderedDerivedSelect = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var orderedJoinSource = new SqlTableSource(null, null, "dsj", orderedDerivedSelect, null, null, null);

        var withoutOuterLimit = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, orderedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("dsj.id")))],
            null,
            [new SqlOrderByItem("u.id", false)],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var withOuterLimit = withoutOuterLimit with
        {
            RowLimit = new SqlLimitOffset(new LiteralExpr(10), null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withoutOuterLimit, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withOuterLimit, metrics, [], []);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }

    /// <summary>
    /// EN: Verifies nested ORDER BY coupling for ordered JOIN derived UNION source is reduced when the inner UNION has a tight LIMIT.
    /// PT: Verifica que o acoplamento de ORDER BY aninhado para fonte UNION derivada ordenada em JOIN é reduzido quando o UNION interno tem LIMIT restrito.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldReduceJoinedNestedOrderByCouplingWhenOrderedDerivedUnionHasInnerLimit()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var unionPart2 = unionPart1 with
        {
            SelectItems = [new SqlSelectItem("userid", null)]
        };

        var orderedNoLimitJoinSource = new SqlTableSource(
            null,
            null,
            "duj",
            null,
            new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], null),
            "(SELECT id FROM orders UNION ALL SELECT userid FROM orders ORDER BY id)",
            null);

        var orderedLimitedJoinSource = orderedNoLimitJoinSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(10), null))
        };

        var baseNoOrderQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, orderedNoLimitJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var baseWithOrderQuery = baseNoOrderQuery with
        {
            OrderBy = [new SqlOrderByItem("u.id", false)]
        };

        var limitedNoOrderQuery = baseNoOrderQuery with
        {
            Joins = [new SqlJoin(SqlJoinType.Inner, orderedLimitedJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))]
        };

        var limitedWithOrderQuery = limitedNoOrderQuery with
        {
            OrderBy = [new SqlOrderByItem("u.id", false)]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noLimitNoOrderPlan = SqlExecutionPlanFormatter.FormatSelect(baseNoOrderQuery, metrics, [], []);
        var noLimitWithOrderPlan = SqlExecutionPlanFormatter.FormatSelect(baseWithOrderQuery, metrics, [], []);
        var limitedNoOrderPlan = SqlExecutionPlanFormatter.FormatSelect(limitedNoOrderQuery, metrics, [], []);
        var limitedWithOrderPlan = SqlExecutionPlanFormatter.FormatSelect(limitedWithOrderQuery, metrics, [], []);

        var noLimitUplift = ExtractEstimatedCost(noLimitWithOrderPlan) - ExtractEstimatedCost(noLimitNoOrderPlan);
        var limitedUplift = ExtractEstimatedCost(limitedWithOrderPlan) - ExtractEstimatedCost(limitedNoOrderPlan);
        limitedUplift.Should().BeLessThan(noLimitUplift);
    }

    /// <summary>
    /// EN: Verifies nested ORDER BY coupling for ordered JOIN derived UNION source increases when inner UNION LIMIT uses a large OFFSET.
    /// PT: Verifica que o acoplamento de ORDER BY aninhado para fonte UNION derivada ordenada em JOIN aumenta quando o LIMIT interno do UNION usa OFFSET alto.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseJoinedNestedOrderByCouplingWhenOrderedDerivedUnionHasLargeInnerOffset()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var unionPart2 = unionPart1 with
        {
            SelectItems = [new SqlSelectItem("userid", null)]
        };

        var orderedNoOffsetJoinSource = new SqlTableSource(
            null,
            null,
            "duj",
            null,
            new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(100), null)),
            "(SELECT id FROM orders UNION ALL SELECT userid FROM orders ORDER BY id LIMIT 100)",
            null);

        var orderedLargeOffsetJoinSource = orderedNoOffsetJoinSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(new LiteralExpr(100), new LiteralExpr(5000)))
        };

        var noOffsetNoOrderQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, orderedNoOffsetJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var noOffsetWithOrderQuery = noOffsetNoOrderQuery with
        {
            OrderBy = [new SqlOrderByItem("u.id", false)]
        };

        var largeOffsetNoOrderQuery = noOffsetNoOrderQuery with
        {
            Joins = [new SqlJoin(SqlJoinType.Inner, orderedLargeOffsetJoinSource, new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("duj.id")))]
        };

        var largeOffsetWithOrderQuery = largeOffsetNoOrderQuery with
        {
            OrderBy = [new SqlOrderByItem("u.id", false)]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noOffsetNoOrderPlan = SqlExecutionPlanFormatter.FormatSelect(noOffsetNoOrderQuery, metrics, [], []);
        var noOffsetWithOrderPlan = SqlExecutionPlanFormatter.FormatSelect(noOffsetWithOrderQuery, metrics, [], []);
        var largeOffsetNoOrderPlan = SqlExecutionPlanFormatter.FormatSelect(largeOffsetNoOrderQuery, metrics, [], []);
        var largeOffsetWithOrderPlan = SqlExecutionPlanFormatter.FormatSelect(largeOffsetWithOrderQuery, metrics, [], []);

        var noOffsetUplift = ExtractEstimatedCost(noOffsetWithOrderPlan) - ExtractEstimatedCost(noOffsetNoOrderPlan);
        var largeOffsetUplift = ExtractEstimatedCost(largeOffsetWithOrderPlan) - ExtractEstimatedCost(largeOffsetNoOrderPlan);
        noOffsetUplift.Should().BeLessThan(largeOffsetUplift);
    }

    /// <summary>
    /// EN: Verifies aggregate functions in projection add estimated cost beyond equivalent non-aggregate scalar functions.
    /// PT: Verifica que funções agregadas na projeção adicionam custo estimado além de funções escalares não agregadas equivalentes.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAggregateFunctionsInProjection()
    {
        var scalarFunctionProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("ABS(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var countProjectionQuery = scalarFunctionProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("COUNT(*)", null)]
        };

        var avgProjectionQuery = scalarFunctionProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("AVG(amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var scalarPlan = SqlExecutionPlanFormatter.FormatSelect(scalarFunctionProjectionQuery, metrics, [], []);
        var countPlan = SqlExecutionPlanFormatter.FormatSelect(countProjectionQuery, metrics, [], []);
        var avgPlan = SqlExecutionPlanFormatter.FormatSelect(avgProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(scalarPlan).Should().BeLessThan(ExtractEstimatedCost(countPlan));
        ExtractEstimatedCost(countPlan).Should().BeLessThan(ExtractEstimatedCost(avgPlan));
    }

    /// <summary>
    /// EN: Verifies projection aggregate-function weighting keeps monotonic ordering COUNT &lt; SUM &lt; AVG for equivalent single-aggregate shapes.
    /// PT: Verifica que a ponderação de funções agregadas na projeção mantém ordenação monotônica COUNT &lt; SUM &lt; AVG para formatos equivalentes de agregação única.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldKeepAggregateProjectionWeightOrderingForCountSumAvg()
    {
        var countQuery = new SqlSelectQuery([], false, [new SqlSelectItem("COUNT(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var sumQuery = countQuery with
        {
            SelectItems = [new SqlSelectItem("SUM(amount)", null)]
        };

        var avgQuery = countQuery with
        {
            SelectItems = [new SqlSelectItem("AVG(amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var countPlan = SqlExecutionPlanFormatter.FormatSelect(countQuery, metrics, [], []);
        var sumPlan = SqlExecutionPlanFormatter.FormatSelect(sumQuery, metrics, [], []);
        var avgPlan = SqlExecutionPlanFormatter.FormatSelect(avgQuery, metrics, [], []);

        ExtractEstimatedCost(countPlan).Should().BeLessThan(ExtractEstimatedCost(sumPlan));
        ExtractEstimatedCost(sumPlan).Should().BeLessThan(ExtractEstimatedCost(avgPlan));
    }

    /// <summary>
    /// EN: Verifies aggregate function detection in projection is robust to optional whitespace before parentheses.
    /// PT: Verifica que a detecção de função agregada na projeção é robusta a espaços opcionais antes dos parênteses.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldTreatAggregateProjectionWithAndWithoutWhitespaceAsEquivalent()
    {
        var compactAggregateQuery = new SqlSelectQuery([], false, [new SqlSelectItem("SUM(amount) + AVG(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var spacedAggregateQuery = compactAggregateQuery with
        {
            SelectItems = [new SqlSelectItem("SUM (amount) + AVG (amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var compactPlan = SqlExecutionPlanFormatter.FormatSelect(compactAggregateQuery, metrics, [], []);
        var spacedPlan = SqlExecutionPlanFormatter.FormatSelect(spacedAggregateQuery, metrics, [], []);

        ExtractEstimatedCost(compactPlan).Should().Be(ExtractEstimatedCost(spacedPlan));
    }

    /// <summary>
    /// EN: Verifies COUNT projection with optional whitespace still increases estimated cost over non-aggregate scalar projection.
    /// PT: Verifica que projeção COUNT com espaço opcional ainda aumenta o custo estimado sobre projeção escalar não agregada.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForCountProjectionWithWhitespace()
    {
        var scalarFunctionProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("ABS(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var countWithWhitespaceQuery = scalarFunctionProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("COUNT (*)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var scalarPlan = SqlExecutionPlanFormatter.FormatSelect(scalarFunctionProjectionQuery, metrics, [], []);
        var countPlan = SqlExecutionPlanFormatter.FormatSelect(countWithWhitespaceQuery, metrics, [], []);

        ExtractEstimatedCost(scalarPlan).Should().BeLessThan(ExtractEstimatedCost(countPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT aggregate projections carry higher estimated cost than equivalent non-distinct aggregate projections.
    /// PT: Verifica que projeções agregadas com DISTINCT carregam custo estimado maior que projeções agregadas equivalentes sem DISTINCT.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForDistinctAggregateProjection()
    {
        var nonDistinctAggregateQuery = new SqlSelectQuery([], false, [new SqlSelectItem("COUNT(tenantid)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var distinctAggregateQuery = nonDistinctAggregateQuery with
        {
            SelectItems = [new SqlSelectItem("COUNT(DISTINCT tenantid)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var nonDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(nonDistinctAggregateQuery, metrics, [], []);
        var distinctPlan = SqlExecutionPlanFormatter.FormatSelect(distinctAggregateQuery, metrics, [], []);

        ExtractEstimatedCost(nonDistinctPlan).Should().BeLessThan(ExtractEstimatedCost(distinctPlan));
    }

    /// <summary>
    /// EN: Verifies MIN/MAX aggregate projections carry higher estimated cost than equivalent non-aggregate scalar projection.
    /// PT: Verifica que projeções agregadas MIN/MAX carregam custo estimado maior que projeção escalar não agregada equivalente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForMinMaxAggregateProjection()
    {
        var scalarProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("ABS(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var minProjectionQuery = scalarProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("MIN(amount)", null)]
        };

        var maxProjectionQuery = scalarProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("MAX(amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var scalarPlan = SqlExecutionPlanFormatter.FormatSelect(scalarProjectionQuery, metrics, [], []);
        var minPlan = SqlExecutionPlanFormatter.FormatSelect(minProjectionQuery, metrics, [], []);
        var maxPlan = SqlExecutionPlanFormatter.FormatSelect(maxProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(scalarPlan).Should().BeLessThan(ExtractEstimatedCost(minPlan));
        ExtractEstimatedCost(scalarPlan).Should().BeLessThan(ExtractEstimatedCost(maxPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT MIN/MAX aggregate projections carry higher estimated cost than equivalent non-distinct MIN/MAX projections.
    /// PT: Verifica que projeções agregadas MIN/MAX com DISTINCT carregam custo estimado maior que projeções MIN/MAX equivalentes sem DISTINCT.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForDistinctMinMaxAggregateProjection()
    {
        var nonDistinctMinQuery = new SqlSelectQuery([], false, [new SqlSelectItem("MIN(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var distinctMinQuery = nonDistinctMinQuery with
        {
            SelectItems = [new SqlSelectItem("MIN(DISTINCT amount)", null)]
        };

        var nonDistinctMaxQuery = nonDistinctMinQuery with
        {
            SelectItems = [new SqlSelectItem("MAX(amount)", null)]
        };

        var distinctMaxQuery = nonDistinctMinQuery with
        {
            SelectItems = [new SqlSelectItem("MAX(DISTINCT amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var nonDistinctMinPlan = SqlExecutionPlanFormatter.FormatSelect(nonDistinctMinQuery, metrics, [], []);
        var distinctMinPlan = SqlExecutionPlanFormatter.FormatSelect(distinctMinQuery, metrics, [], []);
        var nonDistinctMaxPlan = SqlExecutionPlanFormatter.FormatSelect(nonDistinctMaxQuery, metrics, [], []);
        var distinctMaxPlan = SqlExecutionPlanFormatter.FormatSelect(distinctMaxQuery, metrics, [], []);

        ExtractEstimatedCost(nonDistinctMinPlan).Should().BeLessThan(ExtractEstimatedCost(distinctMinPlan));
        ExtractEstimatedCost(nonDistinctMaxPlan).Should().BeLessThan(ExtractEstimatedCost(distinctMaxPlan));
    }

    /// <summary>
    /// EN: Verifies projection cost increases with additional MIN/MAX aggregate calls in the same projection expression.
    /// PT: Verifica que o custo da projeção aumenta com chamadas agregadas MIN/MAX adicionais na mesma expressão de projeção.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalMinMaxAggregateCallsInProjection()
    {
        var oneAggregateQuery = new SqlSelectQuery([], false, [new SqlSelectItem("MIN(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var twoAggregatesQuery = oneAggregateQuery with
        {
            SelectItems = [new SqlSelectItem("MIN(amount) + MAX(amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneAggregatePlan = SqlExecutionPlanFormatter.FormatSelect(oneAggregateQuery, metrics, [], []);
        var twoAggregatesPlan = SqlExecutionPlanFormatter.FormatSelect(twoAggregatesQuery, metrics, [], []);

        ExtractEstimatedCost(oneAggregatePlan).Should().BeLessThan(ExtractEstimatedCost(twoAggregatesPlan));
    }

    /// <summary>
    /// EN: Verifies projection JSON function usage increases estimated cost compared with equivalent scalar projection.
    /// PT: Verifica que uso de função JSON na projeção aumenta o custo estimado em comparação com projeção escalar equivalente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithJsonFunctionsInProjection()
    {
        var scalarProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("ABS(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var jsonProjectionQuery = scalarProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("JSON_VALUE(payload, '$.tenant')", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var scalarPlan = SqlExecutionPlanFormatter.FormatSelect(scalarProjectionQuery, metrics, [], []);
        var jsonPlan = SqlExecutionPlanFormatter.FormatSelect(jsonProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(scalarPlan).Should().BeLessThan(ExtractEstimatedCost(jsonPlan));
    }

    /// <summary>
    /// EN: Verifies projection cost increases with additional JSON function calls in the same projection expression.
    /// PT: Verifica que o custo da projeção aumenta com chamadas de função JSON adicionais na mesma expressão de projeção.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalJsonFunctionsInProjection()
    {
        var oneJsonFunctionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("JSON_VALUE(payload, '$.tenant')", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var twoJsonFunctionsQuery = oneJsonFunctionQuery with
        {
            SelectItems = [new SqlSelectItem("JSON_VALUE(payload, '$.tenant') + JSON_QUERY(payload, '$.metadata')", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneJsonPlan = SqlExecutionPlanFormatter.FormatSelect(oneJsonFunctionQuery, metrics, [], []);
        var twoJsonPlan = SqlExecutionPlanFormatter.FormatSelect(twoJsonFunctionsQuery, metrics, [], []);

        ExtractEstimatedCost(oneJsonPlan).Should().BeLessThan(ExtractEstimatedCost(twoJsonPlan));
    }

    /// <summary>
    /// EN: Verifies JSON arrow operators in projection and ORDER BY expression raise estimated cost over equivalent non-JSON shapes.
    /// PT: Verifica que operadores JSON de seta em projeção e expressão ORDER BY elevam o custo estimado sobre formatos equivalentes sem JSON.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithJsonArrowOperatorsInProjectionAndOrderBy()
    {
        var nonJsonQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("payload", null)],
            [],
            null,
            [new SqlOrderByItem("payload", false)],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "events", null, null, null, null, null)
        };

        var jsonArrowQuery = nonJsonQuery with
        {
            SelectItems = [new SqlSelectItem("payload->>'tenant'", null)],
            OrderBy = [new SqlOrderByItem("payload->>'tenant'", false)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var nonJsonPlan = SqlExecutionPlanFormatter.FormatSelect(nonJsonQuery, metrics, [], []);
        var jsonArrowPlan = SqlExecutionPlanFormatter.FormatSelect(jsonArrowQuery, metrics, [], []);

        ExtractEstimatedCost(nonJsonPlan).Should().BeLessThan(ExtractEstimatedCost(jsonArrowPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT aggregate detection remains robust when optional whitespace appears after opening parenthesis.
    /// PT: Verifica que a detecção de agregação DISTINCT permanece robusta quando há espaço opcional após parêntese de abertura.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldTreatDistinctAggregateWithAndWithoutWhitespaceAsEquivalent()
    {
        var compactDistinctAggregateQuery = new SqlSelectQuery([], false, [new SqlSelectItem("SUM(DISTINCT amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var spacedDistinctAggregateQuery = compactDistinctAggregateQuery with
        {
            SelectItems = [new SqlSelectItem("SUM( DISTINCT amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var compactPlan = SqlExecutionPlanFormatter.FormatSelect(compactDistinctAggregateQuery, metrics, [], []);
        var spacedPlan = SqlExecutionPlanFormatter.FormatSelect(spacedDistinctAggregateQuery, metrics, [], []);

        ExtractEstimatedCost(compactPlan).Should().Be(ExtractEstimatedCost(spacedPlan));
    }

    /// <summary>
    /// EN: Verifies projection cost increases with additional DISTINCT aggregate calls in the same projection expression.
    /// PT: Verifica que o custo de projeção aumenta com chamadas agregadas DISTINCT adicionais na mesma expressão de projeção.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalDistinctAggregateCallsInProjection()
    {
        var oneDistinctAggregateQuery = new SqlSelectQuery([], false, [new SqlSelectItem("COUNT(DISTINCT tenantid)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoDistinctAggregatesQuery = oneDistinctAggregateQuery with
        {
            SelectItems = [new SqlSelectItem("COUNT(DISTINCT tenantid) + SUM(DISTINCT amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(oneDistinctAggregateQuery, metrics, [], []);
        var twoDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(twoDistinctAggregatesQuery, metrics, [], []);

        ExtractEstimatedCost(oneDistinctPlan).Should().BeLessThan(ExtractEstimatedCost(twoDistinctPlan));
    }

    /// <summary>
    /// EN: Verifies projection cost increases with additional CASE expressions in the same projection item.
    /// PT: Verifica que o custo da projeção aumenta com expressões CASE adicionais no mesmo item de projeção.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalCaseExpressionsInProjection()
    {
        var oneCaseProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("CASE WHEN tenantid > 10 THEN 1 ELSE 0 END", "c1")], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoCasesProjectionQuery = oneCaseProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("CASE WHEN tenantid > 10 THEN 1 ELSE 0 END + CASE WHEN status = 'A' THEN 1 ELSE 0 END", "c2")]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneCasePlan = SqlExecutionPlanFormatter.FormatSelect(oneCaseProjectionQuery, metrics, [], []);
        var twoCasesPlan = SqlExecutionPlanFormatter.FormatSelect(twoCasesProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(oneCasePlan).Should().BeLessThan(ExtractEstimatedCost(twoCasesPlan));
    }

    /// <summary>
    /// EN: Verifies projection cost increases with additional OVER clauses in the same projection item.
    /// PT: Verifica que o custo da projeção aumenta com cláusulas OVER adicionais no mesmo item de projeção.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalOverClausesInProjection()
    {
        var oneOverProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("RANK() OVER (ORDER BY tenantid)", "rk1")], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoOverProjectionQuery = oneOverProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("RANK() OVER (ORDER BY tenantid) + DENSE_RANK() OVER (ORDER BY tenantid)", "rk2")]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneOverPlan = SqlExecutionPlanFormatter.FormatSelect(oneOverProjectionQuery, metrics, [], []);
        var twoOverPlan = SqlExecutionPlanFormatter.FormatSelect(twoOverProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(oneOverPlan).Should().BeLessThan(ExtractEstimatedCost(twoOverPlan));
    }

    /// <summary>
    /// EN: Verifies projection cost increases with additional scalar subqueries in SELECT items.
    /// PT: Verifica que o custo da projeção aumenta com subconsultas escalares adicionais nos itens do SELECT.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalProjectionSubqueries()
    {
        var oneSubqueryProjectionQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("(SELECT MAX(userid) FROM orders)", "maxUserId")],
            [],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoSubqueriesProjectionQuery = oneSubqueryProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("(SELECT MAX(userid) FROM orders) + (SELECT MIN(userid) FROM orders)", "sumUserBounds")]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneSubqueryPlan = SqlExecutionPlanFormatter.FormatSelect(oneSubqueryProjectionQuery, metrics, [], []);
        var twoSubqueriesPlan = SqlExecutionPlanFormatter.FormatSelect(twoSubqueriesProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(oneSubqueryPlan).Should().BeLessThan(ExtractEstimatedCost(twoSubqueriesPlan));
    }


    /// <summary>
    /// EN: Verifies optional JSON payload mirrors common aggregated metadata from text output.
    /// PT: Verifica que o payload JSON opcional espelha metadados agregados comuns do output textual.
    /// </summary>
    [Fact]
    public void FormatSelectJson_ShouldMatchTextOutput_ForCommonAggregatedFields()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 31);
        var warnings = new[]
        {
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High, "EstimatedRowsRead", "120", "gte:100;highGte:5000")
        };
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };

        var previous = new SqlPlanRuntimeMetrics(1, 100, 10, 20);
        var previousWarnings = new[] { new SqlPlanWarning("PW002", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning) };

        var textPlan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, warnings, previous, previousWarnings, SqlPlanSeverityHintContext.Prod);
        var json = SqlExecutionPlanFormatter.FormatSelectJson(query, metrics, recommendations, warnings, previous, previousWarnings, SqlPlanSeverityHintContext.Prod);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PerformanceDisclaimerLabel()}: {root.GetProperty("performanceDisclaimer").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanMetadataVersionLabel()}: {root.GetProperty("planMetadataVersion").GetInt32()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanFlagsLabel()}: {root.GetProperty("planFlags").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanPerformanceBandLabel()}: {root.GetProperty("planPerformanceBand").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanRiskScoreLabel()}: {root.GetProperty("planRiskScore").GetInt32()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanQualityGradeLabel()}: {root.GetProperty("planQualityGrade").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanTopActionsLabel()}: {root.GetProperty("planTopActions").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanPrimaryCauseGroupLabel()}: {root.GetProperty("planPrimaryCauseGroup").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanSeverityHintLabel()}: {root.GetProperty("planSeverityHint").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanDeltaLabel()}: {root.GetProperty("planDelta").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.IndexRecommendationSummaryLabel()}: {root.GetProperty("indexRecommendationSummary").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.IndexRecommendationEvidenceLabel()}: {root.GetProperty("indexRecommendationEvidence").GetString()}");
    }

    /// <summary>
    /// EN: Verifies optional JSON payload omits warning-derived fields when warnings are absent.
    /// PT: Verifica que o payload JSON opcional omite campos derivados de warning quando não há warnings.
    /// </summary>
    [Fact]
    public void FormatSelectJson_ShouldOmitWarningDerivedFields_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 5);
        var json = SqlExecutionPlanFormatter.FormatSelectJson(query, metrics, [], []);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        root.TryGetProperty("planRiskScore", out _).Should().BeFalse();
        root.TryGetProperty("planQualityGrade", out _).Should().BeFalse();
        root.TryGetProperty("planWarningSummary", out _).Should().BeFalse();
        root.TryGetProperty("planTopActions", out _).Should().BeFalse();
        root.TryGetProperty("planPrimaryCauseGroup", out _).Should().BeFalse();
        root.TryGetProperty("planSeverityHint", out _).Should().BeFalse();
        root.TryGetProperty("planDelta", out _).Should().BeFalse();
    }


    /// <summary>
    /// EN: Verifies aggregated metadata contract is emitted with parseable stable prefixes.
    /// PT: Verifica que o contrato de metadados agregados é emitido com prefixos parseáveis estáveis.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitStableAggregatedMetadataContract_ForWarningsAndRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 31);
        var warnings = new[]
        {
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High, "EstimatedRowsRead", "120", "gte:100;highGte:5000"),
            new SqlPlanWarning("PW005", "m5", "r5", "a5", SqlPlanWarningSeverity.Warning, "EstimatedRowsRead", "120", "gte:100;highGte:5000")
        };
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, warnings);

        plan.Should().Contain("- PlanMetadataVersion: 1");
        plan.Should().Contain("- PlanCorrelationId:");
        plan.Should().Contain("- PlanFlags: hasWarnings:true;hasIndexRecommendations:true");
        plan.Should().Contain("- PlanPerformanceBand: Slow");
        plan.Should().Contain("- PlanRiskScore: 80");
        plan.Should().Contain("- PlanQualityGrade: D");
        plan.Should().Contain("- PlanWarningSummary: PW001:High;PW005:Warning");
        plan.Should().Contain("- PlanWarningCounts: high:1;warning:1;info:0");
        plan.Should().Contain("- PlanNoiseScore: 50");
        plan.Should().Contain("- PlanTopActions: PW001:AddSelectiveFilter;PW005:CreateDistinctCoveringIndex;IDX:CreateSuggestedIndex");
        plan.Should().Contain("- PlanPrimaryWarning: PW001:High");
        plan.Should().Contain($"- IndexRecommendationSummary: count:1;avgConfidence:{80:N2};maxGainPct:{50:N2}");
        plan.Should().Contain($"- IndexPrimaryRecommendation: table:users;confidence:80;gainPct:{50:N2}");
    }


    /// <summary>
    /// EN: Verifies primary index recommendation is emitted with deterministic selection.
    /// PT: Verifica que a recomendação primária de índice é emitida com seleção determinística.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitIndexPrimaryRecommendation_WhenRecommendationsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60),
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Id ON users (Id);", "reason", 80, 120, 30),
            new SqlIndexRecommendation("accounts", "CREATE INDEX IX_accounts_Status ON accounts (Status);", "reason", 70, 120, 20)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, []);
        plan.Should().Contain($"- IndexPrimaryRecommendation: table:users;confidence:80;gainPct:{75:N2}");
    }

    /// <summary>
    /// EN: Verifies primary index recommendation is omitted when recommendations are absent.
    /// PT: Verifica que a recomendação primária de índice é omitida quando não há recomendações.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitIndexPrimaryRecommendation_WhenNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().NotContain("IndexPrimaryRecommendation:");
    }


    /// <summary>
    /// EN: Verifies plan flags are emitted with stable keys and boolean values.
    /// PT: Verifica que as flags do plano são emitidas com chaves estáveis e valores booleanos.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanFlags_WithWarningsAndRecommendationsState()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };
        var warnings = new[] { new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning) };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, warnings);
        plan.Should().Contain("- PlanFlags: hasWarnings:true;hasIndexRecommendations:true");
    }

    /// <summary>
    /// EN: Verifies plan flags reflect no warnings and no index recommendations.
    /// PT: Verifica que as flags do plano refletem ausência de alerts e recomendações de índice.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanFlags_WithNoWarningsAndNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().Contain("- PlanFlags: hasWarnings:false;hasIndexRecommendations:false");
    }


    /// <summary>
    /// EN: Verifies performance band is emitted according to elapsed milliseconds.
    /// PT: Verifica que a faixa de performance é emitida conforme milissegundos de execução.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanPerformanceBand_ForElapsedMsThresholds()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var fastPlan = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 5), [], []);
        var moderatePlan = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 30), [], []);
        var slowPlan = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 31), [], []);

        fastPlan.Should().Contain("- PlanPerformanceBand: Fast");
        moderatePlan.Should().Contain("- PlanPerformanceBand: Moderate");
        slowPlan.Should().Contain("- PlanPerformanceBand: Slow");
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

    private static int ExtractEstimatedCost(string plan)
    {
        var line = plan
            .Split(new[] { Environment.NewLine }, StringSplitOptions.None)
            .First(l => l.StartsWith($"- {SqlExecutionPlanMessages.EstimatedCostLabel()}:", StringComparison.Ordinal));

        var value = line.Split([':'], 2)[1].Trim();
        return int.Parse(value, CultureInfo.InvariantCulture);
    }
}

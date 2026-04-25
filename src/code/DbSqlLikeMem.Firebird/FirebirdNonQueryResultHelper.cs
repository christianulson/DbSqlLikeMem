namespace DbSqlLikeMem.Firebird;

internal static class FirebirdNonQueryResultHelper
{
    private static readonly Regex CreateTableAsSelectPattern = new(
        @"^\s*CREATE\s+TABLE\s+.+\s+AS\s+(SELECT|WITH)\b",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

    private static readonly Regex SetGeneratorPattern = new(
        @"^\s*SET\s+GENERATOR\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly HashSet<string> NoCountDdlKeywords =
        new(StringComparer.OrdinalIgnoreCase)
        {
            "CREATE",
            "ALTER",
            "DROP",
            "TRUNCATE",
        };

    private static readonly HashSet<string> CountableDmlKeywords =
        new(StringComparer.OrdinalIgnoreCase)
        {
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
        };

    internal static int NormalizeSingleCommandResult(string sqlText, int affectedRows, ISqlDialect dialect)
    {
        if (affectedRows != 0)
            return affectedRows;

        if (IsCreateTableAsSelect(sqlText))
            return affectedRows;

        if (IsSetGenerator(sqlText))
            return -1;

        var (hasCountableDml, hasNoCountDdl, hasStatement) = AnalyzeSql(sqlText, dialect);
        if (!hasStatement || hasCountableDml)
            return affectedRows;

        return hasNoCountDdl ? -1 : affectedRows;
    }

    internal static int NormalizeBatchResult(IEnumerable<string> sqlTexts, int affectedRows, ISqlDialect dialect)
    {
        if (affectedRows != 0)
            return affectedRows;

        var hasStatement = false;
        var hasCountableDml = false;
        var hasNoCountDdl = false;

        foreach (var sqlText in sqlTexts)
        {
            if (IsCreateTableAsSelect(sqlText))
                return affectedRows;

            if (IsSetGenerator(sqlText))
                return -1;

            var (commandHasDml, commandHasNoCountDdl, commandHasStatement) = AnalyzeSql(sqlText, dialect);
            hasStatement |= commandHasStatement;
            hasCountableDml |= commandHasDml;
            hasNoCountDdl |= commandHasNoCountDdl;

            if (hasCountableDml)
                return affectedRows;
        }

        if (!hasStatement || hasCountableDml)
            return affectedRows;

        return hasNoCountDdl ? -1 : affectedRows;
    }

    private static bool IsCreateTableAsSelect(string sqlText)
        => !string.IsNullOrWhiteSpace(sqlText) && CreateTableAsSelectPattern.IsMatch(sqlText);

    private static bool IsSetGenerator(string sqlText)
        => !string.IsNullOrWhiteSpace(sqlText) && SetGeneratorPattern.IsMatch(sqlText);

    private static (bool HasCountableDml, bool HasNoCountDdl, bool HasStatement) AnalyzeSql(string sqlText, ISqlDialect dialect)
    {
        if (string.IsNullOrWhiteSpace(sqlText))
            return (false, false, false);

        var hasCountableDml = false;
        var hasNoCountDdl = false;
        var hasStatement = false;

        foreach (var statement in SqlQueryParser.SplitStatements(sqlText, dialect))
        {
            if (!TryGetLeadingKeyword(statement, out var keyword))
                continue;

            hasStatement = true;

            if (CountableDmlKeywords.Contains(keyword))
            {
                hasCountableDml = true;
                continue;
            }

            if (NoCountDdlKeywords.Contains(keyword))
                hasNoCountDdl = true;
        }

        return (hasCountableDml, hasNoCountDdl, hasStatement);
    }

    private static bool TryGetLeadingKeyword(string sqlText, out string keyword)
    {
        keyword = string.Empty;
        if (string.IsNullOrWhiteSpace(sqlText))
            return false;

        var span = sqlText.AsSpan();
        var i = 0;
        while (i < span.Length && (char.IsWhiteSpace(span[i]) || span[i] == ';'))
            i++;

        var start = i;
        while (i < span.Length && !char.IsWhiteSpace(span[i]) && span[i] != ';')
            i++;

        if (i <= start)
            return false;

        keyword = span[start..i].ToString();
        return true;
    }
}

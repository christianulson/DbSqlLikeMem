namespace DbSqlLikeMem.Db2;

internal static class Db2NonQueryResultHelper
{
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

    internal static int NormalizeSingleCommandResult(string sqlText, int affectedRows)
    {
        if (affectedRows != 0)
            return affectedRows;

        var (hasCountableDml, hasNoCountDdl, hasStatement) = AnalyzeSql(sqlText);
        if (!hasStatement || hasCountableDml)
            return affectedRows;

        return hasNoCountDdl ? -1 : affectedRows;
    }

    internal static int NormalizeBatchResult(IEnumerable<string> sqlTexts, int affectedRows)
    {
        if (affectedRows != 0)
            return affectedRows;

        var hasStatement = false;
        var hasCountableDml = false;
        var hasNoCountDdl = false;

        foreach (var sqlText in sqlTexts)
        {
            var (commandHasDml, commandHasNoCountDdl, commandHasStatement) = AnalyzeSql(sqlText);
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

    private static (bool HasCountableDml, bool HasNoCountDdl, bool HasStatement) AnalyzeSql(string sqlText)
    {
        if (string.IsNullOrWhiteSpace(sqlText))
            return (false, false, false);

        var hasCountableDml = false;
        var hasNoCountDdl = false;
        var hasStatement = false;

        foreach (var statement in SplitStatements(sqlText))
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

    private static IEnumerable<string> SplitStatements(string sqlText)
    {
        foreach (var statement in sqlText.Split(';'))
        {
            if (!string.IsNullOrWhiteSpace(statement))
                yield return statement;
        }
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

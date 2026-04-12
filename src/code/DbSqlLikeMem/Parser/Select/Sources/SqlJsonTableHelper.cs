namespace DbSqlLikeMem;

internal static class SqlJsonTableHelper
{
    internal static SqlJsonTableClause ParseJsonTableClause(string rawColumns)
    {
        var items = SqlRawCommaSplitterHelper.SplitRawByComma(rawColumns)
            .Select(static x => x.Trim())
            .Where(static x => x.Length > 0)
            .ToList();

        if (items.Count == 0)
            throw new InvalidOperationException("JSON_TABLE COLUMNS requires at least one column definition.");

        return new SqlJsonTableClause([.. items.Select(ParseJsonTableEntry)]);
    }

    internal static int IndexOfTopLevelKeyword(string sql, string keyword)
    {
        var depth = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;
        var inBacktick = false;

        for (var i = 0; i < sql.Length; i++)
        {
            var ch = sql[i];
            if (inSingleQuote)
            {
                if (ch == '\'' && i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                if (ch == '\'')
                    inSingleQuote = false;

                continue;
            }

            if (inDoubleQuote)
            {
                if (ch == '"')
                    inDoubleQuote = false;

                continue;
            }

            if (inBacktick)
            {
                if (ch == '`')
                    inBacktick = false;

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '"')
            {
                inDoubleQuote = true;
                continue;
            }

            if (ch == '`')
            {
                inBacktick = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                continue;
            }

            if (depth != 0)
                continue;

            if (!MatchesKeywordAt(sql, keyword, i))
                continue;

            var beforeOk = i == 0 || !IsKeywordIdentifierPart(sql[i - 1]);
            var afterIndex = i + keyword.Length;
            var afterOk = afterIndex >= sql.Length || !IsKeywordIdentifierPart(sql[afterIndex]);
            if (beforeOk && afterOk)
                return i;
        }

        return -1;
    }

    private static SqlJsonTableEntry ParseJsonTableEntry(string rawItem)
    {
        var item = rawItem.Trim();
        if (!item.StartsWith(SqlConst.NESTED, StringComparison.OrdinalIgnoreCase))
            return ParseJsonTableColumn(rawItem);

        var nestedMatch = Regex.Match(
            item,
            @"^NESTED(?:\s+PATH)?\s+(?<path>N?'(?:''|[^'])*')\s+(?<rest>.+)$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
        if (!nestedMatch.Success)
            throw new InvalidOperationException($"JSON_TABLE nested path definition is invalid: '{rawItem}'.");

        var rest = nestedMatch.Groups["rest"].Value.TrimStart();
        if (!rest.StartsWith(SqlConst.COLUMNS, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"JSON_TABLE nested path requires COLUMNS clause: '{rawItem}'.");

        var rawColumns = rest[SqlConst.COLUMNS.Length..].TrimStart();
        if (!TryExtractSingleParenthesizedBlock(rawColumns, out var nestedColumnsRaw, out var trailingSql))
            throw new InvalidOperationException("JSON_TABLE nested COLUMNS clause must be enclosed in parentheses.");

        if (!string.IsNullOrWhiteSpace(trailingSql))
            throw new InvalidOperationException($"JSON_TABLE nested path has unexpected tokens after COLUMNS clause: '{trailingSql.Trim()}'.");

        return new SqlJsonTableNestedPath(
            SqlOpenJsonHelper.UnquoteSqlStringLiteral(nestedMatch.Groups["path"].Value),
            ParseJsonTableClause(nestedColumnsRaw));
    }

    private static SqlJsonTableColumn ParseJsonTableColumn(string rawItem)
    {
        var item = rawItem.Trim();
        var ordinalityMatch = Regex.Match(
            item,
            @"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+FOR\s+ORDINALITY$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (ordinalityMatch.Success)
        {
            return new SqlJsonTableColumn(
                ordinalityMatch.Groups["name"].Value.NormalizeName(),
                "BIGINT",
                DbType.Int64,
                null,
                true);
        }

        var existsPathMatch = Regex.Match(
            item,
            @"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+(?<type>.+?)\s+EXISTS\s+PATH\s+(?<path>N?'(?:''|[^'])*')$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (existsPathMatch.Success)
        {
            var existsName = existsPathMatch.Groups["name"].Value.NormalizeName();
            var existsTypeSql = existsPathMatch.Groups["type"].Value.Trim();
            if (string.IsNullOrWhiteSpace(existsTypeSql))
                throw new InvalidOperationException($"JSON_TABLE column '{existsName}' requires a SQL type.");

            return new SqlJsonTableColumn(
                existsName,
                existsTypeSql,
                SqlOpenJsonHelper.ParseOpenJsonColumnDbType(existsTypeSql),
                SqlOpenJsonHelper.UnquoteSqlStringLiteral(existsPathMatch.Groups["path"].Value),
                false,
                true);
        }

        var onError = ParseJsonTableColumnFallback(ref item, "ON ERROR");
        var onEmpty = ParseJsonTableColumnFallback(ref item, "ON EMPTY");

        string? path = null;
        var pathMatch = Regex.Match(
            item,
            @"\s+PATH\s+(?<path>N?'(?:''|[^'])*')\s*$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (pathMatch.Success)
        {
            path = SqlOpenJsonHelper.UnquoteSqlStringLiteral(pathMatch.Groups["path"].Value);
            item = item[..pathMatch.Index].TrimEnd();
        }

        var nameAndTypeMatch = Regex.Match(
            item,
            @"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+(?<type>.+)$",
            RegexOptions.CultureInvariant);
        if (!nameAndTypeMatch.Success)
            throw new InvalidOperationException($"JSON_TABLE column definition is invalid: '{rawItem}'.");

        var name = nameAndTypeMatch.Groups["name"].Value.NormalizeName();
        var sqlType = nameAndTypeMatch.Groups["type"].Value.Trim();
        if (string.IsNullOrWhiteSpace(sqlType))
            throw new InvalidOperationException($"JSON_TABLE column '{name}' requires a SQL type.");

        return new SqlJsonTableColumn(
            name,
            sqlType,
            SqlOpenJsonHelper.ParseOpenJsonColumnDbType(sqlType),
            path,
            false,
            false,
            onEmpty,
            onError);
    }

    private static SqlJsonTableColumnFallback? ParseJsonTableColumnFallback(ref string item, string clauseName)
    {
        var pattern = clauseName.Equals("ON EMPTY", StringComparison.OrdinalIgnoreCase)
            ? @"^(?<prefix>.*)\s+(?<kind>NULL|ERROR|DEFAULT\s+(?<value>N?'(?:''|[^'])*'))\s+ON\s+EMPTY$"
            : @"^(?<prefix>.*)\s+(?<kind>NULL|ERROR|DEFAULT\s+(?<value>N?'(?:''|[^'])*'))\s+ON\s+ERROR$";

        var match = Regex.Match(
            item,
            pattern,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
        if (!match.Success)
            return null;

        item = match.Groups["prefix"].Value.TrimEnd();
        var kind = match.Groups["kind"].Value.Trim();
        return kind.Equals(SqlConst.NULL, StringComparison.OrdinalIgnoreCase)
            ? new SqlJsonTableColumnFallback(SqlJsonTableColumnFallbackKind.Null)
            : kind.Equals("ERROR", StringComparison.OrdinalIgnoreCase)
                ? new SqlJsonTableColumnFallback(SqlJsonTableColumnFallbackKind.Error)
                : new SqlJsonTableColumnFallback(
                    SqlJsonTableColumnFallbackKind.Default,
                    SqlOpenJsonHelper.UnquoteSqlStringLiteral(match.Groups["value"].Value));
    }

    internal static bool TryExtractSingleParenthesizedBlock(string sql, out string inner, out string trailingSql)
    {
        inner = string.Empty;
        trailingSql = string.Empty;
        if (string.IsNullOrWhiteSpace(sql) || sql[0] != '(')
            return false;

        var depth = 0;
        var inSingleQuote = false;
        for (var i = 0; i < sql.Length; i++)
        {
            var ch = sql[i];
            if (inSingleQuote)
            {
                if (ch == '\'' && i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                if (ch == '\'')
                    inSingleQuote = false;

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                if (depth == 0)
                {
                    inner = sql[1..i];
                    trailingSql = sql[(i + 1)..].Trim();
                    return true;
                }
            }
        }

        return false;
    }

    private static bool MatchesKeywordAt(string sql, string keyword, int index)
        => sql.AsSpan(index).StartsWith(keyword, StringComparison.OrdinalIgnoreCase);

    private static bool IsKeywordIdentifierPart(char ch)
        => char.IsLetterOrDigit(ch) || ch is '_' or '$' or '#';
}

namespace DbSqlLikeMem;

internal static class SqlApplyClauseHelper
{
    internal static NotSupportedException CreateApplyUnsupportedException(
        this SqlQueryParserContext ctx,
        string clause,
        int sourceOffset)
    {
        var functionInfo = ctx.TryPeekApplyTableFunctionInfo(sourceOffset);
        if (functionInfo is not null)
        {
            var (functionName, argCount) = functionInfo.Value;

            if (functionName.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase)
                && (!ctx.Dialect.TryGetTableFunctionDefinition(SqlConst.OPENJSON, out var openJsonDefinition)
                    || openJsonDefinition is null))
                return SqlUnsupported.ForDialect(ctx.Dialect, SqlConst.OPENJSON);

            if (functionName.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase)
                && (!ctx.Dialect.TryGetTableFunctionDefinition(SqlConst.JSON_TABLE, out var jsonTableDefinition)
                    || jsonTableDefinition is null))
                return SqlUnsupported.ForDialect(ctx.Dialect, SqlConst.JSON_TABLE);

            if (functionName.Equals(SqlConst.STRING_SPLIT, StringComparison.OrdinalIgnoreCase))
            {
                if (argCount == 3 && !ctx.Dialect.SupportsStringSplitOrdinalArgument)
                    return SqlUnsupported.ForDialect(ctx.Dialect, "STRING_SPLIT enable_ordinal");

                if (!ctx.Dialect.SupportsStringSplitFunction)
                    return SqlUnsupported.ForDialect(ctx.Dialect, SqlConst.STRING_SPLIT);
            }
        }

        return SqlUnsupported.ForDialect(ctx.Dialect, clause);
    }

    internal static (string Name, int ArgCount)? TryPeekApplyTableFunctionInfo(
        this SqlQueryParserContext ctx,
        int startOffset)
    {
        var parts = new List<string>();
        for (var offset = startOffset; offset <= startOffset + 24; offset++)
        {
            var token = ctx.Peek(offset);
            if (token.Kind == SqlTokenKind.EndOfFile)
                break;

            parts.Add(token.Text);
        }

        if (parts.Count == 0)
            return null;

        var names = ctx.Dialect.TableFunctions.Keys
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(static name => name.Length)
            .Select(Regex.Escape)
            .ToArray();
        if (names.Length == 0)
            return null;

        var snippet = string.Join(" ", parts);
        var match = Regex.Match(
            snippet,
            $@"(?ix)\b(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*(?:{string.Join("|", names)})\s*\(");
        if (!match.Success)
            return null;

        var functionName = match.Groups[1].Value;
        var openParenIndex = snippet.IndexOf('(', match.Index + match.Length - 1);
        if (openParenIndex < 0)
            return (functionName, 0);

        return (functionName, CountFunctionArgsInSnippet(snippet, openParenIndex));
    }

    internal static void ValidateApplySource(SqlTableSource table, string clause)
    {
        if (table.Derived is not null || table.DerivedUnion is not null || table.TableFunction is not null)
            return;

        throw new NotSupportedException($"{clause} currently supports only derived subqueries and supported table-valued functions in the mock.");
    }

    private static int CountFunctionArgsInSnippet(string snippet, int openParenIndex)
    {
        var depth = 0;
        var argCount = 0;
        var sawTokenInCurrentArg = false;

        for (var index = openParenIndex; index < snippet.Length; index++)
        {
            var ch = snippet[index];

            if (ch == '(')
            {
                depth++;
                if (depth > 1)
                    sawTokenInCurrentArg = true;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                if (depth == 0)
                {
                    if (sawTokenInCurrentArg)
                        argCount++;

                    return argCount;
                }

                sawTokenInCurrentArg = true;
                continue;
            }

            if (depth == 1 && ch == ',')
            {
                if (sawTokenInCurrentArg)
                {
                    argCount++;
                    sawTokenInCurrentArg = false;
                }

                continue;
            }

            if (depth >= 1 && !char.IsWhiteSpace(ch))
                sawTokenInCurrentArg = true;
        }

        return 0;
    }
}

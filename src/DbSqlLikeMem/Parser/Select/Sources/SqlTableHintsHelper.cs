namespace DbSqlLikeMem;

internal static class SqlTableHintsHelper
{
    internal static IReadOnlyList<SqlMySqlIndexHint> ConsumeTableHintsIfPresent(
        this SqlQueryParserContext ctx)
    {
        var mySqlHints = new List<SqlMySqlIndexHint>();

        while (true)
        {
            if (ctx.IsWord(SqlConst.WITH) && SqlQueryParserContext.IsSymbol(ctx.Peek(1), "("))
            {
                if (!ctx.Dialect.SupportsSqlServerTableHints)
                    throw ctx.NotSupported("WITH(table hints)");

                ctx.Consume(); // WITH
                _ = ctx.ReadBalancedParenRawTokens();
                continue;
            }

            if (ctx.IsSymbol("("))
            {
                if (!ctx.Dialect.SupportsSqlServerTableHints)
                    break;

                _ = ctx.ReadBalancedParenRawTokens();
                continue;
            }

            if (ctx.IsWord(SqlConst.USE) || ctx.IsWord(SqlConst.IGNORE) || ctx.IsWord(SqlConst.FORCE))
            {
                if (!ctx.Dialect.SupportsMySqlIndexHints)
                    throw ctx.NotSupported("INDEX hints");

                mySqlHints.Add(ctx.ConsumeMySqlIndexHint());
                continue;
            }

            break;
        }

        return mySqlHints;
    }

    private static SqlMySqlIndexHint ConsumeMySqlIndexHint(
        this SqlQueryParserContext ctx)
    {
        var kindToken = ctx.Consume(); // USE | IGNORE | FORCE
        var kind = kindToken.Text.NormalizeName();
        SqlMySqlIndexHintKind mappedKind;
        if (kind.Equals("use", StringComparison.OrdinalIgnoreCase))
            mappedKind = SqlMySqlIndexHintKind.Use;
        else if (kind.Equals("ignore", StringComparison.OrdinalIgnoreCase))
            mappedKind = SqlMySqlIndexHintKind.Ignore;
        else if (kind.Equals("force", StringComparison.OrdinalIgnoreCase))
            mappedKind = SqlMySqlIndexHintKind.Force;
        else
            throw new InvalidOperationException("MySQL index hint inválido: tipo de hint desconhecido.");

        if (ctx.IsWord(SqlConst.INDEX) || ctx.IsWord("KEY"))
        {
            ctx.Consume();
        }
        else
        {
            throw new InvalidOperationException("MySQL index hint inválido: esperado INDEX/KEY.");
        }

        var scope = SqlMySqlIndexHintScope.Any;
        if (ctx.IsWord(SqlConst.FOR))
        {
            ctx.Consume();
            if (ctx.IsWord(SqlConst.JOIN))
            {
                ctx.Consume();
                scope = SqlMySqlIndexHintScope.Join;
            }
            else if (ctx.IsWord(SqlConst.ORDER))
            {
                ctx.Consume();
                if (!ctx.IsWord(SqlConst.BY))
                    throw new InvalidOperationException("MySQL index hint inválido: esperado BY após ORDER.");

                ctx.Consume();
                scope = SqlMySqlIndexHintScope.OrderBy;
            }
            else if (ctx.IsWord(SqlConst.GROUP))
            {
                ctx.Consume();
                if (!ctx.IsWord(SqlConst.BY))
                    throw new InvalidOperationException("MySQL index hint inválido: esperado BY após GROUP.");

                ctx.Consume();
                scope = SqlMySqlIndexHintScope.GroupBy;
            }
            else
            {
                throw new InvalidOperationException("MySQL index hint inválido: esperado JOIN, ORDER BY ou GROUP BY após FOR.");
            }
        }

        if (!ctx.IsSymbol("("))
            throw new InvalidOperationException("MySQL index hint inválido: esperado lista de índices entre parênteses.");

        var raw = ctx.ReadBalancedParenRawTokens();
        var indexes = ValidateMySqlIndexHintList(raw);
        return new SqlMySqlIndexHint(mappedKind, scope, indexes);
    }

    private static IReadOnlyList<string> ValidateMySqlIndexHintList(string hintIndexListRaw)
    {
        var rawItems = SqlRawCommaSplitterHelper.SplitRawByComma(hintIndexListRaw)
            .ConvertAll(static x => x.Trim());

        if (rawItems.Count == 0 || rawItems.All(static x => x.Length == 0))
            throw new InvalidOperationException("MySQL index hint inválido: lista de índices vazia.");

        if (rawItems.Any(static x => x.Length == 0))
            throw new InvalidOperationException("MySQL index hint inválido: lista contém item vazio.");

        var parsedItems = new List<string>(rawItems.Count);
        foreach (var item in rawItems)
        {
            if (item.Equals(SqlConst.PRIMARY, StringComparison.OrdinalIgnoreCase))
            {
                parsedItems.Add(SqlConst.PRIMARY);
                continue;
            }

            if (Regex.IsMatch(item, @"^`(?:``|[^`])+`$", RegexOptions.CultureInvariant))
            {
                parsedItems.Add(UnquoteMySqlIdentifier(item));
                continue;
            }

            if (Regex.IsMatch(item, @"^[A-Za-z_$][A-Za-z0-9_$]*$", RegexOptions.CultureInvariant))
            {
                parsedItems.Add(item);
                continue;
            }

            throw new InvalidOperationException($"MySQL index hint inválido: índice '{item}' não é válido.");
        }

        return parsedItems;
    }

    private static string UnquoteMySqlIdentifier(string item)
        => item[1..^1].Replace("``", "`");
}

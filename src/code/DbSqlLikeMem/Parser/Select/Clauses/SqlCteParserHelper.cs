namespace DbSqlLikeMem;

internal static class SqlCteParserHelper
{
    internal static List<SqlCte> TryParseCtes(
        SqlQueryParserContext ctx,
        Func<string, SqlQueryBase> parseQuery)
    {
        return TryParseCtesCore(
            ctx.Dialect,
            () => { ctx.Consume(); },
            ctx.ExpectIdentifier,
            offset => ctx.IsWord(offset, SqlConst.WITH),
            offset => ctx.IsWord(offset, SqlConst.RECURSIVE),
            offset => ctx.IsWord(offset, SqlConst.AS),
            offset => ctx.IsWord(offset, SqlConst.NOT),
            offset => ctx.IsWord(offset, SqlConst.MATERIALIZED),
            offset => ctx.IsSymbol(offset, "("),
            offset => ctx.IsSymbol(offset, ")"),
            offset => ctx.IsSymbol(offset, ","),
            ctx.ReadBalancedParenRawTokens,
            parseQuery);
    }

    internal static List<SqlCte> TryParseCtes(
        ISqlDialect dialect,
        Func<int, SqlToken> peek,
        Action consume,
        Func<string> expectIdentifier,
        Func<SqlToken, string, bool> isWord,
        Func<SqlToken, string, bool> isSymbol,
        Func<string> readBalancedParenRawTokens,
        Func<string, SqlQueryBase> parseQuery)
    {
        return TryParseCtesCore(
            dialect,
            consume,
            expectIdentifier,
            offset => isWord(peek(offset), SqlConst.WITH),
            offset => isWord(peek(offset), SqlConst.RECURSIVE),
            offset => isWord(peek(offset), SqlConst.AS),
            offset => isWord(peek(offset), SqlConst.NOT),
            offset => isWord(peek(offset), SqlConst.MATERIALIZED),
            offset => isSymbol(peek(offset), "("),
            offset => isSymbol(peek(offset), ")"),
            offset => isSymbol(peek(offset), ","),
            readBalancedParenRawTokens,
            parseQuery);
    }

    private static List<SqlCte> TryParseCtesCore(
        ISqlDialect dialect,
        Action consume,
        Func<string> expectIdentifier,
        Func<int, bool> isWithWord,
        Func<int, bool> isRecursiveWord,
        Func<int, bool> isAsWord,
        Func<int, bool> isNotWord,
        Func<int, bool> isMaterializedWord,
        Func<int, bool> isOpenParen,
        Func<int, bool> isCloseParen,
        Func<int, bool> isComma,
        Func<string> readBalancedParenRawTokens,
        Func<string, SqlQueryBase> parseQuery)
    {
        var list = new List<SqlCte>();
        if (!isWithWord(0))
            return list;

        consume();
        if (!dialect.SupportsWithCte)
            throw SqlUnsupported.NotSupported(dialect, SqlConst.WITH_CTE);

        if (isRecursiveWord(0))
        {
            if (!dialect.SupportsWithRecursive)
                throw SqlUnsupported.NotSupportedWithRecursive(dialect);

            consume();
        }

        while (true)
        {
            var name = expectIdentifier();
            SkipOptionalCteColumnList(isOpenParen, isCloseParen, consume);

            if (!isAsWord(0))
                throw new InvalidOperationException("CTE requires AS.");

            consume();
            ConsumeOptionalMaterializedHint(
                dialect,
                consume,
                isNotWord,
                isMaterializedWord);

            var innerSql = readBalancedParenRawTokens();
            var q = parseQuery(innerSql);
            if (q is SqlSelectQuery or SqlUnionQuery)
                list.Add(new SqlCte(name, q));

            if (isComma(0))
            {
                consume();
                continue;
            }

            break;
        }

        return list;
    }

    private static void SkipOptionalCteColumnList(
        Func<int, bool> isOpenParen,
        Func<int, bool> isCloseParen,
        Action consume)
    {
        if (!isOpenParen(0))
            return;

        consume();
        while (!isCloseParen(0))
            consume();
        consume();
    }

    private static void ConsumeOptionalMaterializedHint(
        ISqlDialect dialect,
        Action consume,
        Func<int, bool> isNotWord,
        Func<int, bool> isMaterializedWord)
    {
        if (isNotWord(0) && isMaterializedWord(1))
        {
            if (!dialect.SupportsWithMaterializedHint)
                throw SqlUnsupported.NotSupported(dialect, "WITH ... AS NOT MATERIALIZED");

            consume();
            consume();
            return;
        }

        if (isMaterializedWord(0))
        {
            if (!dialect.SupportsWithMaterializedHint)
                throw SqlUnsupported.NotSupported(dialect, "WITH ... AS MATERIALIZED");

            consume();
        }
    }
}

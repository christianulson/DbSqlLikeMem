namespace DbSqlLikeMem;

internal static class SqlPartitionClauseHelper
{
    internal static IReadOnlyList<string> ConsumeOptionalTablePartitionClause(SqlQueryParserContext ctx)
    {
        if (!ctx.IsWord(SqlConst.PARTITION))
            return [];

        ctx.Consume(); // PARTITION
        if (!ctx.IsSymbol("("))
            throw new InvalidOperationException("PARTITION clause requires a partition list.");

        var rawPartitions = ctx.ReadBalancedParenRawTokens().Trim();
        var partitionNames = SqlRawCommaSplitterHelper.SplitRawByComma(rawPartitions)
            .Select(static part => part.Trim().Trim('`', '"', '[', ']').NormalizeName())
            .Where(static part => !string.IsNullOrWhiteSpace(part))
            .ToArray();

        if (partitionNames.Length == 0)
            throw new InvalidOperationException("PARTITION clause requires at least one partition name.");

        return partitionNames;
    }

    internal static IReadOnlyList<string> ConsumeOptionalTablePartitionClause(
        Func<SqlToken> peek,
        Action consume,
        Func<SqlToken, string, bool> isWord,
        Func<SqlToken, string, bool> isSymbol,
        Func<string> readBalancedParenRawTokens)
    {
        if (!isWord(peek(), SqlConst.PARTITION))
            return [];

        consume(); // PARTITION
        if (!isSymbol(peek(), "("))
            throw new InvalidOperationException("PARTITION clause requires a partition list.");

        var rawPartitions = readBalancedParenRawTokens().Trim();
        var partitionNames = SqlRawCommaSplitterHelper.SplitRawByComma(rawPartitions)
            .Select(static part => part.Trim().Trim('`', '"', '[', ']').NormalizeName())
            .Where(static part => !string.IsNullOrWhiteSpace(part))
            .ToArray();

        if (partitionNames.Length == 0)
            throw new InvalidOperationException("PARTITION clause requires at least one partition name.");

        return partitionNames;
    }
}

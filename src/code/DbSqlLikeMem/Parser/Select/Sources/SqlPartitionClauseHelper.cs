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
        var partitionNames = NormalizePartitionNames(rawPartitions);

        if (partitionNames.Count == 0)
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
        var partitionNames = NormalizePartitionNames(rawPartitions);

        if (partitionNames.Count == 0)
            throw new InvalidOperationException("PARTITION clause requires at least one partition name.");

        return partitionNames;
    }

    private static List<string> NormalizePartitionNames(string rawPartitions)
    {
        var names = new List<string>();
        foreach (var part in SqlRawCommaSplitterHelper.SplitRawByComma(rawPartitions))
        {
            var normalized = StringCompatibility.Trim(part.AsSpan(), '`', '"', '[', ']').NormalizeName();
            if (!string.IsNullOrWhiteSpace(normalized))
                names.Add(normalized);
        }

        return names;
    }
}

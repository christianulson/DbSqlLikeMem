namespace DbSqlLikeMem;

internal static class SqlInsertPartitionHelper
{
    internal static IReadOnlyList<string> ParseOptionalPartitionClause(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.IsWord(SqlConst.PARTITION))
            return [];

        ctx.Consume(); // PARTITION
        if (!ctx.IsSymbol("("))
            throw new InvalidOperationException("INSERT PARTITION clause requires a partition list.");

        var rawPartitions = ctx.ReadBalancedParenRawTokens().Trim();
        var partitionNames = SqlRawCommaSplitterHelper.SplitRawByComma(rawPartitions)
            .Select(static part => part.Trim().Trim('`', '"', '[', ']').NormalizeName())
            .Where(static part => !string.IsNullOrWhiteSpace(part))
            .ToArray();

        if (partitionNames.Length == 0)
            throw new InvalidOperationException("INSERT PARTITION clause requires at least one partition name.");

        return partitionNames;
    }
}

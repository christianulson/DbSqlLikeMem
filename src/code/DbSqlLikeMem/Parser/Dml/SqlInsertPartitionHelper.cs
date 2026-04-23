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
        var partitionNames = new List<string>();
        foreach (var part in SqlRawCommaSplitterHelper.SplitRawByComma(rawPartitions))
        {
            var normalized = StringCompatibility.Trim(part.AsSpan(), '`', '"', '[', ']').NormalizeName();
            if (!string.IsNullOrWhiteSpace(normalized))
                partitionNames.Add(normalized);
        }

        if (partitionNames.Count == 0)
            throw new InvalidOperationException("INSERT PARTITION clause requires at least one partition name.");

        return partitionNames;
    }
}

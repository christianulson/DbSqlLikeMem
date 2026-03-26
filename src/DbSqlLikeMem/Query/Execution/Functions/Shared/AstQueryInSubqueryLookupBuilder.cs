namespace DbSqlLikeMem;

internal static class AstQueryInSubqueryLookupBuilder
{
    internal static AstQueryExecutorBase.InSubqueryLookupState BuildScalarState(
        List<object?>? values)
    {
        var safeValues = values ?? [];
        if (safeValues.Count == 0)
            return new AstQueryExecutorBase.InSubqueryLookupState(safeValues, [], null, null, HasNullCandidate: false);

        var hasNullCandidate = false;
        var scalarCandidates = new HashSet<AstQueryExecutorBase.InLookupScalarKey>();
        for (var i = 0; i < safeValues.Count; i++)
        {
            var value = safeValues[i];
            if (AstQueryBinarySupportHelper.IsSqlNullLike(value))
            {
                hasNullCandidate = true;
                continue;
            }

            if (!AstQuerySubqueryLookupSupport.TryCreateInLookupScalarKey(value, null, out var key))
                return new AstQueryExecutorBase.InSubqueryLookupState(safeValues, null, null, null, hasNullCandidate);

            scalarCandidates.Add(key);
        }

        return new AstQueryExecutorBase.InSubqueryLookupState(safeValues, scalarCandidates, null, null, hasNullCandidate);
    }

    internal static AstQueryExecutorBase.InSubqueryLookupState BuildRowState(
        List<object?[]> values)
    {
        if (values.Count == 0)
            return new AstQueryExecutorBase.InSubqueryLookupState([], null, values, new HashSet<string>(StringComparer.Ordinal), HasNullCandidate: false);

        var hasNullCandidate = false;
        var rowCandidates = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (AstQueryBinarySupportHelper.HasNullElement(value))
            {
                hasNullCandidate = true;
                continue;
            }

            if (!AstQuerySubqueryLookupSupport.TryBuildInLookupCompositeKey(value, out var key))
                return new AstQueryExecutorBase.InSubqueryLookupState([], null, values, null, hasNullCandidate);

            rowCandidates.Add(key);
        }

        return new AstQueryExecutorBase.InSubqueryLookupState([], null, values, rowCandidates, hasNullCandidate);
    }
}

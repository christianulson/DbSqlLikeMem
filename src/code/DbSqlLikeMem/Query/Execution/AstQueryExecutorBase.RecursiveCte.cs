namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private const int RecursiveCteMaxIterations = 1000;

    private TableResultMock ExecuteCte(
        SqlCte cte,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow)
    {
        if (cte.IsRecursive
            && cte.Query is SqlUnionQuery recursiveUnion
            && recursiveUnion.Parts.Count >= 2
            && QueryReferencesCte(recursiveUnion, cte.Name))
        {
            return ExecuteRecursiveCte(cte, ctes, outerRow);
        }

        return cte.Query switch
        {
            SqlSelectQuery cteSelect => ExecuteSelect(cteSelect, ctes, outerRow),
            SqlUnionQuery cteUnion => ExecuteUnion(cteUnion, ctes, outerRow),
            _ => throw new NotSupportedException($"CTE query type '{cte.Query.GetType().Name}' is not supported.")
        };
    }

    private TableResultMock ExecuteRecursiveCte(
        SqlCte cte,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow)
    {
        if (cte.Query is not SqlUnionQuery union || union.Parts.Count < 2)
            throw new NotSupportedException("Recursive CTE execution requires a UNION query with anchor and recursive members.");

        var anchor = ExecuteSelect(union.Parts[0], ctes, outerRow);
        var result = CreateRecursiveCteResult(anchor);
        AppendRecursiveCteRows(anchor, result);

        var currentDelta = anchor;
        for (var iteration = 0; iteration < RecursiveCteMaxIterations; iteration++)
        {
            ctes[cte.Name] = Source.FromResult(cte.Name, currentDelta);
            var nextDelta = ExecuteRecursiveCteStep(union, ctes, outerRow);
            if (nextDelta.Count == 0)
            {
                ctes[cte.Name] = Source.FromResult(cte.Name, result);
                return result;
            }

            AppendRecursiveCteRows(nextDelta, result);
            currentDelta = nextDelta;
        }

        throw new InvalidOperationException($"Recursive CTE '{cte.Name}' exceeded {RecursiveCteMaxIterations} iterations.");
    }

    private TableResultMock ExecuteRecursiveCteStep(
        SqlUnionQuery union,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow)
    {
        if (union.Parts.Count == 2)
            return ExecuteSelect(union.Parts[1], ctes, outerRow);

        var recursiveParts = union.Parts.Skip(1).ToArray();
        var recursiveAllFlags = union.AllFlags.Skip(1).ToArray();
        var recursiveUnion = new SqlUnionQuery(recursiveParts, recursiveAllFlags, [], null)
        {
            RawSql = union.RawSql
        };

        return ExecuteUnion(recursiveUnion, ctes, outerRow);
    }

    private static TableResultMock CreateRecursiveCteResult(TableResultMock source)
        => new()
        {
            Columns = new List<TableResultColMock>(source.Columns),
            JoinFields = new List<Dictionary<string, object?>>()
        };

    private static void AppendRecursiveCteRows(TableResultMock source, TableResultMock target)
    {
        for (var rowIndex = 0; rowIndex < source.Count; rowIndex++)
        {
            target.Add(new Dictionary<int, object?>(source[rowIndex]));
            target.JoinFields.Add(rowIndex < source.JoinFields.Count
                ? new Dictionary<string, object?>(source.JoinFields[rowIndex], StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));
        }
    }

    private static bool QueryReferencesCte(SqlQueryBase query, string cteName)
        => query switch
        {
            SqlSelectQuery select => SelectReferencesCte(select, cteName),
            SqlUnionQuery union => QueryReferencesCte(union, cteName),
            _ => false
        };

    private static bool QueryReferencesCte(SqlUnionQuery union, string cteName)
        => union.Parts.Any(part => SelectReferencesCte(part, cteName));

    private static bool SelectReferencesCte(SqlSelectQuery select, string cteName)
        => TableSourceReferencesCte(select.Table, cteName)
            || select.Joins.Any(join => TableSourceReferencesCte(join.Table, cteName));

    private static bool TableSourceReferencesCte(SqlTableSource? tableSource, string cteName)
    {
        if (tableSource is null)
            return false;

        if (string.Equals(tableSource.Name, cteName, StringComparison.OrdinalIgnoreCase))
            return true;

        if (tableSource.Derived is not null && SelectReferencesCte(tableSource.Derived, cteName))
            return true;

        return tableSource.DerivedUnion is not null
            && tableSource.DerivedUnion.Parts.Any(part => SelectReferencesCte(part, cteName));
    }
}

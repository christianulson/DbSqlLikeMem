using static DbSqlLikeMem.SqlExecutionPlanFormatter;

namespace DbSqlLikeMem;

internal static class SqlExecutionPlanFormatterCostHelper
{
    internal static int EstimateSelectCost(SqlSelectQuery query)
    {
        var cost = 10;
        cost += EstimateCteCost(query.Ctes);
        cost += EstimateJoinGraphCost(query.Joins);
        cost += EstimateSourceCost(query.Table);
        cost += query.Joins.Sum(static j => EstimateSourceCost(j.Table));
        if (query.Where is not null) cost += 8 + EstimatePredicateComplexityCost(query.Where);
        cost += EstimateAggregationCost(query.GroupBy, query.Having);
        cost += EstimateSortAndDedupCost(query.OrderBy, query.Distinct, query.RowLimit);
        cost += EstimateGroupByExpressionComplexityCost(query.GroupBy);
        cost += EstimateOrderByExpressionComplexityCost(query.OrderBy);
        cost += EstimateDistinctGroupByOrderByCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.RowLimit);
        cost += EstimateDistinctGroupByOrderByJoinCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Joins);
        cost += EstimateDistinctGroupByOrderByHavingCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Having);
        cost += EstimateDistinctGroupByOrderByHavingJoinCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Having, query.Joins);
        cost += EstimateNestedOrderByCouplingCost(query.Table, query.OrderBy, query.RowLimit);
        cost += query.Joins.Sum(join => EstimateNestedOrderByCouplingCost(join.Table, query.OrderBy, query.RowLimit));
        cost += EstimateProjectionCost(query.SelectItems);
        cost -= EstimateRowLimitRelief(query.RowLimit);
        return Math.Max(1, cost);
    }

    internal static int EstimateInsertCost(SqlInsertQuery query)
    {
        var cost = query.IsReplace ? 12 : 8;
        cost += query.Columns.Count * 2;
        cost += query.ValuesRaw.Count * 3;

        if (query.InsertSelect is not null)
            cost += EstimateSelectCost(query.InsertSelect);

        if (query.IsReplace)
            cost += 4 + query.ValuesRaw.Count * 2;

        if (query.HasOnDuplicateKeyUpdate)
            cost += 15 + query.OnDupAssigns.Count * 4;

        if (query.Returning.Count > 0)
            cost += query.Returning.Count * 2;

        return Math.Max(1, cost);
    }

    internal static int EstimateUpdateCost(SqlUpdateQuery query)
    {
        var cost = 12;
        cost += query.Set.Count * 4;

        if (query.Where is not null)
            cost += 8;

        if (query.UpdateFromSelect is not null)
            cost += EstimateSelectCost(query.UpdateFromSelect);

        if (query.Returning.Count > 0)
            cost += query.Returning.Count * 2;

        return Math.Max(1, cost);
    }

    internal static int EstimateDeleteCost(SqlDeleteQuery query)
    {
        var cost = 10;

        if (query.Where is not null || !string.IsNullOrWhiteSpace(query.WhereRaw))
            cost += 8;

        if (query.DeleteFromSelect is not null)
            cost += EstimateSelectCost(query.DeleteFromSelect);

        if (query.Returning.Count > 0)
            cost += query.Returning.Count * 2;

        return Math.Max(1, cost);
    }

    internal static int EstimateUnionCost(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy,
        SqlRowLimit? rowLimit)
    {
        var cost = parts.Sum(EstimateSelectCost) + 12;
        cost += allFlags.Count(flag => !flag) * 20;
        cost += EstimateUnionSetOperatorTransitionCost(allFlags);
        cost += EstimateUnionOrderByMergeFanInCost(parts.Count, orderBy ?? [], rowLimit);
        cost += EstimateSortAndDedupCost(orderBy ?? [], false, rowLimit);
        cost += EstimateOrderByExpressionComplexityCost(orderBy ?? []);
        cost -= EstimateRowLimitRelief(rowLimit);
        return Math.Max(1, cost);
    }
}

using System.Resources;

namespace DbSqlLikeMem.Resources;

internal static class SqlExecutionPlanMessages
{
    private static readonly ResourceManager ResourceManager =
        new("DbSqlLikeMem.Resources.SqlExecutionPlanMessages", typeof(SqlExecutionPlanMessages).Assembly);

    public static string ExecutionPlanTitle() => Format(nameof(ExecutionPlanTitle));
    public static string QueryTypeLabel() => Format(nameof(QueryTypeLabel));
    public static string EstimatedCostLabel() => Format(nameof(EstimatedCostLabel));
    public static string CtesLabel() => Format(nameof(CtesLabel));
    public static string CteMaterializeLabel() => Format(nameof(CteMaterializeLabel));
    public static string FromLabel() => Format(nameof(FromLabel));
    public static string JoinLabel() => Format(nameof(JoinLabel));
    public static string FilterLabel() => Format(nameof(FilterLabel));
    public static string GroupByLabel() => Format(nameof(GroupByLabel));
    public static string HavingLabel() => Format(nameof(HavingLabel));
    public static string ProjectionLabel() => Format(nameof(ProjectionLabel));
    public static string DistinctLabel() => Format(nameof(DistinctLabel));
    public static string SortLabel() => Format(nameof(SortLabel));
    public static string LimitLabel() => Format(nameof(LimitLabel));
    public static string InputTablesLabel() => Format(nameof(InputTablesLabel));
    public static string EstimatedRowsReadLabel() => Format(nameof(EstimatedRowsReadLabel));
    public static string ActualRowsLabel() => Format(nameof(ActualRowsLabel));
    public static string SelectivityPctLabel() => Format(nameof(SelectivityPctLabel));
    public static string RowsPerMsLabel() => Format(nameof(RowsPerMsLabel));
    public static string ElapsedMsLabel() => Format(nameof(ElapsedMsLabel));
    public static string IndexRecommendationsLabel() => Format(nameof(IndexRecommendationsLabel));
    public static string TableLabel() => Format(nameof(TableLabel));
    public static string SuggestedIndexLabel() => Format(nameof(SuggestedIndexLabel));
    public static string ReasonLabel() => Format(nameof(ReasonLabel));
    public static string ConfidenceLabel() => Format(nameof(ConfidenceLabel));
    public static string EstimatedRowsReadBeforeLabel() => Format(nameof(EstimatedRowsReadBeforeLabel));
    public static string EstimatedRowsReadAfterLabel() => Format(nameof(EstimatedRowsReadAfterLabel));
    public static string EstimatedGainPctLabel() => Format(nameof(EstimatedGainPctLabel));
    public static string PartsLabel() => Format(nameof(PartsLabel));
    public static string PartLabel() => Format(nameof(PartLabel));
    public static string CombineLabel() => Format(nameof(CombineLabel));
    public static string PlanWarningsLabel() => Format(nameof(PlanWarningsLabel));
    public static string CodeLabel() => Format(nameof(CodeLabel));
    public static string MessageLabel() => Format(nameof(MessageLabel));
    public static string SuggestedActionLabel() => Format(nameof(SuggestedActionLabel));
    public static string SeverityLabel() => Format(nameof(SeverityLabel));

    public static string ReasonFilterAndOrder(string filters, string orders, string key)
        => Format(nameof(ReasonFilterAndOrder), filters, orders, key);

    public static string ReasonFilterOnly(string filters)
        => Format(nameof(ReasonFilterOnly), filters);

    public static string ReasonOrderOnly(string orders)
        => Format(nameof(ReasonOrderOnly), orders);

    public static string WarningOrderByWithoutLimitMessage()
        => Format(nameof(WarningOrderByWithoutLimitMessage));


    public static string SeverityInfoValue() => Format(nameof(SeverityInfoValue));
    public static string SeverityWarningValue() => Format(nameof(SeverityWarningValue));
    public static string SeverityHighValue() => Format(nameof(SeverityHighValue));

    public static string WarningOrderByWithoutLimitReason(long estimatedRowsRead)
        => Format(nameof(WarningOrderByWithoutLimitReason), estimatedRowsRead);

    public static string WarningOrderByWithoutLimitAction()
        => Format(nameof(WarningOrderByWithoutLimitAction));

    public static string WarningLowSelectivityMessage()
        => Format(nameof(WarningLowSelectivityMessage));

    public static string WarningLowSelectivityReason(double selectivityPct, long estimatedRowsRead)
        => Format(nameof(WarningLowSelectivityReason), selectivityPct, estimatedRowsRead);

    public static string WarningLowSelectivityAction()
        => Format(nameof(WarningLowSelectivityAction));

    public static string WarningSelectStarMessage()
        => Format(nameof(WarningSelectStarMessage));

    public static string WarningSelectStarReason(long estimatedRowsRead)
        => Format(nameof(WarningSelectStarReason), estimatedRowsRead);

    public static string WarningSelectStarAction()
        => Format(nameof(WarningSelectStarAction));

    private static string Format(string key, params object?[] args)
    {
        var template = ResourceManager.GetString(key, CultureInfo.CurrentUICulture)
            ?? ResourceManager.GetString(key, CultureInfo.InvariantCulture)
            ?? key;

        return string.Format(CultureInfo.CurrentCulture, template, args);
    }
}

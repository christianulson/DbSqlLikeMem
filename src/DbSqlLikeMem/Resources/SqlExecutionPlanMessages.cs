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
    public static string MetricNameLabel() => Format(nameof(MetricNameLabel));
    public static string ObservedValueLabel() => Format(nameof(ObservedValueLabel));
    public static string ThresholdLabel() => Format(nameof(ThresholdLabel));

    public static string PlanMetadataVersionLabel() => Format(nameof(PlanMetadataVersionLabel));
    public static string PlanCorrelationIdLabel() => Format(nameof(PlanCorrelationIdLabel));
    public static string PlanFlagsLabel() => Format(nameof(PlanFlagsLabel));
    public static string PlanPerformanceBandLabel() => Format(nameof(PlanPerformanceBandLabel));
    public static string PlanRiskScoreLabel() => Format(nameof(PlanRiskScoreLabel));
    public static string PlanQualityGradeLabel() => Format(nameof(PlanQualityGradeLabel));
    public static string PlanWarningSummaryLabel() => Format(nameof(PlanWarningSummaryLabel));
    public static string PlanWarningCountsLabel() => Format(nameof(PlanWarningCountsLabel));
    public static string PlanNoiseScoreLabel() => Format(nameof(PlanNoiseScoreLabel));
    public static string PlanTopActionsLabel() => Format(nameof(PlanTopActionsLabel));
    public static string PlanPrimaryWarningLabel() => Format(nameof(PlanPrimaryWarningLabel));
    public static string PlanPrimaryCauseGroupLabel() => Format(nameof(PlanPrimaryCauseGroupLabel));
    public static string PlanDeltaLabel() => Format(nameof(PlanDeltaLabel));
    public static string PlanSeverityHintLabel() => Format(nameof(PlanSeverityHintLabel));
    public static string IndexRecommendationSummaryLabel() => Format(nameof(IndexRecommendationSummaryLabel));
    public static string IndexPrimaryRecommendationLabel() => Format(nameof(IndexPrimaryRecommendationLabel));
    public static string IndexRecommendationEvidenceLabel() => Format(nameof(IndexRecommendationEvidenceLabel));

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

    public static string WarningLowSelectivityHighImpactMessage()
        => Format(nameof(WarningLowSelectivityHighImpactMessage));

    public static string WarningLowSelectivityReason(double selectivityPct, long estimatedRowsRead)
        => Format(nameof(WarningLowSelectivityReason), selectivityPct, estimatedRowsRead);

    public static string WarningLowSelectivityAction()
        => Format(nameof(WarningLowSelectivityAction));

    public static string WarningSelectStarMessage()
        => Format(nameof(WarningSelectStarMessage));

    public static string WarningSelectStarHighImpactMessage()
        => Format(nameof(WarningSelectStarHighImpactMessage));

    public static string WarningSelectStarCriticalImpactMessage()
        => Format(nameof(WarningSelectStarCriticalImpactMessage));

    public static string WarningSelectStarReason(long estimatedRowsRead)
        => Format(nameof(WarningSelectStarReason), estimatedRowsRead);

    public static string WarningSelectStarAction()
        => Format(nameof(WarningSelectStarAction));


    public static string WarningNoWhereHighReadMessage()
        => Format(nameof(WarningNoWhereHighReadMessage));

    public static string WarningNoWhereHighReadHighImpactMessage()
        => Format(nameof(WarningNoWhereHighReadHighImpactMessage));

    public static string WarningNoWhereHighReadReason(long estimatedRowsRead)
        => Format(nameof(WarningNoWhereHighReadReason), estimatedRowsRead);

    public static string WarningNoWhereHighReadAction()
        => Format(nameof(WarningNoWhereHighReadAction));


    public static string WarningDistinctHighReadMessage()
        => Format(nameof(WarningDistinctHighReadMessage));

    public static string WarningDistinctHighReadHighImpactMessage()
        => Format(nameof(WarningDistinctHighReadHighImpactMessage));

    public static string WarningDistinctHighReadReason(long estimatedRowsRead)
        => Format(nameof(WarningDistinctHighReadReason), estimatedRowsRead);

    public static string WarningDistinctHighReadAction()
        => Format(nameof(WarningDistinctHighReadAction));

    private static string Format(string key, params object?[] args)
    {
        var template = ResourceManager.GetString(key, CultureInfo.CurrentUICulture)
            ?? ResourceManager.GetString(key, CultureInfo.InvariantCulture)
            ?? key;

        return string.Format(CultureInfo.CurrentCulture, template, args);
    }
}

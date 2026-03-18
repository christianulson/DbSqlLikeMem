namespace DbSqlLikeMem;

internal static class DbPerformanceMetricKeys
{
    public const string SqlParse = "sql.parse";
    public const string IndexRebuild = "index.rebuild";
    public const string IndexUpdate = "index.update";
    public const string IndexRemove = "index.remove";
    public const string IndexShift = "index.shift";
    public const string MaterializationLinqPlan = "materialization.linq.plan";
    public const string MaterializationLinqRow = "materialization.linq.row";
    public const string MaterializationObjectRow = "materialization.object.row";
}

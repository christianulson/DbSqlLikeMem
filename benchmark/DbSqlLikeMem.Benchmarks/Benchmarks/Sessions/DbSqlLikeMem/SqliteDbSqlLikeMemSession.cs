using DbSqlLikeMem.Sqlite;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// 
/// </summary>
public sealed class SqliteDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new SqliteDialect())
{
    private readonly SqliteDbMock _singleThreadDb = new()
    {
        ThreadSafe = false,
        CaptureExecutionPlans = true
    };

    private readonly SqliteDbMock _parallelDb = new()
    {
        ThreadSafe = true,
        CaptureExecutionPlans = true
    };

    private BenchmarkFeatureId _currentFeature;

    public override void Execute(BenchmarkFeatureId feature)
    {
        _currentFeature = feature;
        base.Execute(feature);
    }

    /// <summary>
    /// 
    /// </summary>
    protected override DbConnection CreateConnection()
    {
        var db = _currentFeature == BenchmarkFeatureId.InsertBatch100Parallel
            ? _parallelDb
            : _singleThreadDb;

        var capturePlans = _currentFeature is BenchmarkFeatureId.ExecutionPlan
            or BenchmarkFeatureId.ExecutionPlanSelect
            or BenchmarkFeatureId.ExecutionPlanJoin
            or BenchmarkFeatureId.ExecutionPlanDml
            or BenchmarkFeatureId.LastExecutionPlansHistory;

        return new SqliteConnectionMock(db)
        {
            CaptureExecutionPlans = capturePlans,
            CaptureAffectedRowSnapshots = false
        };
    }
}

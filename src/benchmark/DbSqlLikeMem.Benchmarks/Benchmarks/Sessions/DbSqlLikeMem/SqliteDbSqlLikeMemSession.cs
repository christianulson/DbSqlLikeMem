using DbSqlLikeMem.Sqlite;
using DbSqlLikeMem.Sqlite.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs the benchmark session against the SQLite mock provider.
/// PT: Executa a sessão de benchmark contra o provider mock de SQLite.
/// </summary>
public sealed class SqliteDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new SqliteProviderSqlDialect())
{
    private readonly SqliteDbMock _singleThreadDb = new()
    {
        ThreadSafe = false,
        CaptureExecutionPlans = false
    };

    private readonly SqliteDbMock _parallelDb = new()
    {
        ThreadSafe = true,
        CaptureExecutionPlans = false
    };

    private BenchmarkFeatureId _currentFeature;

    /// <summary>
    /// EN: Dispatches the requested benchmark feature for the current SQLite session.
    /// PT: Encaminha o recurso de benchmark solicitado para a sessão SQLite atual.
    /// </summary>
    public override void Execute(BenchmarkFeatureId feature)
    {
        _currentFeature = feature;
        base.Execute(feature);
    }

    /// <summary>
    /// EN: Creates the SQLite mock connection used by the benchmark session.
    /// PT: Cria a conexão mock de SQLite usada pela sessão de benchmark.
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

        var connection = new SqliteConnectionMock(db)
        {
            CaptureExecutionPlans = capturePlans,
            CaptureAffectedRowSnapshots = false
        };

        connection.Metrics.Enabled = capturePlans;
        return connection;
    }
}

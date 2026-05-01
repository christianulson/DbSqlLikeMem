using DbSqlLikeMem.TestTools.Benchmarks;
using DbSqlLikeMem.TestTools.DDL;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Query;
using DbSqlLikeMem.TestTools.Schema;
using DbSqlLikeMem.TestTools.TemporaryTable;
using System.Globalization;
using System.Collections.Concurrent;

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    private readonly object _preparedStateSync = new();
    private readonly ConcurrentDictionary<string, IDisposable> _preparedStates = new();

    protected static string NextToken() => Interlocked.Increment(ref _objectCounter).ToString("x8", CultureInfo.InvariantCulture).ToUpperInvariant();

    private PreparedScenarioScope<TScenario, TService> CreatePreparedScenarioScope<TScenario, TService>(
        Func<RepoService, FidelityTestContext, TScenario> scenarioFactory,
        Func<RepoService, FidelityTestContext, TService> serviceFactory)
        where TScenario : class, ITestScenario
    {
        var repo = new RepoService(CreateConnection, Dialect);
        repo.Cnn.Open();

        var context = new FidelityTestContext();
        var scenario = scenarioFactory(repo, context);
        scenario.CreateScenarioAsync().GetAwaiter().GetResult();
        var service = serviceFactory(repo, context);

        return new PreparedScenarioScope<TScenario, TService>(repo, context, scenario, service);
    }

    private NotFidelityTestService<DbConnection> CreateBenchmarkRunner(params object?[][] initialData)
        => new(CreateConnection, Dialect, initialData);

    /// <summary>
    /// EN: Measures the cost of opening a new database connection.
    /// PT-br: Mede o custo de abrir uma nova conexão de banco de dados.
    /// </summary>
    protected virtual void RunConnectionOpen()
    {
        using var connection = CreateConnection();
        connection.Open();
        GC.KeepAlive(connection.State);
    }

    private PreparedScenarioScope<SelectTableScenario, SelectByPKServiceTest> CreateSelectByPkScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateSelectTableScenario,
            (repo, context) => new SelectByPKServiceTest(repo, context));

    private PreparedScenarioScope<SelectTableScenario, QueryServiceTest> CreateSelectTableQueryScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateSelectTableScenario,
            (repo, context) => new QueryServiceTest(repo, context));

    private PreparedScenarioScope<UsersOrdersScenario, DmlMutationSelectJoinServiceTest> CreateUsersOrdersMutationScope(
        (int id, string name)[]? seedUsers = null,
        (int id, int userId, string note)[]? seedOrders = null)
        => CreatePreparedScenarioScope(
            (repo, context) => BenchmarkScenarioFactory.CreateUsersOrdersScenario(repo, context, seedUsers, seedOrders),
            (repo, context) => new DmlMutationSelectJoinServiceTest(repo, context));


    private PreparedScenarioScope<CreateTableScenario, CreateTableServiceTest> CreateCreateTableScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateTableScenario,
            (repo, context) => new CreateTableServiceTest(repo, context));

    private PreparedScenarioScope<CreateTableWithFKScenario, CreateTableWithFKServiceTest> CreateCreateTableWithFkScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateTableWithFKScenario,
            (repo, context) => new CreateTableWithFKServiceTest(repo, context));

    private PreparedScenarioScope<DropTableScenario, DropTableServiceTest> CreateDropTableScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateDropTableScenario,
            (repo, context) => new DropTableServiceTest(repo, context));

    private PreparedScenarioScope<InsertUsersScenario, BatchInsertReturningServiceTest> CreateReturningInsertScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateInsertUsersScenario,
            (repo, context) => new BatchInsertReturningServiceTest(repo, context));

    internal PreparedScenarioScope<TemporaryTableScenario, TemporaryTableServiceOpsTest> CreateTemporaryTableScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateTemporaryTableScenario,
            (repo, context) => new TemporaryTableServiceOpsTest(repo, context));

    internal PreparedScenarioScope<TemporaryUsersScenario, TemporaryTableServiceOpsTest> CreateTemporaryUsersScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateTemporaryUsersScenario,
            (repo, context) => new TemporaryTableServiceOpsTest(repo, context));

    private PreparedScenarioScope<UsersScenario, LastExecutionPlansHistoryServiceTest> CreateExecutionPlanHistoryScope(
        (int id, string name)[]? seedRows = null)
        => CreatePreparedScenarioScope(
            (repo, context) => BenchmarkScenarioFactory.CreateUsersScenario(repo, context, seedRows ?? [(1, "Alice")]),
            (repo, context) => new LastExecutionPlansHistoryServiceTest(repo, context));

    private PreparedScenarioScope<SelectTableScenario, SelectByPKServiceTest> GetPreparedSelectByPkState()
        => GetOrCreatePreparedState(
            "select-bypk",
            () => CreateSelectByPkScope());

    private PreparedScenarioScope<SelectTableScenario, QueryServiceTest> GetPreparedSelectTableQueryState(string key)
        => GetOrCreatePreparedState(
            key,
            () => CreateSelectTableQueryScope());

    private PreparedScenarioScope<UsersOrdersScenario, DmlMutationSelectJoinServiceTest> GetPreparedSelectJoinState()
        => GetOrCreatePreparedState(
            "select-join",
            () => CreateUsersOrdersMutationScope());

    private PreparedCreateSchemaState GetPreparedCreateSchemaState()
        => GetOrCreatePreparedState(
            "create-schema",
            () => new PreparedCreateSchemaState(CreateCreateTableScope()));

    private PreparedCreateTableWithFkState GetPreparedCreateTableWithFkState()
        => GetOrCreatePreparedState(
            "create-table-with-fk",
            () => new PreparedCreateTableWithFkState(CreateCreateTableWithFkScope()));

    private PreparedDropTableState GetPreparedDropTableState()
        => GetOrCreatePreparedState(
            "drop-table",
            () => new PreparedDropTableState(CreateDropTableScope()));

    private PreparedInsertUsersState GetPreparedInsertUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedInsertUsersState(CreateBenchmarkRunner()));

    private PreparedCheckConstraintsState GetPreparedCheckConstraintsState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedCheckConstraintsState(CreateBenchmarkRunner()));

    private PreparedCrudUsersState GetPreparedCrudUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedCrudUsersState(CreateBenchmarkRunner([[(1, "Alice"), (2, "Bob")]])));

    private PreparedMergeUsersState GetPreparedMergeUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedMergeUsersState(CreateBenchmarkRunner()));

    private PreparedTransactionUsersState GetPreparedTransactionUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedTransactionUsersState(CreateBenchmarkRunner()));

    private PreparedNoopMutationState GetPreparedNoopMutationState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var repo = new RepoService(CreateConnection, Dialect);
                repo.Cnn.Open();
                var context = new FidelityTestContext();
                var scenario = BenchmarkScenarioFactory.CreateNoopScenario(repo, context);
                scenario.CreateScenarioAsync().GetAwaiter().GetResult();
                var service = new DmlMutationServiceTest(repo, context);
                return new PreparedNoopMutationState(repo.Cnn, service);
            });

    private PreparedNoopQueryState GetPreparedNoopQueryState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var repo = new RepoService(CreateConnection, Dialect);
                repo.Cnn.Open();
                var context = new FidelityTestContext();
                var scenario = BenchmarkScenarioFactory.CreateNoopScenario(repo, context);
                scenario.CreateScenarioAsync().GetAwaiter().GetResult();
                var service = new QueryServiceTest(repo, context);
                return new PreparedNoopQueryState(repo.Cnn, service);
            });

    private PreparedParameterProjectionState GetPreparedParameterProjectionState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedParameterProjectionState(CreateBenchmarkRunner()));

    private PreparedParameterMatrixState GetPreparedParameterMatrixState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedParameterMatrixState(CreateBenchmarkRunner(), Dialect));

    private PreparedTypedFieldStorageMatrixState GetPreparedTypedFieldStorageMatrixState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedTypedFieldStorageMatrixState(CreateBenchmarkRunner(), Dialect));

    private PreparedParameterTransactionUsersState GetPreparedParameterTransactionUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedParameterTransactionUsersState(CreateBenchmarkRunner()));

    private PreparedParameterInsertUsersState GetPreparedParameterInsertUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedParameterInsertUsersState(CreateBenchmarkRunner()));

    protected int RunPreparedStoredProcedureCall(string key, int tenantId, string note)
        => GetPreparedStoredProcedureState(key).RunStoredProcedureCall(tenantId, note);

    private PreparedStoredProcedureState GetPreparedStoredProcedureState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedStoredProcedureState(CreateBenchmarkRunner()));

    private PreparedSequenceState GetPreparedSequenceState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedSequenceState(CreateBenchmarkRunner()));

    private PreparedSequenceUsersState GetPreparedSequenceUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedSequenceUsersState(CreateBenchmarkRunner()));

    private PreparedSequenceExpressionFilterState GetPreparedSequenceExpressionFilterState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedSequenceExpressionFilterState(CreateBenchmarkRunner([[(1, "Ana")]])));

    private PreparedReturningInsertState GetPreparedReturningInsertState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedReturningInsertState(CreateReturningInsertScope()));

    private PreparedBatchUsersState GetPreparedBatchUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedBatchUsersState(CreateBenchmarkRunner()));

    private PreparedTemporaryTableSourceState GetPreparedTemporaryTableSourceState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedTemporaryTableSourceState(
                CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateTemporaryTableScenario(repo, context),
                    (repo, context) => new TemporaryTableServiceOpsTest(repo, context))));

    private PreparedScenarioScope<TemporaryUsersScenario, TemporaryTableServiceOpsTest> GetPreparedTemporaryUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateTemporaryUsersScenario(repo, context),
                    (repo, context) => new TemporaryTableServiceOpsTest(repo, context)));

    private PreparedScenarioScope<NoopScenario, SchemaSnapshotServiceOpsTest> GetPreparedSchemaSnapshotState(string key)
        => GetOrCreatePreparedState(
            key,
            () => CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateNoopScenario(repo, context),
                    (repo, context) => new SchemaSnapshotServiceOpsTest(repo, context)));

    private PreparedScenarioScope<UsersScenario, QueryServiceTest> GetPreparedUsersQueryState(
        string key,
        params (int id, string name)[] seedRows)
        => GetOrCreatePreparedState(
            key,
            () => CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateUsersScenario(repo, context, seedRows),
                    (repo, context) => new QueryServiceTest(repo, context)));

    private PreparedUsersOrdersQueryState GetPreparedUsersOrdersQueryState(
        string key,
        (int id, string name)[] seedUsers,
        (int id, int userId, string note)[] seedOrders)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedUsersOrdersQueryState(
                CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateUsersOrdersScenario(repo, context, seedUsers, seedOrders),
                    (repo, context) => new QueryServiceTest(repo, context))));

    private PreparedUsersOrdersQueryState GetPreparedUsersOrdersMetricsQueryState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedUsersOrdersQueryState(
                CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateUsersOrdersScenarioWithMetrics(
                        repo,
                        context,
                        [(1, "Alice"), (2, "Bob"), (3, "Carla")],
                        [(10, 1, "A", 1.25m, 2, false), (11, 1, "B", 2.75m, 1, true), (12, 2, "C", 5.50m, 4, false)]),
                    (repo, context) => new QueryServiceTest(repo, context))));

    private PreparedExecutionPlanState GetPreparedExecutionPlanState(
        string key,
        params (int id, string name)[] seedRows)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedExecutionPlanState(
                CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateUsersScenario(repo, context, seedRows),
                    (repo, context) => new ExecutionPlanSelectServiceTest(repo, context))));

    private PreparedLastExecutionPlansHistoryState GetPreparedLastExecutionPlansHistoryState(
        string key,
        params (int id, string name)[] seedRows)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedLastExecutionPlansHistoryState(CreateExecutionPlanHistoryScope(seedRows)));

    private PreparedExecutionPlanJoinState GetPreparedExecutionPlanJoinState(
        string key,
        (int id, string name)[] seedUsers,
        (int id, int userId, string note)[] seedOrders)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedExecutionPlanJoinState(
                CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateUsersOrdersScenario(repo, context, seedUsers, seedOrders),
                    (repo, context) => new ExecutionPlanJoinServiceTest(repo, context))));

    private PreparedExecutionPlanDmlState GetPreparedExecutionPlanDmlState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedExecutionPlanDmlState(
                CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateInsertUsersScenario(repo, context),
                    (repo, context) => new ExecutionPlanDmlServiceTest(repo, context))));

    private PreparedDebugTraceSelectState GetPreparedDebugTraceSelectState(
        string key,
        params (int id, string name)[] seedRows)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedDebugTraceSelectState(
                CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateUsersScenario(repo, context, seedRows),
                    (repo, context) => new DebugTraceSelectServiceTest(repo, context))));

    private PreparedDebugTraceBatchState GetPreparedDebugTraceBatchState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedDebugTraceBatchState(
                CreatePreparedScenarioScope(
                    (repo, context) => BenchmarkScenarioFactory.CreateInsertUsersScenario(repo, context),
                    (repo, context) => new DebugTraceBatchServiceTest(repo, context))));

    /// <summary>
    /// EN: Returns a cached benchmark state for the supplied key or creates it on first use.
    /// PT-br: Retorna um estado de benchmark em cache para a chave informada ou o cria no primeiro uso.
    /// </summary>
    /// <typeparam name="TState">EN: The cached state type. PT-br: O tipo do estado em cache.</typeparam>
    /// <param name="key">EN: The cache key that identifies the prepared benchmark state. PT-br: A chave do cache que identifica o estado preparado do benchmark.</param>
    /// <param name="factory">EN: The factory used to prepare the benchmark state. PT-br: A fábrica usada para preparar o estado do benchmark.</param>
    /// <returns>EN: The cached or newly created benchmark state. PT-br: O estado de benchmark em cache ou recém-criado.</returns>
    protected TState GetOrCreatePreparedState<TState>(
        string key,
        Func<TState> factory)
        where TState : class, IDisposable
    => (TState)_preparedStates.GetOrAdd(key, _ => factory());


    /// <summary>
    /// EN: Releases all cached benchmark states prepared during the session.
    /// PT-br: Libera todos os estados de benchmark em cache preparados durante a sessao.
    /// </summary>
    protected virtual void DisposePreparedStates()
    {
        foreach (var state in _preparedStates.Values)
        {
            state.Dispose();
        }

        _preparedStates.Clear();
    }

}

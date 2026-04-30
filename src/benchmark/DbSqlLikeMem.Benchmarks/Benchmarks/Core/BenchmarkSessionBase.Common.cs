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

    protected sealed class PreparedScenarioScope<TScenario, TService>(
        RepoService repo,
        FidelityTestContext context,
        TScenario scenario,
        TService service) : IDisposable
        where TScenario : class, ITestScenario
    {
        public RepoService Repo { get; } = repo;

        public FidelityTestContext Context { get; } = context;

        public TScenario Scenario { get; } = scenario;

        public TService Service { get; } = service;

        public DbConnection Connection => Repo.Cnn;

        public void Dispose()
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                Scenario.DropScenarioAsync().GetAwaiter().GetResult();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
            finally
            {
                Repo.Dispose();
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }
    }

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

    protected PreparedScenarioScope<TemporaryTableScenario, TemporaryTableServiceOpsTest> CreateTemporaryTableScope()
        => CreatePreparedScenarioScope(
            BenchmarkScenarioFactory.CreateTemporaryTableScenario,
            (repo, context) => new TemporaryTableServiceOpsTest(repo, context));

    protected PreparedScenarioScope<TemporaryUsersScenario, TemporaryTableServiceOpsTest> CreateTemporaryUsersScope()
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

    private PreparedCrudUsersState GetPreparedCrudUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedCrudUsersState(CreateBenchmarkRunner([[(1, "Alice"), (2, "Bob")]])));

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

    protected int RunPreparedStoredProcedureCall(string key, int tenantId, string note)
        => GetPreparedStoredProcedureState(key).RunStoredProcedureCall(tenantId, note);

    private PreparedStoredProcedureState GetPreparedStoredProcedureState(string key)
        => GetOrCreatePreparedState(
            key,
            () => new PreparedStoredProcedureState(CreateBenchmarkRunner()));

    private PreparedSequenceState GetPreparedSequenceState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var repo = new RepoService(CreateConnection, Dialect);
                repo.Cnn.Open();
                var context = new FidelityTestContext();
                var scenario = BenchmarkScenarioFactory.CreateSequenceScenario(repo, context);
                scenario.CreateScenarioAsync().GetAwaiter().GetResult();
                var service = new DmlMutationSequenceServiceTest(repo, context);
                return new PreparedSequenceState(
                    new PreparedScenarioScope<SequenceScenario, DmlMutationSequenceServiceTest>(
                        repo,
                        context,
                        scenario,
                        service));
            });

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

    private sealed class PreparedCreateSchemaState(
        PreparedScenarioScope<CreateTableScenario, CreateTableServiceTest> scope) : IDisposable
    {
        public void RunCreateSchema()
        {
            try
            {
                var result = scope.Service.RunTestAsync().GetAwaiter().GetResult();
                GC.KeepAlive(result);
            }
            finally
            {
                try
                {
                    scope.Scenario.DropScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedCreateTableWithFkState(
        PreparedScenarioScope<CreateTableWithFKScenario, CreateTableWithFKServiceTest> scope) : IDisposable
    {
        public void RunCreateTableWithFk()
        {
            try
            {
                var result = scope.Service.RunTestAsync().GetAwaiter().GetResult();
                GC.KeepAlive(result);
            }
            finally
            {
                try
                {
                    scope.Scenario.DropScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    scope.Scenario.CreateScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore restore failures during benchmark teardown.
                }
            }
        }

        public int RunCreateTableWithFkInsert(int userId, int orderId)
        {
            try
            {
                var result = scope.Service.RunTestAsync(userId, orderId).GetAwaiter().GetResult();
                GC.KeepAlive(result);
                return Convert.ToInt32(result, CultureInfo.InvariantCulture);
            }
            finally
            {
                try
                {
                    scope.Scenario.DropScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    scope.Scenario.CreateScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore restore failures during benchmark teardown.
                }
            }
        }

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedDropTableState(
        PreparedScenarioScope<DropTableScenario, DropTableServiceTest> scope) : IDisposable
    {
        public void RunDropTable()
        {
            try
            {
                var result = scope.Service.RunTestAsync().GetAwaiter().GetResult();
                GC.KeepAlive(result);
            }
            finally
            {
                try
                {
                    scope.Scenario.CreateScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore restore failures during benchmark teardown.
                }
            }
        }

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedInsertUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunSequentialInsert(int rowCount)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertUsersServiceTest>(rowCount, 1, rowCount).GetAwaiter().GetResult();
            return ((List<List<object[]>>)result!)[0].Count;
        }

        public int RunParallelInsert(int rowCount)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertParallelUsersServiceTest>(rowCount, 1, rowCount).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunRowCountAfterInsert()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertRowCountUsersServiceTest>(1).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunParameterInsertSingle()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertParameterUsersServiceTest>(1, "User 1").GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public (string firstName, string lastName) RunInsertCustomStartId()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertCustomStartUsersServiceTest>(10).GetAwaiter().GetResult();
            return ((string firstName, string lastName))result!;
        }

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedCrudUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public string RunUpdateByPk(int userId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationUpdateByPkServiceTest>(userId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public int RunDeleteByPk(int userId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationDeleteByPkServiceTest>(userId).GetAwaiter().GetResult();
            return ((List<List<object[]>>)result!).Count;
        }

        public int RunRowCountAfterUpdate()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationRowCountAfterUpdateServiceTest>(1).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunUpdateDeleteRoundTrip(int updateUserId, int deleteUserId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationUpdateDeleteRoundTripServiceTest>(updateUserId, deleteUserId).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunTransactionalUpdateDeleteCommit(int updateUserId, int deleteUserId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationTransactionalUpdateDeleteCommitServiceTest>(updateUserId, deleteUserId).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public string RunUpsert(int userId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationUpsertServiceTest>(userId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedParameterTransactionUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunParameterTransactionCommit()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, DmlMutationParameterTransactionCommitServiceTest>(
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified),
                new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified)).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunParameterTransactionRollback()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, DmlMutationParameterTransactionRollbackServiceTest>(
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified),
                new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified)).GetAwaiter().GetResult();
            var count2 = (int)(result!.GetType().GetProperty("count2")?.GetValue(result)
                ?? throw new InvalidOperationException("Parameter transaction rollback did not return a count2 value."));
            return count2;
        }

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedTransactionUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunTransactionCommit()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunTransactionCommit()),
                Array.Empty<object>()).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunTransactionRollback()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunTransactionRollback()),
                Array.Empty<object>()).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunRollbackToSavepoint()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunRollbackToSavepoint()),
                Array.Empty<object>()).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunNestedSavepointFlow()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunNestedSavepointFlow()),
                Array.Empty<object>()).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedNoopMutationState(
        DbConnection connection,
        DmlMutationServiceTest service) : IDisposable
    {
        public DmlMutationServiceTest Service => service;

        public void Dispose()
            => connection.Dispose();
    }

    private sealed class PreparedNoopQueryState(
        DbConnection connection,
        QueryServiceTest service) : IDisposable
    {
        public QueryServiceTest Service => service;

        public void Dispose()
            => connection.Dispose();
    }

    private sealed class PreparedTypedFieldStorageMatrixState(
        NotFidelityTestService<DbConnection> runner,
        ProviderSqlDialect dialect) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;

        public QueryResultSnapshot RunTypedFieldStorageMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTypedFieldStorageMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunTypedFieldFunctionMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTypedFieldFunctionMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunTypedFieldCalculationMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTypedFieldCalculationMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunTemporalFieldMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTemporalFieldMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunTemporalComparisonMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTemporalComparisonMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunTemporalArithmeticMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTemporalArithmeticMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunJsonTypedFieldMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunJsonTypedFieldMatrixAsync()).GetAwaiter().GetResult()!;

        public int RunParameterRoundTripMatrix()
        {
            var createdAt = _dialect.Provider == ProviderId.Npgsql
                ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
                : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
            var result = runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, args) => (object?)await service.RunParameterRoundTripMatrixAsync(args),
                1,
                "Param Alice",
                DBNull.Value,
                true,
                (short)31,
                12.34m,
                createdAt,
                DBNull.Value,
                DBNull.Value).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunTypedFieldAndFunctionBlend()
            => Convert.ToInt32(runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTypedFieldAndFunctionBlendAsync()).GetAwaiter().GetResult(), CultureInfo.InvariantCulture);

        public int RunTypedFieldCompoundPredicateMatrix()
            => Convert.ToInt32(runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTypedFieldCompoundPredicateMatrixAsync()).GetAwaiter().GetResult(), CultureInfo.InvariantCulture);

        public QueryResultSnapshot RunCastCalculationMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunCastCalculationMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunNullComparisonMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunNullComparisonMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunTextLengthMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTextLengthMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunTextCaseMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTextCaseMatrixAsync()).GetAwaiter().GetResult()!;

        public QueryResultSnapshot RunTypedFieldPredicateMatrix()
            => (QueryResultSnapshot)runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, _) => (object?)await service.RunTypedFieldPredicateMatrixAsync()).GetAwaiter().GetResult()!;

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedParameterProjectionState(
        NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public object? RunParameterProjection()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunParameterProjection())).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture);
        }

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedParameterMatrixState(
        NotFidelityTestService<DbConnection> runner,
        ProviderSqlDialect dialect) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;

        public int RunParameterTypeMatrix()
        {
            var createdAt = _dialect.Provider == ProviderId.Npgsql
                ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
                : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
            var result = runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, args) => (object?)await service.RunParameterTypeMatrixAsync(args),
                "Typed param",
                "Ansi param",
                "Fixed ANSI",
                "Fixed Text",
                (short)12,
                34,
                56L,
                true,
                78.90m,
                12.5d,
                TimeSpan.FromHours(1.5),
                new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero),
                createdAt,
                Guid.Parse("11111111-2222-3333-4444-555555555555"),
                new byte[] { 1, 2, 3, 4 }).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunParameterDateCurrencyMatrix()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (service, args) => (object?)await service.RunParameterDateCurrencyMatrixAsync(args),
                new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Unspecified),
                123.45m).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedStoredProcedureState(
        NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunStoredProcedureCall(int tenantId, string note)
        {
            var result = runner.RunTestAsync<NoopScenario, StoredProcedureBenchmarkServiceTest>(tenantId, note).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedReturningInsertState(
        PreparedScenarioScope<InsertUsersScenario, BatchInsertReturningServiceTest> scope) : IDisposable
    {
        public BatchInsertReturningServiceTest Service => scope.Service;

        public object? RunReturningInsert()
        {
            try
            {
                var value = scope.Service.RunTestAsync().GetAwaiter().GetResult();
                GC.KeepAlive(value);
                return value;
            }
            finally
            {
                try
                {
                    scope.Scenario.DropScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    scope.Scenario.CreateScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore restore failures during benchmark teardown.
                }
            }
        }

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedSequenceState(
        PreparedScenarioScope<SequenceScenario, DmlMutationSequenceServiceTest> scope) : IDisposable
    {
        public object? RunSequenceNextValue()
        {
            var value = scope.Service.RunTestAsync().GetAwaiter().GetResult();
            GC.KeepAlive(value);
            return value;
        }

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedBatchUsersState(
        NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public string RunBatchMixedReadWrite(int firstUserId, int secondUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchMixedReadWriteServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public string RunBatchScalar(int firstUserId, int secondUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchScalarServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public int RunBatchNonQuery(int firstUserId, int secondUserId, int updateUserId, int deleteUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchNonQueryServiceTest>(firstUserId, secondUserId, updateUserId, deleteUserId).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public object? RunBatchReaderMultiResult(int firstUserId, int secondUserId)
            => runner.RunTestAsync<InsertUsersScenario, BatchReaderMultiResultServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();

        public int RunBatchInsert(int rowCount)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchInsertServiceTest>(rowCount).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunRowCountInBatch(int firstUserId, int secondUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchRowCountInServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public string RunBatchTransactionControl(int firstUserId, int secondUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchTransactionControlServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public void Dispose()
            => runner.Dispose();
    }

    private sealed class PreparedTemporaryTableSourceState(
        PreparedScenarioScope<TemporaryTableScenario, TemporaryTableServiceOpsTest> scope) : IDisposable
    {
        public TemporaryTableServiceOpsTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedUsersOrdersQueryState(
        PreparedScenarioScope<UsersOrdersScenario, QueryServiceTest> scope) : IDisposable
    {
        public QueryServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedExecutionPlanState(
        PreparedScenarioScope<UsersScenario, ExecutionPlanSelectServiceTest> scope) : IDisposable
    {
        public ExecutionPlanSelectServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedLastExecutionPlansHistoryState(
        PreparedScenarioScope<UsersScenario, LastExecutionPlansHistoryServiceTest> scope) : IDisposable
    {
        public LastExecutionPlansHistoryServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedExecutionPlanJoinState(
        PreparedScenarioScope<UsersOrdersScenario, ExecutionPlanJoinServiceTest> scope) : IDisposable
    {
        public ExecutionPlanJoinServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedExecutionPlanDmlState(
        PreparedScenarioScope<InsertUsersScenario, ExecutionPlanDmlServiceTest> scope) : IDisposable
    {
        private int _nextInsertId = 1;

        public object? RunExecutionPlanDml()
        {
            var value = scope.Service.RunTestAsync(_nextInsertId++).GetAwaiter().GetResult();
            return value;
        }

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedDebugTraceSelectState(
        PreparedScenarioScope<UsersScenario, DebugTraceSelectServiceTest> scope) : IDisposable
    {
        public DebugTraceSelectServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

    private sealed class PreparedDebugTraceBatchState(
        PreparedScenarioScope<InsertUsersScenario, DebugTraceBatchServiceTest> scope) : IDisposable
    {
        public DebugTraceBatchServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

}

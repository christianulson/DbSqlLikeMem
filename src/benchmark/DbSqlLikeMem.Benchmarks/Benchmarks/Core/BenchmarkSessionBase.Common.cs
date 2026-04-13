using DbSqlLikeMem.TestTools.Benchmarks;
using DbSqlLikeMem.TestTools.DDL;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Schema;
using DbSqlLikeMem.TestTools.TemporaryTable;
using DbSqlLikeMem.TestTools.Query;
using System.Globalization;
using System.Data;
using System.Text;

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    private readonly object _preparedStateSync = new();
    private readonly Dictionary<string, IDisposable> _preparedStates = [];
    private readonly string _scopeToken = Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();

    /// <summary>
    /// EN: Generates a unique temporary table name for the users table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de usuários usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary users table name. PT-br: Um nome único de tabela temporária de usuários.</returns>
    protected virtual string NewUsersTableName() => $"USR_{_scopeToken}_{NextToken()}";

    /// <summary>
    /// EN: Generates a unique temporary users table name for rollback and isolation benchmark flows.
    /// PT-br: Gera um nome único de tabela temporaria de usuarios para fluxos de rollback e isolamento de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary users table name. PT-br: Um nome único de tabela temporaria de usuarios.</returns>
    protected virtual string NewTemporaryUsersTableName() => $"temp_users_{NextToken()}";

    /// <summary>
    /// EN: Generates a unique temporary table name for the orders table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de pedidos usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary orders table name. PT-br: Um nome único de tabela temporária de pedidos.</returns>
    protected virtual string NewOrdersTableName() => $"ORD_{_scopeToken}_{NextToken()}";

    /// <summary>
    /// EN: Generates a unique temporary sequence name for sequence-based benchmark operations.
    /// PT-br: Gera um nome único de sequência temporária para operações de benchmark baseadas em sequência.
    /// </summary>
    /// <returns>EN: A unique temporary sequence name. PT-br: Um nome único de sequência temporária.</returns>
    protected virtual string NewSequenceName() => $"SEQ_{_scopeToken}_{NextToken()}";

    protected virtual string NewSavepointName() => $"SP_{_scopeToken}_{NextToken()}";

    protected static string NextToken() => Interlocked.Increment(ref _objectCounter).ToString("x8", CultureInfo.InvariantCulture).ToUpperInvariant();

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

    private InsertUsersServiceTest<DbConnection> CreateInsertUsersService(
        DbConnection connection,
        Func<DbConnection>? connectionFactory = null)
        => new(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect), Dialect, connectionFactory);

    private DmlMutationServiceTest<DbConnection> CreateMutationService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private BatchServiceTest<DbConnection> CreateBatchService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private QueryServiceTest<DbConnection> CreateQueryService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private ExecutionPlanServiceTest<DbConnection> CreateExecutionPlanService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private DebugTraceServiceTest<DbConnection> CreateDebugTraceService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private ConnectionLifecycleServiceTest<DbConnection> CreateConnectionLifecycleService(
        DbConnection connection)
        => new(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>(), Dialect);

    protected TemporaryTableServiceTest<DbConnection> CreateTemporaryTableService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario,
        Func<DbConnection>? connectionFactory = null)
        => new(connection, scenario, Dialect, connectionFactory);

    private SchemaSnapshotServiceTest<DbConnection> CreateSchemaSnapshotService(
        DbConnection connection)
        => new(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>(), Dialect);

    private PreparedSelectByPkState GetPreparedSelectByPkState()
        => GetOrCreatePreparedState(
            "select-bypk",
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = new SelectByPKServiceTest<DbConnection>(
                    connection,
                    BenchmarkScenarioFactory.CreateSelectTableScenario<DbConnection>(Dialect),
                    Dialect);
                service.CreateScenario(users, uId);
                return new PreparedSelectByPkState(connection, service, users, uId);
            });

    private PreparedSelectJoinState GetPreparedSelectJoinState()
        => GetOrCreatePreparedState(
            "select-join",
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var orders = NewOrdersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateMutationService(
                    connection,
                    BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, orders, uId);
                return new PreparedSelectJoinState(connection, service, users, orders, uId);
            });

    private PreparedCreateSchemaState GetPreparedCreateSchemaState()
        => GetOrCreatePreparedState(
            "create-schema",
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = new CreateTableServiceTest<DbConnection>(
                    connection,
                    BenchmarkScenarioFactory.CreateTableScenario<DbConnection>(),
                    Dialect);
                return new PreparedCreateSchemaState(this, connection, service, users, uId);
            });

    private PreparedCreateTableWithFkState GetPreparedCreateTableWithFkState()
        => GetOrCreatePreparedState(
            "create-table-with-fk",
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var orders = NewOrdersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = new CreateTableWithFKServiceTest<DbConnection>(
                    connection,
                    BenchmarkScenarioFactory.CreateTableWithFKScenario<DbConnection>(),
                    Dialect);
                return new PreparedCreateTableWithFkState(this, connection, service, users, orders, uId);
            });

    private PreparedDropTableState GetPreparedDropTableState()
        => GetOrCreatePreparedState(
            "drop-table",
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = new DropTableServiceTest<DbConnection>(
                    connection,
                    BenchmarkScenarioFactory.CreateDropTableScenario<DbConnection>(),
                    Dialect);
                return new PreparedDropTableState(this, connection, service, users, uId);
            });

    private PreparedInsertUsersState GetPreparedInsertUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateInsertUsersService(connection, CreateConnection);
                service.CreateScenario(users, uId);
                return new PreparedInsertUsersState(connection, service, users, uId);
            });

    private PreparedCrudUsersState GetPreparedCrudUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice"), (2, "Bob")));
                service.CreateScenario(users, uId);
                return new PreparedCrudUsersState(connection, service, Dialect, users, uId);
            });

    private PreparedTransactionUsersState GetPreparedTransactionUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedTransactionUsersState(connection, service, Dialect, users, uId);
            });

    private PreparedNoopMutationState GetPreparedNoopMutationState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var connection = CreateConnection();
                connection.Open();
                var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
                service.CreateScenario();
                return new PreparedNoopMutationState(connection, service);
            });

    private PreparedNoopQueryState GetPreparedNoopQueryState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var connection = CreateConnection();
                connection.Open();
                var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
                return new PreparedNoopQueryState(connection, service);
            });

    private PreparedParameterProjectionState GetPreparedParameterProjectionState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var connection = CreateConnection();
                connection.Open();
                return new PreparedParameterProjectionState(connection, Dialect);
            });

    private PreparedParameterMatrixState GetPreparedParameterMatrixState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateQueryService(
                    connection,
                    BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedParameterMatrixState(connection, service, users, uId);
            });

    private PreparedTypedFieldStorageMatrixState GetPreparedTypedFieldStorageMatrixState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateQueryService(
                    connection,
                    BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedTypedFieldStorageMatrixState(connection, service, users, uId);
            });

    private PreparedParameterTransactionUsersState GetPreparedParameterTransactionUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedParameterTransactionUsersState(connection, service, Dialect, users, uId);
            });

    protected int RunPreparedStoredProcedureCall(string key)
        => GetPreparedStoredProcedureState(key).RunStoredProcedureCall();

    private PreparedStoredProcedureState GetPreparedStoredProcedureState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var connection = CreateConnection();
                connection.Open();

                if (connection is not DbConnectionMockBase mockConnection)
                {
                    throw new NotSupportedException($"{Dialect.DisplayName} does not support stored procedure benchmarks.");
                }

                var procedure = new ProcedureDef(
                    "sp_benchmark",
                    RequiredIn:
                    [
                        new ProcParam("tenantId", DbType.Int32, Required: true)
                    ],
                    OptionalIn:
                    [
                        new ProcParam("note", DbType.String, Required: false)
                    ],
                    OutParams:
                    [
                        new ProcParam("counter", DbType.Int32, Required: true),
                        new ProcParam("message", DbType.String, Required: true)
                    ],
                    ReturnParam: new ProcParam("resultCode", DbType.Int32, Value: 0));

                mockConnection.AddProdecure(procedure);
                return new PreparedStoredProcedureState(connection, procedure.Name);
            });

    private PreparedSequenceState GetPreparedSequenceState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var sequence = NewSequenceName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateSequenceScenario<DbConnection>(Dialect));
                service.CreateScenario(sequence);
                return new PreparedSequenceState(connection, service, sequence);
            });

    private PreparedReturningInsertState GetPreparedReturningInsertState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedReturningInsertState(connection, service, Dialect, users, uId);
            });

    private PreparedBatchUsersState GetPreparedBatchUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedBatchUsersState(connection, service, Dialect, users, uId);
            });

    private PreparedTemporaryTableSourceState GetPreparedTemporaryTableSourceState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateTemporaryTableService(connection, BenchmarkScenarioFactory.CreateTemporaryTableScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedTemporaryTableSourceState(connection, service, users, uId);
            });

    private PreparedTemporaryUsersState GetPreparedTemporaryUsersState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var users = NewTemporaryUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateTemporaryTableService(
                    connection,
                    BenchmarkScenarioFactory.CreateTemporaryUsersScenario<DbConnection>(Dialect),
                    CreateConnection);
                service.CreateScenario(users);
                return new PreparedTemporaryUsersState(connection, service, Dialect, users);
            });

    private PreparedSchemaSnapshotState GetPreparedSchemaSnapshotState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var connection = CreateConnection();
                connection.Open();
                var service = CreateSchemaSnapshotService(connection);
                return new PreparedSchemaSnapshotState(connection, service);
            });

    private PreparedUsersQueryState GetPreparedUsersQueryState(
        string key,
        params (int id, string name)[] seedRows)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateQueryService(
                    connection,
                    BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, seedRows));
                service.CreateScenario(users, uId);
                return new PreparedUsersQueryState(connection, service, users, uId);
            });

    private PreparedUsersOrdersQueryState GetPreparedUsersOrdersQueryState(
        string key,
        (int id, string name)[] seedUsers,
        (int id, int userId, string note)[] seedOrders)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var orders = NewOrdersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateQueryService(
                    connection,
                    BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                        Dialect,
                        seedUsers,
                        seedOrders));
                service.CreateScenario(users, orders, uId);
                return new PreparedUsersOrdersQueryState(connection, service, users, orders, uId);
            });

    private PreparedExecutionPlanState GetPreparedExecutionPlanState(
        string key,
        params (int id, string name)[] seedRows)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateExecutionPlanService(
                    connection,
                    BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, seedRows));
                service.CreateScenario(users, uId);
                return new PreparedExecutionPlanState(connection, service, users, uId);
            });

    private PreparedExecutionPlanJoinState GetPreparedExecutionPlanJoinState(
        string key,
        (int id, string name)[] seedUsers,
        (int id, int userId, string note)[] seedOrders)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var orders = NewOrdersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateExecutionPlanService(
                    connection,
                    BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                        Dialect,
                        seedUsers,
                        seedOrders));
                service.CreateScenario(users, orders, uId);
                return new PreparedExecutionPlanJoinState(connection, service, users, orders, uId);
            });

    private PreparedExecutionPlanDmlState GetPreparedExecutionPlanDmlState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateExecutionPlanService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedExecutionPlanDmlState(connection, service, users, uId);
            });

    private PreparedDebugTraceSelectState GetPreparedDebugTraceSelectState(
        string key,
        params (int id, string name)[] seedRows)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateDebugTraceService(
                    connection,
                    BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, seedRows));
                service.CreateScenario(users, uId);
                return new PreparedDebugTraceSelectState(connection, service, users, uId);
            });

    private PreparedDebugTraceBatchState GetPreparedDebugTraceBatchState(string key)
        => GetOrCreatePreparedState(
            key,
            () =>
            {
                var uId = NextToken();
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateDebugTraceService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
                service.CreateScenario(users, uId);
                return new PreparedDebugTraceBatchState(connection, service, users, uId);
            });

    protected static int ExecuteNonQuery(
        DbConnection connection,
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        return command.ExecuteNonQuery();
    }

    protected static int ExecuteNonQuery(
        DbConnection connection,
        string sql,
        Action<DbCommand> configureCommand,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        configureCommand(command);
        return command.ExecuteNonQuery();
    }

    protected static object? ExecuteScalar(
        DbConnection connection,
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        return command.ExecuteScalar();
    }

    protected static object? ExecuteScalar(
        DbConnection connection,
        string sql,
        Action<DbCommand> configureCommand,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        configureCommand(command);
        return command.ExecuteScalar();
    }

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
    {
        lock (_preparedStateSync)
        {
            if (_preparedStates.TryGetValue(key, out var existing))
            {
                return (TState)existing;
            }

            var created = factory();
            _preparedStates[key] = created;
            return created;
        }
    }

    /// <summary>
    /// EN: Releases all cached benchmark states prepared during the session.
    /// PT-br: Libera todos os estados de benchmark em cache preparados durante a sessao.
    /// </summary>
    protected virtual void DisposePreparedStates()
    {
        lock (_preparedStateSync)
        {
            foreach (var state in _preparedStates.Values)
            {
                state.Dispose();
            }

            _preparedStates.Clear();
        }
    }

    private sealed class PreparedSelectByPkState(
        DbConnection connection,
        SelectByPKServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public SelectByPKServiceTest<DbConnection> Service => service;

        public string Users => users;

        public string UId => uId;

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    var fallbackSql = service.Dialect.DropTable(users, uId);
                    ExecuteNonQuery(connection, fallbackSql);
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedSelectJoinState(
        DbConnection connection,
        DmlMutationServiceTest<DbConnection> service,
        string users,
        string orders,
        string uId) : IDisposable
    {
        public DmlMutationServiceTest<DbConnection> Service => service;

        public string Users => users;

        public string Orders => orders;

        public string UId => uId;

        public string UsersTable => $"{users}_{uId}";

        public string OrdersTable => $"{orders}_{uId}";

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(orders, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedCreateSchemaState(
        BenchmarkSessionBase owner,
        DbConnection connection,
        CreateTableServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public void RunCreateSchema()
        {
            try
            {
                service.CreateScenario(users, uId);
                service.RunTest(users, uId);
            }
            finally
            {
                try
                {
                    service.DropScenario(users, uId);
                }
                catch
                {
                    owner.SafeDropTable(connection, users, uId);
                }
            }
        }

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    owner.SafeDropTable(connection, users, uId);
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedCreateTableWithFkState(
        BenchmarkSessionBase owner,
        DbConnection connection,
        CreateTableWithFKServiceTest<DbConnection> service,
        string users,
        string orders,
        string uId) : IDisposable
    {
        public void RunCreateTableWithFk()
        {
            try
            {
                service.CreateScenario(users, uId);
                service.RunTest(users, orders, uId);
            }
            finally
            {
                try
                {
                    service.DropScenario(users, orders, uId);
                }
                catch
                {
                    try
                    {
                        owner.SafeDropTable(connection, orders, uId);
                    }
                    catch
                    {
                        // Ignore cleanup failures during benchmark teardown.
                    }

                    try
                    {
                        owner.SafeDropTable(connection, users, uId);
                    }
                    catch
                    {
                        // Ignore cleanup failures during benchmark teardown.
                    }
                }
            }
        }

        public int RunCreateTableWithFkInsert()
        {
            var orderedAt = service.Dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

            try
            {
                service.CreateScenario(users, uId);
                service.RunTest(users, orders, uId);

                var usersTable = $"{users}_{uId}";
                var ordersTable = $"{orders}_{uId}";

                ExecuteNonQuery(connection, service.Dialect.InsertUser(usersTable, 1, "Ana"));
                ExecuteNonQuery(connection, service.Dialect.InsertOrder(ordersTable, usersTable, 10, 1, "first", "o-10", 12.34m, 2, true, orderedAt));

                var count = Convert.ToInt32(
                    ExecuteScalar(connection, service.Dialect.CountJoinForUser(usersTable, ordersTable, 1)),
                    CultureInfo.InvariantCulture);
                if (count != 1)
                {
                    throw new InvalidOperationException($"Unexpected foreign-key insert benchmark join count for {service.Dialect.DisplayName}: {count}.");
                }

                return count;
            }
            finally
            {
                try
                {
                    service.DropScenario(users, orders, uId);
                }
                catch
                {
                    try
                    {
                        owner.SafeDropTable(connection, orders, uId);
                    }
                    catch
                    {
                        // Ignore cleanup failures during benchmark teardown.
                    }

                    try
                    {
                        owner.SafeDropTable(connection, users, uId);
                    }
                    catch
                    {
                        // Ignore cleanup failures during benchmark teardown.
                    }
                }
            }
        }

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                try
                {
                    owner.SafeDropTable(connection, orders, uId);
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    owner.SafeDropTable(connection, users, uId);
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedDropTableState(
        BenchmarkSessionBase owner,
        DbConnection connection,
        DropTableServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public void RunDropTable()
        {
            try
            {
                service.CreateScenario(users, "Orders", uId);
                service.RunTest(users, uId);
            }
            finally
            {
                try
                {
                    owner.SafeDropTable(connection, users, uId);
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public void Dispose()
        {
            try
            {
                owner.SafeDropTable(connection, users, uId);
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedInsertUsersState(
        DbConnection connection,
        InsertUsersServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        private int _nextInsertId = 1;
        private int _rowCount;
        private DbCommand? _singleInsertCommand;
        private DbParameter? _singleInsertIdParameter;
        private DbParameter? _singleInsertNameParameter;
        private DbCommand? _countCommand;

        public int RunSequentialInsert(int rowCount)
        {
            var startId = _nextInsertId;
            var expectedCount = _rowCount + rowCount;

            for (var i = 0; i < rowCount; i++)
            {
                var id = startId + i;
                ExecutePreparedInsertRow(id, rowCount == 1 ? "Alice" : $"User-{id}");
            }

            var count = Convert.ToInt32(GetOrCreateCountCommand().ExecuteScalar(), CultureInfo.InvariantCulture);
            if (count != expectedCount)
            {
                throw new InvalidOperationException($"Expected {expectedCount} rows for {service.Dialect.DisplayName}, got {count}.");
            }

            _nextInsertId += rowCount;
            _rowCount = expectedCount;
            return count;
        }

        public int RunParallelInsert(int rowCount)
        {
            var startId = _nextInsertId;
            var expectedCount = _rowCount + rowCount;
            var count = service.RunParallelTest(users, uId, rowCount, startId, expectedCount);
            _nextInsertId += rowCount;
            _rowCount = expectedCount;
            return count;
        }

        public int RunRowCountAfterInsert()
        {
            var id = _nextInsertId;
            var affected = ExecutePreparedInsertRow(id, "Alice");
            _nextInsertId += 1;
            _rowCount += 1;
            return affected;
        }

        public int RunParameterInsertSingle()
        {
            var id = _nextInsertId;
            var affected = ExecutePreparedInsertRow(id, $"User {id}");

            _nextInsertId += 1;
            _rowCount += 1;
            return affected;
        }

        public (string firstName, string lastName) RunInsertCustomStartId()
        {
            try
            {
                ExecutePreparedInsertRow(10, "User-10");
                ExecutePreparedInsertRow(11, "User-11");
                ExecutePreparedInsertRow(12, "User-12");
                var firstName = Convert.ToString(ExecuteScalar(connection, service.Dialect.SelectUserNameById(UsersTable, 10)), CultureInfo.InvariantCulture) ?? string.Empty;
                var lastName = Convert.ToString(ExecuteScalar(connection, service.Dialect.SelectUserNameById(UsersTable, 12)), CultureInfo.InvariantCulture) ?? string.Empty;
                if (!string.Equals(firstName, "User-10", StringComparison.Ordinal) || !string.Equals(lastName, "User-12", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Unexpected custom-start insert benchmark result for {service.Dialect.DisplayName}: {firstName}, {lastName}.");
                }

                return (firstName, lastName);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DeleteUserById(UsersTable, 12));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DeleteUserById(UsersTable, 11));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DeleteUserById(UsersTable, 10));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        private string UsersTable => $"{users}_{uId}";

        private int ExecutePreparedInsertRow(int id, string name)
        {
            var command = GetOrCreateSingleInsertCommand();
            var idParameter = _singleInsertIdParameter ?? throw new InvalidOperationException("Single insert id parameter was not initialized.");
            var nameParameter = _singleInsertNameParameter ?? throw new InvalidOperationException("Single insert name parameter was not initialized.");

            idParameter.Value = id;
            nameParameter.Value = name;
            return command.ExecuteNonQuery();
        }

        private DbCommand GetOrCreateSingleInsertCommand()
        {
            if (_singleInsertCommand is not null)
                return _singleInsertCommand;

            var command = connection.CreateCommand();
            command.CommandText = $"""
INSERT INTO {UsersTable} (
    Id,
    Name
)
VALUES (
    {service.Dialect.Parameter("id")},
    {service.Dialect.Parameter("name")}
)
""";

            _singleInsertIdParameter = CreateParameter(command, "id", DbType.Int32);
            _singleInsertNameParameter = CreateParameter(command, "name", DbType.String);
            _singleInsertCommand = command;
            return command;
        }

        private DbCommand GetOrCreateCountCommand()
        {
            if (_countCommand is not null)
                return _countCommand;

            var command = connection.CreateCommand();
            command.CommandText = service.Dialect.CountRows(UsersTable);
            _countCommand = command;
            return command;
        }

        private static void AddParameter(DbCommand command, string name, DbType dbType, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;

            var parameterTypeName = parameter.GetType().FullName;
            var isOracleParameter = parameterTypeName == "Oracle.ManagedDataAccess.Client.OracleParameter";
            var isFirebirdParameter = parameterTypeName == "FirebirdSql.Data.FirebirdClient.FbParameter";
            var isDb2Parameter = parameterTypeName is "IBM.Data.Db2.DB2Parameter"
                or "IBM.Data.DB2.Core.DB2Parameter"
                or "IBM.Data.DB2.iSeries.iDB2Parameter";

            if (isOracleParameter)
            {
            }
            else if (isFirebirdParameter && (dbType == DbType.Currency || dbType == DbType.DateTimeOffset || dbType == DbType.Guid))
            {
                // Firebird rejects these DbType assignments in this benchmark path.
                // Keep the payload-based flow and let the provider infer the storage type.
            }
            else if (isDb2Parameter && (dbType == DbType.Guid || dbType == DbType.DateTimeOffset || dbType == DbType.Time || dbType == DbType.DateTime))
            {
                parameter.DbType = DbType.String;
            }
            else
            {
                parameter.DbType = dbType;
            }

            parameter.Value = isDb2Parameter
                ? NormalizeDb2ParameterValue(dbType, value)
                : isFirebirdParameter
                    ? NormalizeFirebirdParameterValue(dbType, value)
                : value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        private static DbParameter CreateParameter(DbCommand command, string name, DbType dbType)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = dbType;
            parameter.Value = DBNull.Value;
            command.Parameters.Add(parameter);
            return parameter;
        }

        private static object NormalizeDb2ParameterValue(DbType dbType, object? value)
        {
            if (value is null)
                return DBNull.Value;

            return (dbType, value) switch
            {
                (DbType.Guid, Guid guid) => guid.ToString("D", CultureInfo.InvariantCulture),
                (DbType.Time, TimeSpan timeSpan) => timeSpan.ToString("c", CultureInfo.InvariantCulture),
                (DbType.DateTime, DateTime dateTime) => dateTime.ToString("O", CultureInfo.InvariantCulture),
                (DbType.DateTimeOffset, DateTimeOffset dateTimeOffset) => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
                _ => value
            };
        }

        private static object NormalizeFirebirdParameterValue(DbType dbType, object? value)
        {
            if (value is null)
                return DBNull.Value;

            return (dbType, value) switch
            {
                (DbType.Guid, Guid guid) => guid.ToString("D", CultureInfo.InvariantCulture),
                (DbType.DateTimeOffset, DateTimeOffset dateTimeOffset) => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
                _ => value
            };
        }

        public void Dispose()
        {
            try
            {
                _singleInsertCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _countCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedCrudUsersState(
        DbConnection connection,
        DmlMutationServiceTest<DbConnection> service,
        ProviderSqlDialect dialect,
        string users,
        string uId) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;
        private DbCommand? _updateByIdCommand;
        private DbParameter? _updateByIdNameParameter;
        private DbParameter? _updateByIdIdParameter;
        private DbCommand? _deleteByIdCommand;
        private DbParameter? _deleteByIdIdParameter;
        private DbCommand? _selectByIdCommand;
        private DbParameter? _selectByIdIdParameter;
        private DbCommand? _countCommand;
        private DbCommand? _upsertCommand;
        private DbCommand? _insertUser1Command;
        private DbCommand? _insertUser2Command;

        private string UsersTable => $"{users}_{uId}";

        public string RunUpdateByPk()
        {
            try
            {
                ExecutePreparedUpdateById("Alice-v2");
                var value = Convert.ToString(ExecutePreparedSelectNameById(1), CultureInfo.InvariantCulture);
                if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Unexpected update result for {_dialect.DisplayName}: {value ?? "<null>"}.");
                }

                return value!;
            }
            finally
            {
                try
                {
                    ExecutePreparedUpdateById("Alice");
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public int RunDeleteByPk()
        {
            try
            {
                ExecutePreparedDeleteById(1);
                var count = Convert.ToInt32(ExecutePreparedCountRows(), CultureInfo.InvariantCulture);
                if (count != 1)
                {
                    throw new InvalidOperationException($"Unexpected delete count for {_dialect.DisplayName}: {count}.");
                }

                return count;
            }
            finally
            {
                try
                {
                    ExecutePreparedInsertUser1();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public int RunRowCountAfterUpdate()
        {
            try
            {
                return ExecutePreparedUpdateById("Alice-v2");
            }
            finally
            {
                try
                {
                    ExecutePreparedUpdateById("Alice");
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public int RunUpdateDeleteRoundTrip()
        {
            try
            {
                ExecutePreparedUpdateById("Alice-v2");
                ExecutePreparedDeleteById(2);

                var remaining = Convert.ToString(ExecutePreparedSelectNameById(1), CultureInfo.InvariantCulture);
                var count = Convert.ToInt32(ExecutePreparedCountRows(), CultureInfo.InvariantCulture);
                if (count != 1)
                {
                    throw new InvalidOperationException($"Unexpected update/delete count for {_dialect.DisplayName}: {count}.");
                }

                GC.KeepAlive(remaining);
                return count;
            }
            finally
            {
                try
                {
                    ExecutePreparedUpdateById("Alice");
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecutePreparedInsertUser2();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public int RunTransactionalUpdateDeleteCommit()
        {
            try
            {
                using var transaction = connection.BeginTransaction();
                ExecutePreparedUpdateById("Alice-v2", transaction);
                ExecutePreparedDeleteById(2, transaction);
                transaction.Commit();

                var value = Convert.ToString(ExecutePreparedSelectNameById(1), CultureInfo.InvariantCulture);
                var count = Convert.ToInt32(ExecutePreparedCountRows(), CultureInfo.InvariantCulture);
                if (count != 1)
                {
                    throw new InvalidOperationException($"Unexpected transactional update/delete count for {_dialect.DisplayName}: {count}.");
                }

                GC.KeepAlive(value);
                return count;
            }
            finally
            {
                try
                {
                    ExecutePreparedUpdateById("Alice");
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecutePreparedInsertUser2();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public string RunUpsert()
        {
            try
            {
                ExecutePreparedUpsert();
                var value = Convert.ToString(ExecutePreparedSelectNameById(1), CultureInfo.InvariantCulture);
                if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Unexpected upsert result for {_dialect.DisplayName}: {value ?? "<null>"}.");
                }

                return value!;
            }
            finally
            {
                try
                {
                    ExecutePreparedUpdateById("Alice");
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        private void ExecutePreparedUpsert()
        {
            var command = GetOrCreateUpsertCommand();
            ExecuteNonQuery(command);
        }

        private int ExecutePreparedUpdateById(string name, DbTransaction? transaction = null)
        {
            var command = GetOrCreateUpdateByIdCommand();
            _updateByIdNameParameter!.Value = name;
            _updateByIdIdParameter!.Value = 1;
            return ExecuteNonQuery(command, transaction);
        }

        private int ExecutePreparedDeleteById(int id, DbTransaction? transaction = null)
        {
            var command = GetOrCreateDeleteByIdCommand();
            _deleteByIdIdParameter!.Value = id;
            return ExecuteNonQuery(command, transaction);
        }

        private int ExecutePreparedInsertUser1()
            => ExecutePreparedInsertUser(1, "Alice", ref _insertUser1Command);

        private int ExecutePreparedInsertUser2()
            => ExecutePreparedInsertUser(2, "Bob", ref _insertUser2Command);

        private int ExecutePreparedInsertUser(int id, string name, ref DbCommand? command)
        {
            var prepared = command ??= CreateInsertUserCommand(id, name);
            return ExecuteNonQuery(prepared);
        }

        private DbCommand CreateInsertUserCommand(int id, string name)
        {
            var command = connection.CreateCommand();
            command.CommandText = _dialect.InsertUser(UsersTable, id, name);
            return command;
        }

        private object? ExecutePreparedSelectNameById(int id)
        {
            var command = GetOrCreateSelectByIdCommand();
            _selectByIdIdParameter!.Value = id;
            return ExecuteScalar(command);
        }

        private object? ExecutePreparedCountRows()
            => ExecuteScalar(GetOrCreateCountCommand());

        private int ExecuteNonQuery(DbCommand command, DbTransaction? transaction = null)
        {
            var previousTransaction = command.Transaction;
            if (transaction is not null)
                command.Transaction = transaction;

            try
            {
                return command.ExecuteNonQuery();
            }
            finally
            {
                command.Transaction = previousTransaction;
            }
        }

        private object? ExecuteScalar(DbCommand command, DbTransaction? transaction = null)
        {
            var previousTransaction = command.Transaction;
            if (transaction is not null)
                command.Transaction = transaction;

            try
            {
                return command.ExecuteScalar();
            }
            finally
            {
                command.Transaction = previousTransaction;
            }
        }

        private object? ExecuteScalar(DbCommand command)
            => command.ExecuteScalar();

        private static int ExecuteNonQuery(DbConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQuery();
        }

        private DbCommand GetOrCreateUpdateByIdCommand()
        {
            if (_updateByIdCommand is not null)
                return _updateByIdCommand;

            var command = connection.CreateCommand();
            command.CommandText = $"""
UPDATE {UsersTable}
SET Name = {service.Dialect.Parameter("name")}
WHERE Id = {service.Dialect.Parameter("id")}
""";

            _updateByIdNameParameter = CreateParameter(command, "name", DbType.String);
            _updateByIdIdParameter = CreateParameter(command, "id", DbType.Int32);
            _updateByIdCommand = command;
            return command;
        }

        private DbCommand GetOrCreateDeleteByIdCommand()
        {
            if (_deleteByIdCommand is not null)
                return _deleteByIdCommand;

            var command = connection.CreateCommand();
            command.CommandText = $"""
DELETE FROM {UsersTable}
WHERE Id = {service.Dialect.Parameter("id")}
""";

            _deleteByIdIdParameter = CreateParameter(command, "id", DbType.Int32);
            _deleteByIdCommand = command;
            return command;
        }

        private DbCommand GetOrCreateSelectByIdCommand()
        {
            if (_selectByIdCommand is not null)
                return _selectByIdCommand;

            var command = connection.CreateCommand();
            command.CommandText = $"""
SELECT Name
FROM {UsersTable}
WHERE Id = {service.Dialect.Parameter("id")}
""";

            _selectByIdIdParameter = CreateParameter(command, "id", DbType.Int32);
            _selectByIdCommand = command;
            return command;
        }

        private DbCommand GetOrCreateCountCommand()
        {
            if (_countCommand is not null)
                return _countCommand;

            var command = connection.CreateCommand();
            command.CommandText = service.Dialect.CountRows(UsersTable);
            _countCommand = command;
            return command;
        }

        private DbCommand GetOrCreateUpsertCommand()
        {
            if (_upsertCommand is not null)
                return _upsertCommand;

            var command = connection.CreateCommand();
            command.CommandText = _dialect.Upsert(UsersTable, 1, "Alice-v2");
            _upsertCommand = command;
            return command;
        }

        private static DbParameter CreateParameter(DbCommand command, string name, DbType dbType)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = dbType;
            parameter.Value = DBNull.Value;
            command.Parameters.Add(parameter);
            return parameter;
        }

        public void Dispose()
        {
            try
            {
                _updateByIdCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _deleteByIdCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _selectByIdCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _countCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _upsertCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _insertUser1Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _insertUser2Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedParameterTransactionUsersState(
        DbConnection connection,
        DmlMutationServiceTest<DbConnection> service,
        ProviderSqlDialect dialect,
        string users,
        string uId) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;
        private DbCommand? _deleteUser1Command;
        private DbCommand? _deleteUser2Command;

        private string UsersTable => $"{users}_{uId}";

        public int RunParameterTransactionCommit()
        {
            try
            {
                return service.RunParameterTransactionCommit(
                    UsersTable,
                    "Alice-v2",
                    "Bob-v2",
                    "alice@example.com",
                    "bob@example.com",
                    NormalizeDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified)),
                    NormalizeDateTimeInput(new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified)));
            }
            finally
            {
                try
                {
                    ExecutePreparedDeleteUser2();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecutePreparedDeleteUser1();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public int RunParameterTransactionRollback()
            => service.RunParameterTransactionRollback(
                UsersTable,
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                NormalizeDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified)),
                NormalizeDateTimeInput(new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified)));

        private int ExecutePreparedDeleteUser1()
            => ExecutePreparedDeleteById(1, ref _deleteUser1Command);

        private int ExecutePreparedDeleteUser2()
            => ExecutePreparedDeleteById(2, ref _deleteUser2Command);

        private int ExecutePreparedDeleteById(int id, ref DbCommand? command)
        {
            var prepared = command ??= CreateDeleteCommand(id);
            return ExecuteNonQuery(prepared);
        }

        private DbCommand CreateDeleteCommand(int id)
        {
            var command = connection.CreateCommand();
            command.CommandText = _dialect.DeleteUserById(UsersTable, id);
            return command;
        }

        private static int ExecuteNonQuery(DbCommand command)
            => command.ExecuteNonQuery();

        private static int ExecuteNonQuery(DbConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQuery();
        }

        private DateTime NormalizeDateTimeInput(DateTime value)
            => _dialect.Provider == ProviderId.Npgsql && value.Kind == DateTimeKind.Unspecified
                ? DateTime.SpecifyKind(value, DateTimeKind.Utc)
                : value;

        public void Dispose()
        {
            try
            {
                _deleteUser1Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _deleteUser2Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedTransactionUsersState(
        DbConnection connection,
        DmlMutationServiceTest<DbConnection> service,
        ProviderSqlDialect dialect,
        string users,
        string uId) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;
        private DbCommand? _deleteUser1Command;
        private DbCommand? _deleteUser2Command;

        private string UsersTable => $"{users}_{uId}";

        public int RunTransactionCommit()
        {
            try
            {
                return service.RunTransactionCommit(UsersTable);
            }
            finally
            {
                try
                {
                    ExecutePreparedDeleteUser1();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public int RunTransactionRollback()
            => service.RunTransactionRollback(UsersTable);

        public int RunRollbackToSavepoint()
        {
            try
            {
                return service.RunRollbackToSavepoint(UsersTable);
            }
            finally
            {
                try
                {
                    ExecutePreparedDeleteUser1();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public int RunNestedSavepointFlow()
        {
            try
            {
                return service.RunNestedSavepointFlow(UsersTable);
            }
            finally
            {
                try
                {
                    ExecutePreparedDeleteUser2();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecutePreparedDeleteUser1();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        private int ExecutePreparedDeleteUser1()
            => ExecutePreparedDeleteById(1, ref _deleteUser1Command);

        private int ExecutePreparedDeleteUser2()
            => ExecutePreparedDeleteById(2, ref _deleteUser2Command);

        private int ExecutePreparedDeleteById(int id, ref DbCommand? command)
        {
            var prepared = command ??= CreateDeleteCommand(id);
            return ExecuteNonQuery(prepared);
        }

        private DbCommand CreateDeleteCommand(int id)
        {
            var command = connection.CreateCommand();
            command.CommandText = _dialect.DeleteUserById(UsersTable, id);
            return command;
        }

        private static int ExecuteNonQuery(DbCommand command)
            => command.ExecuteNonQuery();

        private static int ExecuteNonQuery(DbConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQuery();
        }

        public void Dispose()
        {
            try
            {
                _deleteUser1Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _deleteUser2Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedNoopMutationState(
        DbConnection connection,
        DmlMutationServiceTest<DbConnection> service) : IDisposable
    {
        public DmlMutationServiceTest<DbConnection> Service => service;

        public void Dispose()
        {
            try
            {
                service.DropScenario();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedNoopQueryState(
        DbConnection connection,
        QueryServiceTest<DbConnection> service) : IDisposable
    {
        public QueryServiceTest<DbConnection> Service => service;

        public void Dispose()
        {
            try
            {
                service.DropScenario();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedTypedFieldStorageMatrixState(
        DbConnection connection,
        QueryServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = service.Dialect;

        public QueryResultSnapshot RunTypedFieldStorageMatrix()
            => RunTypedFieldMatrix(() => service.RunTypedFieldStorageMatrix(users, uId));

        public QueryResultSnapshot RunTypedFieldFunctionMatrix()
            => RunTypedFieldMatrix(() => service.RunTypedFieldFunctionMatrix(users, uId));

        public QueryResultSnapshot RunTypedFieldCalculationMatrix()
            => RunTypedFieldMatrix(() => service.RunTypedFieldCalculationMatrix(users, uId));

        public QueryResultSnapshot RunTemporalFieldMatrix()
            => RunTypedFieldMatrix(() => service.RunTemporalFieldMatrix(users, uId));

        public QueryResultSnapshot RunTemporalComparisonMatrix()
            => RunTypedFieldMatrix(() => service.RunTemporalComparisonMatrix(users, uId));

        public QueryResultSnapshot RunTemporalArithmeticMatrix()
            => RunTypedFieldMatrix(() => service.RunTemporalArithmeticMatrix(users, uId));

        public QueryResultSnapshot RunJsonTypedFieldMatrix()
            => RunTypedFieldMatrix(() => service.RunJsonTypedFieldMatrix(users, uId));

        public int RunParameterRoundTripMatrix()
        {
            var createdAt = _dialect.Provider == ProviderId.Npgsql
                ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
                : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);

            return RunTypedFieldMatrix(() => service.RunParameterRoundTripMatrix(
                users,
                uId,
                1,
                "Param Alice",
                DBNull.Value,
                true,
                (short)31,
                12.34m,
                createdAt,
                DBNull.Value,
                DBNull.Value));
        }

        public int RunTypedFieldAndFunctionBlend()
            => RunTypedFieldMatrix(() => service.RunTypedFieldAndFunctionBlend(users, uId));

        public int RunTypedFieldCompoundPredicateMatrix()
            => RunTypedFieldMatrix(() => service.RunTypedFieldCompoundPredicateMatrix(users, uId));

        public QueryResultSnapshot RunCastCalculationMatrix()
            => RunTypedFieldMatrix(() => service.RunCastCalculationMatrix(users, uId));

        public QueryResultSnapshot RunNullComparisonMatrix()
            => RunTypedFieldMatrix(() => service.RunNullComparisonMatrix(users, uId));

        public QueryResultSnapshot RunTextLengthMatrix()
            => RunTypedFieldMatrix(() => service.RunTextLengthMatrix(users, uId));

        public QueryResultSnapshot RunTextCaseMatrix()
            => RunTypedFieldMatrix(() => service.RunTextCaseMatrix(users, uId));

        public QueryResultSnapshot RunTypedFieldPredicateMatrix()
            => RunTypedFieldMatrix(() => service.RunTypedFieldPredicateMatrix(users, uId));

        private T RunTypedFieldMatrix<T>(Func<T> action)
        {
            try
            {
                return action();
            }
            finally
            {
                CleanupTypedFieldRows();
            }
        }

        private void CleanupTypedFieldRows()
        {
            var usersTable = $"{users}_{uId}";

            try
            {
                ExecuteNonQuery(connection, _dialect.DeleteUserById(usersTable, 4));
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                ExecuteNonQuery(connection, _dialect.DeleteUserById(usersTable, 3));
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                ExecuteNonQuery(connection, _dialect.DeleteUserById(usersTable, 2));
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                ExecuteNonQuery(connection, _dialect.DeleteUserById(usersTable, 1));
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
        }

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedParameterProjectionState(
        DbConnection connection,
        ProviderSqlDialect dialect) : IDisposable
    {
        private static readonly Guid ProjectionGuid = Guid.Parse("11111111-2222-3333-4444-555555555555");
        private static readonly byte[] ProjectionBinary = [1, 2, 3, 4];
        private static readonly DateTime ProjectionDateTime = new(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
        private static readonly DateTimeOffset ProjectionDateTimeOffset = new(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);
        private static readonly DateTime ProjectionDate = new(2024, 1, 2);
        private static readonly TimeSpan ProjectionTimeSpan = TimeSpan.FromHours(1.5);
        private readonly ProviderSqlDialect _dialect = dialect;
        private DbCommand? _command;
        private DbParameter? _textValueParameter;
        private DbParameter? _ansiTextValueParameter;
        private DbParameter? _ansiFixedTextValueParameter;
        private DbParameter? _fixedTextValueParameter;
        private DbParameter? _int16ValueParameter;
        private DbParameter? _int32ValueParameter;
        private DbParameter? _int64ValueParameter;
        private DbParameter? _boolValueParameter;
        private DbParameter? _decimalValueParameter;
        private DbParameter? _doubleValueParameter;
        private DbParameter? _timeSpanValueParameter;
        private DbParameter? _dateTimeOffsetValueParameter;
        private DbParameter? _dateTimeValueParameter;
        private DbParameter? _guidValueParameter;
        private DbParameter? _binaryValueParameter;
        private DbParameter? _dateValueParameter;
        private DbParameter? _currencyValueParameter;

        public object? RunParameterProjection()
        {
            var command = GetOrCreateCommand();

            SetParameterValue(_textValueParameter!, DbType.String, "benchmark");
            SetParameterValue(_ansiTextValueParameter!, DbType.AnsiString, "ansi");
            SetParameterValue(_ansiFixedTextValueParameter!, DbType.AnsiStringFixedLength, "fixed-ansi");
            SetParameterValue(_fixedTextValueParameter!, DbType.StringFixedLength, "fixed-text");
            SetParameterValue(_int16ValueParameter!, DbType.Int16, (short)16);
            SetParameterValue(_int32ValueParameter!, DbType.Int32, 32);
            SetParameterValue(_int64ValueParameter!, DbType.Int64, 64L);
            SetParameterValue(_boolValueParameter!, DbType.Boolean, true);
            SetParameterValue(_decimalValueParameter!, DbType.Decimal, 12.34m);
            SetParameterValue(_doubleValueParameter!, DbType.Double, 56.78d);
            SetParameterValue(_timeSpanValueParameter!, DbType.Time, ProjectionTimeSpan);
            SetParameterValue(_dateTimeOffsetValueParameter!, DbType.DateTimeOffset, ProjectionDateTimeOffset);
            SetParameterValue(_dateTimeValueParameter!, DbType.DateTime, ProjectionDateTime);
            SetParameterValue(_guidValueParameter!, DbType.Guid, ProjectionGuid);
            SetParameterValue(_binaryValueParameter!, DbType.Binary, ProjectionBinary);
            SetParameterValue(_dateValueParameter!, DbType.Date, ProjectionDate);
            SetParameterValue(_currencyValueParameter!, DbType.Currency, 123.45m);

            return command.ExecuteScalar();
        }

        private DbCommand GetOrCreateCommand()
        {
            if (_command is not null)
                return _command;

            var command = connection.CreateCommand();
            command.CommandText = _dialect.Provider == ProviderId.Db2
                ? $"""
SELECT
    CAST({_dialect.Parameter("textValue")} AS VARCHAR(100)) AS TextValue,
    CAST({_dialect.Parameter("ansiTextValue")} AS VARCHAR(100)) AS AnsiTextValue,
    CAST({_dialect.Parameter("ansiFixedTextValue")} AS CHAR(20)) AS AnsiFixedTextValue,
    CAST({_dialect.Parameter("fixedTextValue")} AS CHAR(20)) AS FixedTextValue,
    CAST({_dialect.Parameter("int16Value")} AS SMALLINT) AS Int16Value,
    CAST({_dialect.Parameter("int32Value")} AS INTEGER) AS Int32Value,
    CAST({_dialect.Parameter("int64Value")} AS BIGINT) AS Int64Value,
    CAST({_dialect.Parameter("boolValue")} AS BOOLEAN) AS BoolValue,
    CAST({_dialect.Parameter("decimalValue")} AS DECIMAL(19,4)) AS DecimalValue,
    CAST({_dialect.Parameter("doubleValue")} AS DOUBLE) AS DoubleValue,
    CAST({_dialect.Parameter("timeSpanValue")} AS VARCHAR(32)) AS TimeSpanValue,
    CAST({_dialect.Parameter("dateTimeOffsetValue")} AS VARCHAR(40)) AS DateTimeOffsetValue,
    CAST({_dialect.Parameter("dateTimeValue")} AS TIMESTAMP) AS DateTimeValue,
    CAST({_dialect.Parameter("guidValue")} AS VARCHAR(36)) AS GuidValue,
    CAST({_dialect.Parameter("binaryValue")} AS VARCHAR(4) FOR BIT DATA) AS BinaryValue,
    CAST({_dialect.Parameter("dateValue")} AS DATE) AS DateValue,
    CAST({_dialect.Parameter("currencyValue")} AS DECIMAL(19,2)) AS CurrencyValue
FROM SYSIBM.SYSDUMMY1
"""
                : _dialect.SelectParameterProjection($@"
    {_dialect.Parameter("textValue")} AS TextValue,
    {_dialect.Parameter("ansiTextValue")} AS AnsiTextValue,
    {_dialect.Parameter("ansiFixedTextValue")} AS AnsiFixedTextValue,
    {_dialect.Parameter("fixedTextValue")} AS FixedTextValue,
    {_dialect.Parameter("int16Value")} AS Int16Value,
    {_dialect.Parameter("int32Value")} AS Int32Value,
    {_dialect.Parameter("int64Value")} AS Int64Value,
    {_dialect.Parameter("boolValue")} AS BoolValue,
    {_dialect.Parameter("decimalValue")} AS DecimalValue,
    {_dialect.Parameter("doubleValue")} AS DoubleValue,
    {_dialect.Parameter("timeSpanValue")} AS TimeSpanValue,
    {_dialect.Parameter("dateTimeOffsetValue")} AS DateTimeOffsetValue,
    {_dialect.Parameter("dateTimeValue")} AS DateTimeValue,
    {_dialect.Parameter("guidValue")} AS GuidValue,
    {_dialect.Parameter("binaryValue")} AS BinaryValue,
    {_dialect.Parameter("dateValue")} AS DateValue,
    {_dialect.Parameter("currencyValue")} AS CurrencyValue");

            _textValueParameter = CreateParameter(command, "textValue", DbType.String);
            _ansiTextValueParameter = CreateParameter(command, "ansiTextValue", DbType.AnsiString);
            _ansiFixedTextValueParameter = CreateParameter(command, "ansiFixedTextValue", DbType.AnsiStringFixedLength);
            _fixedTextValueParameter = CreateParameter(command, "fixedTextValue", DbType.StringFixedLength);
            _int16ValueParameter = CreateParameter(command, "int16Value", DbType.Int16);
            _int32ValueParameter = CreateParameter(command, "int32Value", DbType.Int32);
            _int64ValueParameter = CreateParameter(command, "int64Value", DbType.Int64);
            _boolValueParameter = CreateParameter(command, "boolValue", DbType.Boolean);
            _decimalValueParameter = CreateParameter(command, "decimalValue", DbType.Decimal);
            _doubleValueParameter = CreateParameter(command, "doubleValue", DbType.Double);
            _timeSpanValueParameter = CreateParameter(command, "timeSpanValue", DbType.Time);
            _dateTimeOffsetValueParameter = CreateParameter(command, "dateTimeOffsetValue", DbType.DateTimeOffset);
            _dateTimeValueParameter = CreateParameter(command, "dateTimeValue", DbType.DateTime);
            _guidValueParameter = CreateParameter(command, "guidValue", DbType.Guid);
            _binaryValueParameter = CreateParameter(command, "binaryValue", DbType.Binary);
            _dateValueParameter = CreateParameter(command, "dateValue", DbType.Date);
            _currencyValueParameter = CreateParameter(command, "currencyValue", DbType.Currency);

            _command = command;
            return command;
        }

        private static DbParameter CreateParameter(DbCommand command, string name, DbType dbType)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            var parameterTypeName = parameter.GetType().FullName;
            var isOracleParameter = parameterTypeName == "Oracle.ManagedDataAccess.Client.OracleParameter";
            var isFirebirdParameter = parameterTypeName == "FirebirdSql.Data.FirebirdClient.FbParameter";
            var isDb2Parameter = parameterTypeName is "IBM.Data.Db2.DB2Parameter"
                or "IBM.Data.DB2.Core.DB2Parameter"
                or "IBM.Data.DB2.iSeries.iDB2Parameter";

            if (isFirebirdParameter && (dbType == DbType.Currency || dbType == DbType.DateTimeOffset || dbType == DbType.Guid))
            {
                // Firebird rejects these DbType assignments in this benchmark path.
                // Keep the payload-based flow and let the provider infer the storage type.
            }
            else if (isDb2Parameter && (dbType == DbType.Guid || dbType == DbType.DateTimeOffset || dbType == DbType.Time || dbType == DbType.DateTime))
            {
                parameter.DbType = DbType.String;
            }
            else if (!isOracleParameter)
            {
                parameter.DbType = dbType;
            }

            command.Parameters.Add(parameter);
            return parameter;
        }

        private static void SetParameterValue(DbParameter parameter, DbType dbType, object? value)
        {
            var parameterTypeName = parameter.GetType().FullName;
            var isOracleParameter = parameterTypeName == "Oracle.ManagedDataAccess.Client.OracleParameter";
            var isFirebirdParameter = parameterTypeName == "FirebirdSql.Data.FirebirdClient.FbParameter";
            var isDb2Parameter = parameterTypeName is "IBM.Data.Db2.DB2Parameter"
                or "IBM.Data.DB2.Core.DB2Parameter"
                or "IBM.Data.DB2.iSeries.iDB2Parameter";

            parameter.Value = isOracleParameter
                ? NormalizeOracleParameterValue(value)
                : isDb2Parameter
                    ? NormalizeDb2ParameterValue(dbType, value)
                    : isFirebirdParameter
                        ? NormalizeFirebirdParameterValue(dbType, value)
                        : value ?? DBNull.Value;
        }

        private static object NormalizeDb2ParameterValue(DbType dbType, object? value)
        {
            if (value is null)
                return DBNull.Value;

            return (dbType, value) switch
            {
                (DbType.Guid, Guid guid) => guid.ToString("D", CultureInfo.InvariantCulture),
                (DbType.Time, TimeSpan timeSpan) => timeSpan.ToString("c", CultureInfo.InvariantCulture),
                (DbType.DateTime, DateTime dateTime) => dateTime.ToString("O", CultureInfo.InvariantCulture),
                (DbType.DateTimeOffset, DateTimeOffset dateTimeOffset) => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
                _ => value
            };
        }

        private static object NormalizeFirebirdParameterValue(DbType dbType, object? value)
        {
            if (value is null)
                return DBNull.Value;

            return (dbType, value) switch
            {
                (DbType.Guid, Guid guid) => guid.ToString("D", CultureInfo.InvariantCulture),
                (DbType.DateTimeOffset, DateTimeOffset dateTimeOffset) => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
                _ => value
            };
        }

        private static object NormalizeOracleParameterValue(object? value)
        {
            if (value is null)
                return DBNull.Value;

            return value switch
            {
                TimeSpan timeSpan => timeSpan.ToString("c", CultureInfo.InvariantCulture),
                Guid guid => guid.ToString("D", CultureInfo.InvariantCulture),
                _ => value
            };
        }

        public void Dispose()
        {
            try
            {
                _command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            connection.Dispose();
        }
    }

    private sealed class PreparedParameterMatrixState(
        DbConnection connection,
        QueryServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public int RunParameterTypeMatrix()
        {
            var createdAt = service.Dialect.Provider == ProviderId.Npgsql
                ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
                : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);

            return service.RunParameterTypeMatrix(
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
                new byte[] { 1, 2, 3, 4 });
        }

        public int RunParameterDateCurrencyMatrix()
            => service.RunParameterDateCurrencyMatrix(new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Unspecified), 123.45m);

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedStoredProcedureState(
        DbConnection connection,
        string procedureName) : IDisposable
    {
        public int RunStoredProcedureCall()
        {
            using var command = connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = procedureName;

            AddParameter(command, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
            AddParameter(command, "note", DbType.String, "benchmark", ParameterDirection.Input);
            AddParameter(command, "counter", DbType.Int32, DBNull.Value, ParameterDirection.Output);
            AddParameter(command, "message", DbType.String, DBNull.Value, ParameterDirection.Output);
            var resultCodeApplied = AddParameter(command, "resultCode", DbType.Int32, DBNull.Value, ParameterDirection.ReturnValue);

            var affected = command.ExecuteNonQuery();
            GC.KeepAlive(command.Parameters["counter"].Value);
            GC.KeepAlive(command.Parameters["message"].Value);
            if (!resultCodeApplied)
            {
                command.Parameters["resultCode"].Value = 0;
            }
            GC.KeepAlive(command.Parameters["resultCode"].Value);
            return affected;
        }

        private static bool AddParameter(
            DbCommand command,
            string name,
            DbType dbType,
            object? value,
            ParameterDirection direction)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = dbType;
            var directionApplied = TrySetDirection(parameter, direction);
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
            return directionApplied;
        }

        private static bool TrySetDirection(DbParameter parameter, ParameterDirection direction)
        {
            try
            {
                parameter.Direction = direction;
                return true;
            }
            catch (ArgumentException) when (direction != ParameterDirection.Input)
            {
                return false;
            }
        }

        public void Dispose()
        {
            connection.Dispose();
        }
    }

    private sealed class PreparedReturningInsertState(
        DbConnection connection,
        BatchServiceTest<DbConnection> service,
        ProviderSqlDialect dialect,
        string users,
        string uId) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;
        private DbCommand? _returningInsertCommand;
        private DbCommand? _countCommand;
        private DbCommand? _deleteUser1Command;
        private DbCommand? _deleteUser2Command;

        private string UsersTable => $"{users}_{uId}";

        public int RunReturningInsert()
        {
            try
            {
                var rows = ExecuteReaderCount(GetOrCreateReturningInsertCommand());
                var count = Convert.ToInt32(ExecuteScalar(GetOrCreateCountCommand()), CultureInfo.InvariantCulture);

                if (rows != 1)
                {
                    throw new InvalidOperationException($"Unexpected RETURNING rowcount for {_dialect.DisplayName}: {rows}.");
                }

                if (count != 1)
                {
                    throw new InvalidOperationException($"Unexpected RETURNING insert persistence for {_dialect.DisplayName}: {count}.");
                }

                GC.KeepAlive(rows);
                GC.KeepAlive(count);
                return rows;
            }
            finally
            {
                try
                {
                    ExecutePreparedDeleteUser1();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        private DbCommand GetOrCreateReturningInsertCommand()
        {
            if (_returningInsertCommand is not null)
                return _returningInsertCommand;

            var command = connection.CreateCommand();
            command.CommandText = _dialect.InsertUserReturning(UsersTable, 1, "Alice");
            _returningInsertCommand = command;
            return command;
        }

        private DbCommand GetOrCreateCountCommand()
        {
            if (_countCommand is not null)
                return _countCommand;

            var command = connection.CreateCommand();
            command.CommandText = _dialect.CountRows(UsersTable);
            _countCommand = command;
            return command;
        }

        private int ExecutePreparedDeleteUser1()
            => ExecutePreparedDeleteById(1, ref _deleteUser1Command);

        private int ExecutePreparedDeleteUser2()
            => ExecutePreparedDeleteById(2, ref _deleteUser2Command);

        private int ExecutePreparedDeleteById(int id, ref DbCommand? command)
        {
            var prepared = command ??= CreateDeleteCommand(id);
            return ExecuteNonQuery(prepared);
        }

        private DbCommand CreateDeleteCommand(int id)
        {
            var command = connection.CreateCommand();
            command.CommandText = _dialect.DeleteUserById(UsersTable, id);
            return command;
        }

        private static int ExecuteNonQuery(DbCommand command)
            => command.ExecuteNonQuery();

        private static object? ExecuteScalar(DbCommand command)
            => command.ExecuteScalar();

        private static int ExecuteNonQuery(DbConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQuery();
        }

        private static int ExecuteReaderCount(DbCommand command)
        {
            using var reader = command.ExecuteReader();
            var count = 0;
            while (reader.Read())
                count++;

            return count;
        }

        public void Dispose()
        {
            try
            {
                _returningInsertCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _countCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _deleteUser1Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _deleteUser2Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedSequenceState(
        DbConnection connection,
        DmlMutationServiceTest<DbConnection> service,
        string sequence) : IDisposable
    {
        public object? RunSequenceNextValue()
            => service.RunSequenceNextValue(sequence);

        public void Dispose()
        {
            try
            {
                service.DropScenario(sequence);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropSequence(sequence));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        private static int ExecuteNonQuery(DbConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQuery();
        }
    }

    private sealed class PreparedBatchUsersState(
        DbConnection connection,
        BatchServiceTest<DbConnection> service,
        ProviderSqlDialect dialect,
        string users,
        string uId) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;
        private readonly (int id, string name)[] _batch10Values = CreateBatchValues(10);
        private readonly (int id, string name)[] _batch100Values = CreateBatchValues(100);
        private BatchInsertPlan? _batch10Plan;
        private BatchInsertPlan? _batch100Plan;
        private DbCommand? _insertUser1Command;
        private DbCommand? _insertUser2Command;
        private DbCommand? _updateUser2Command;
        private DbParameter? _updateUser2NameParameter;
        private DbCommand? _deleteUser1Command;
        private DbCommand? _deleteUser2Command;
        private DbCommand? _selectUser1Command;
        private DbCommand? _selectUser2Command;
        private DbCommand? _countCommand;

        private string UsersTable => $"{users}_{uId}";

        private static (int id, string name)[] CreateBatchValues(int rowCount)
        {
            var values = new (int id, string name)[rowCount];
            for (var i = 1; i <= rowCount; i++)
            {
                values[i - 1] = (i, $"User-{i}");
            }

            return values;
        }

        public string RunBatchMixedReadWrite()
        {
            using var transaction = connection.BeginTransaction();
            ExecutePreparedInsertUser1(transaction);
            ExecutePreparedInsertUser2(transaction);
            var value = Convert.ToString(ExecutePreparedSelectUser1(transaction), CultureInfo.InvariantCulture);
            ExecutePreparedUpdateUser2(transaction);
            transaction.Rollback();

            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected mixed-batch read result for {_dialect.DisplayName}: {value ?? "<null>"}.");
            }

            return value!;
        }

        public string RunBatchScalar()
        {
            using var transaction = connection.BeginTransaction();
            ExecutePreparedInsertUser1(transaction);
            ExecutePreparedInsertUser2(transaction);
            var count = Convert.ToInt32(ExecutePreparedCountRows(transaction), CultureInfo.InvariantCulture);
            var second = Convert.ToString(ExecutePreparedSelectUser2(transaction), CultureInfo.InvariantCulture);
            transaction.Rollback();

            if (count != 2 || !string.Equals(second, "Bob", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected scalar batch result for {_dialect.DisplayName}: count={count}, second={second ?? "<null>"}.");
            }

            return second!;
        }

        public int RunBatchNonQuery()
        {
            using var transaction = connection.BeginTransaction();
            ExecutePreparedInsertUser1(transaction);
            ExecutePreparedInsertUser2(transaction);
            ExecutePreparedUpdateUser2(transaction);
            ExecutePreparedDeleteUser1(transaction);
            var count = Convert.ToInt32(ExecutePreparedCountRows(transaction), CultureInfo.InvariantCulture);
            transaction.Rollback();

            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected non-query batch count for {_dialect.DisplayName}: {count}.");
            }

            return count;
        }

        public object? RunBatchReaderMultiResult()
        {
            using var transaction = connection.BeginTransaction();
            ExecutePreparedInsertUser1(transaction);
            ExecutePreparedInsertUser2(transaction);
            var first = Convert.ToInt32(ExecutePreparedCountRows(transaction), CultureInfo.InvariantCulture);
            var second = ExecutePreparedSelectUser1(transaction);
            transaction.Rollback();

            if (first != 2 || !string.Equals(Convert.ToString(second, CultureInfo.InvariantCulture), "Alice", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected batch reader result for {_dialect.DisplayName}: count={first}, value={second ?? "<null>"}.");
            }

            GC.KeepAlive(first);
            GC.KeepAlive(second);
            return second;
        }

        public int RunBatchInsert(int rowCount)
        {
            using var transaction = connection.BeginTransaction();
            var values = rowCount == 10 ? _batch10Values : rowCount == 100 ? _batch100Values : CreateBatchValues(rowCount);
            var plan = rowCount == 10
                ? _batch10Plan ??= CreateBatchInsertPlan(10)
                : rowCount == 100
                    ? _batch100Plan ??= CreateBatchInsertPlan(100)
                    : CreateBatchInsertPlan(rowCount);

            try
            {
                plan.Command.Transaction = transaction;
                for (var i = 0; i < rowCount; i++)
                {
                    plan.IdParameters[i].Value = values[i].id;
                    plan.NameParameters[i].Value = values[i].name;
                }

                plan.Command.ExecuteNonQuery();
                var count = Convert.ToInt32(ExecutePreparedCountRows(transaction), CultureInfo.InvariantCulture);
                transaction.Rollback();

                if (count != rowCount)
                {
                    throw new InvalidOperationException($"Expected {rowCount} rows for {_dialect.DisplayName}, got {count}.");
                }

                return count;
            }
            finally
            {
                plan.Command.Transaction = null;
                if (rowCount != 10 && rowCount != 100)
                    plan.Dispose();
            }
        }

        public int RunRowCountInBatch()
        {
            using var transaction = connection.BeginTransaction();
            ExecutePreparedInsertUser1(transaction);
            ExecutePreparedInsertUser2(transaction);

            var count = Convert.ToInt32(ExecutePreparedCountRows(transaction), CultureInfo.InvariantCulture);
            transaction.Rollback();

            if (count != 2)
            {
                throw new InvalidOperationException($"Unexpected batch rowcount for {_dialect.DisplayName}: {count}.");
            }

            return count;
        }

        public string RunBatchTransactionControl()
        {
            using var transaction = connection.BeginTransaction();
            ExecutePreparedInsertUser1(transaction);
            ExecutePreparedInsertUser2(transaction);
            transaction.Commit();

            try
            {
                ExecutePreparedDeleteUser1();
                ExecutePreparedDeleteUser2();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            GC.KeepAlive(UsersTable);
            return UsersTable;
        }

        public void Dispose()
        {
            try
            {
                _insertUser1Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _insertUser2Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _updateUser2Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _deleteUser1Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _deleteUser2Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _selectUser1Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _selectUser2Command?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _countCommand?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _batch10Plan?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                _batch100Plan?.Dispose();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }

            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        private int ExecutePreparedInsertUser1(DbTransaction? transaction = null)
            => ExecutePreparedInsertUser(1, "Alice", ref _insertUser1Command, transaction);

        private int ExecutePreparedInsertUser2(DbTransaction? transaction = null)
            => ExecutePreparedInsertUser(2, "Bob", ref _insertUser2Command, transaction);

        private int ExecutePreparedUpdateUser2(DbTransaction? transaction = null)
        {
            var command = _updateUser2Command ??= CreateUpdateUser2Command();
            _updateUser2NameParameter!.Value = "Bob-v2";
            return ExecuteNonQuery(command, transaction);
        }

        private int ExecutePreparedDeleteUser1(DbTransaction? transaction = null)
            => ExecutePreparedDeleteById(1, ref _deleteUser1Command, transaction);

        private int ExecutePreparedDeleteUser2(DbTransaction? transaction = null)
            => ExecutePreparedDeleteById(2, ref _deleteUser2Command, transaction);

        private object? ExecutePreparedSelectUser1(DbTransaction? transaction = null)
            => ExecutePreparedSelectById(1, ref _selectUser1Command, transaction);

        private object? ExecutePreparedSelectUser2(DbTransaction? transaction = null)
            => ExecutePreparedSelectById(2, ref _selectUser2Command, transaction);

        private object? ExecutePreparedCountRows(DbTransaction? transaction = null)
            => ExecuteScalar(GetOrCreateCountCommand(), transaction);

        private int ExecutePreparedInsertUser(int id, string name, ref DbCommand? command, DbTransaction? transaction)
        {
            var prepared = command ??= CreateInsertUserCommand(id, name);
            return ExecuteNonQuery(prepared, transaction);
        }

        private int ExecutePreparedDeleteById(int id, ref DbCommand? command, DbTransaction? transaction)
        {
            var prepared = command ??= CreateDeleteUserCommand(id);
            return ExecuteNonQuery(prepared, transaction);
        }

        private object? ExecutePreparedSelectById(int id, ref DbCommand? command, DbTransaction? transaction)
        {
            var prepared = command ??= CreateSelectUserCommand(id);
            return ExecuteScalar(prepared, transaction);
        }

        private DbCommand GetOrCreateCountCommand()
        {
            if (_countCommand is not null)
                return _countCommand;

            var command = connection.CreateCommand();
            command.CommandText = _dialect.CountRows(UsersTable);
            _countCommand = command;
            return command;
        }

        private DbCommand CreateInsertUserCommand(int id, string name)
        {
            var command = connection.CreateCommand();
            command.CommandText = _dialect.InsertUser(UsersTable, id, name);
            return command;
        }

        private DbCommand CreateDeleteUserCommand(int id)
        {
            var command = connection.CreateCommand();
            command.CommandText = _dialect.DeleteUserById(UsersTable, id);
            return command;
        }

        private DbCommand CreateSelectUserCommand(int id)
        {
            var command = connection.CreateCommand();
            command.CommandText = _dialect.SelectUserNameById(UsersTable, id);
            return command;
        }

        private DbCommand CreateUpdateUser2Command()
        {
            var command = connection.CreateCommand();
            command.CommandText = $"""
UPDATE {UsersTable}
SET Name = {_dialect.Parameter("name")}
WHERE Id = 2
""";

            _updateUser2NameParameter = CreateParameter(command, "name", DbType.String);
            return command;
        }

        private static DbParameter CreateParameter(DbCommand command, string name, DbType dbType)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = dbType;
            parameter.Value = DBNull.Value;
            command.Parameters.Add(parameter);
            return parameter;
        }

        private static int ExecuteNonQuery(DbCommand command, DbTransaction? transaction)
        {
            var previousTransaction = command.Transaction;
            if (transaction is not null)
                command.Transaction = transaction;

            try
            {
                return command.ExecuteNonQuery();
            }
            finally
            {
                command.Transaction = previousTransaction;
            }
        }

        private static object? ExecuteScalar(DbCommand command, DbTransaction? transaction)
        {
            var previousTransaction = command.Transaction;
            if (transaction is not null)
                command.Transaction = transaction;

            try
            {
                return command.ExecuteScalar();
            }
            finally
            {
                command.Transaction = previousTransaction;
            }
        }

        private static int ExecuteNonQuery(DbConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQuery();
        }

        private BatchInsertPlan CreateBatchInsertPlan(int rowCount)
        {
            var command = connection.CreateCommand();
            var idParameters = new DbParameter[rowCount];
            var nameParameters = new DbParameter[rowCount];

            var sql = new StringBuilder()
                .AppendLine($"INSERT INTO {UsersTable} (")
                .AppendLine("    Id,")
                .AppendLine("    Name")
                .AppendLine(")")
                .AppendLine("VALUES");

            for (var i = 0; i < rowCount; i++)
            {
                var suffix = i.ToString(CultureInfo.InvariantCulture);
                var idName = $"id{suffix}";
                var nameName = $"name{suffix}";

                sql.Append("    (")
                    .Append(_dialect.Parameter(idName))
                    .Append(", ")
                    .Append(_dialect.Parameter(nameName))
                    .Append(')');
                sql.AppendLine(i < rowCount - 1 ? "," : string.Empty);

                idParameters[i] = CreateParameter(command, idName, DbType.Int32);
                nameParameters[i] = CreateParameter(command, nameName, DbType.String);
            }

            command.CommandText = sql.ToString();
            return new BatchInsertPlan(command, idParameters, nameParameters);
        }

        private sealed class BatchInsertPlan(DbCommand command, DbParameter[] idParameters, DbParameter[] nameParameters) : IDisposable
        {
            public DbCommand Command { get; } = command;
            public DbParameter[] IdParameters { get; } = idParameters;
            public DbParameter[] NameParameters { get; } = nameParameters;

            public void Dispose()
                => Command.Dispose();
        }
    }

    private sealed class PreparedTemporaryTableSourceState(
        DbConnection connection,
        TemporaryTableServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public TemporaryTableServiceTest<DbConnection> Service => service;

        public string Users => users;

        public string UId => uId;

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedTemporaryUsersState(
        DbConnection connection,
        TemporaryTableServiceTest<DbConnection> service,
        ProviderSqlDialect dialect,
        string users) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;

        public TemporaryTableServiceTest<DbConnection> Service => service;

        public string Users => users;

        public void Dispose()
        {
            try
            {
                service.DropScenario(users);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTemporaryUsersTable(users));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedSchemaSnapshotState(
        DbConnection connection,
        SchemaSnapshotServiceTest<DbConnection> service) : IDisposable
    {
        public SchemaSnapshotServiceTest<DbConnection> Service => service;

        public void Dispose()
        {
            connection.Dispose();
        }
    }

    private sealed class PreparedUsersQueryState(
        DbConnection connection,
        QueryServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public QueryServiceTest<DbConnection> Service => service;

        public string Users => users;

        public string UId => uId;

        public string UsersTable => $"{users}_{uId}";

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedUsersOrdersQueryState(
        DbConnection connection,
        QueryServiceTest<DbConnection> service,
        string users,
        string orders,
        string uId) : IDisposable
    {
        public QueryServiceTest<DbConnection> Service => service;

        public string Users => users;

        public string Orders => orders;

        public string UId => uId;

        public string UsersTable => $"{users}_{uId}";

        public string OrdersTable => $"{orders}_{uId}";

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(orders, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedExecutionPlanState(
        DbConnection connection,
        ExecutionPlanServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public ExecutionPlanServiceTest<DbConnection> Service => service;

        public string UsersTable => $"{users}_{uId}";

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedExecutionPlanJoinState(
        DbConnection connection,
        ExecutionPlanServiceTest<DbConnection> service,
        string users,
        string orders,
        string uId) : IDisposable
    {
        public ExecutionPlanServiceTest<DbConnection> Service => service;

        public string UsersTable => $"{users}_{uId}";

        public string OrdersTable => $"{orders}_{uId}";

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(orders, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedExecutionPlanDmlState(
        DbConnection connection,
        ExecutionPlanServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        private int _nextInsertId = 1;

        public object? RunExecutionPlanDml()
        {
            var value = service.RunExecutionPlanDml($"{users}_{uId}", _nextInsertId);
            _nextInsertId += 1;
            return value;
        }

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedDebugTraceSelectState(
        DbConnection connection,
        DebugTraceServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public DebugTraceServiceTest<DbConnection> Service => service;

        public string UsersTable => $"{users}_{uId}";

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

    private sealed class PreparedDebugTraceBatchState(
        DbConnection connection,
        DebugTraceServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public DebugTraceServiceTest<DbConnection> Service => service;

        public string UsersTable => $"{users}_{uId}";

        public void Dispose()
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(connection, service.Dialect.DropTable(users, uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }

}

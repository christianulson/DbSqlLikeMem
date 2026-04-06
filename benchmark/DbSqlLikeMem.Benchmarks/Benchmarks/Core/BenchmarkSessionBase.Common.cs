using DbSqlLikeMem.TestTools.Benchmarks;
using DbSqlLikeMem.TestTools.DDL;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Schema;
using DbSqlLikeMem.TestTools.TemporaryTable;
using DbSqlLikeMem.TestTools.Query;
using System.Globalization;
using System.Data;

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    private readonly object _preparedStateSync = new();
    private readonly Dictionary<string, IDisposable> _preparedStates = [];

    /// <summary>
    /// EN: Generates a unique temporary table name for the users table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de usuários usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary users table name. PT-br: Um nome único de tabela temporária de usuários.</returns>
    protected virtual string NewUsersTableName() => $"USR";

    /// <summary>
    /// EN: Generates a unique temporary table name for the orders table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de pedidos usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary orders table name. PT-br: Um nome único de tabela temporária de pedidos.</returns>
    protected virtual string NewOrdersTableName() => $"ORD";

    /// <summary>
    /// EN: Generates a unique temporary sequence name for sequence-based benchmark operations.
    /// PT-br: Gera um nome único de sequência temporária para operações de benchmark baseadas em sequência.
    /// </summary>
    /// <returns>EN: A unique temporary sequence name. PT-br: Um nome único de sequência temporária.</returns>
    protected virtual string NewSequenceName() => $"SEQ_{NextToken()}";

    protected virtual string NewSavepointName() => $"SP_{NextToken()}";

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
                var users = NewUsersTableName();
                var connection = CreateConnection();
                connection.Open();
                var service = CreateTemporaryTableService(
                    connection,
                    BenchmarkScenarioFactory.CreateTemporaryUsersScenario<DbConnection>(Dialect),
                    CreateConnection);
                service.CreateScenario(users);
                return new PreparedTemporaryUsersState(connection, service, Dialect, CreateConnection, users);
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

    protected static int CountReaderRows(
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

        using var reader = command.ExecuteReader();
        var count = 0;
        while (reader.Read())
        {
            count++;
        }

        return count;
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

        public int RunSequentialInsert(int rowCount)
        {
            var startId = _nextInsertId;
            var expectedCount = _rowCount + rowCount;
            var count = service.RunTest(users, uId, rowCount, startId, expectedCount);
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
            var affected = service.RunRowCountAfterInsert(users, uId, id);
            _nextInsertId += 1;
            _rowCount += 1;
            return affected;
        }

        public int RunParameterInsertSingle()
        {
            var id = _nextInsertId;
            var sql = $"""
INSERT INTO {UsersTable} (
    Id,
    Name
)
VALUES (
    {service.Dialect.Parameter("id")},
    {service.Dialect.Parameter("name")}
)
""";

            var affected = ExecuteNonQuery(connection, sql, command =>
            {
                AddParameter(command, "id", DbType.Int32, id);
                AddParameter(command, "name", DbType.String, $"User {id}");
            });

            _nextInsertId += 1;
            _rowCount += 1;
            return affected;
        }

        public (string firstName, string lastName) RunInsertCustomStartId()
        {
            try
            {
                service.RunTest(users, uId, 3, 10, 3);
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

        private static void AddParameter(DbCommand command, string name, DbType dbType, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;

            var parameterTypeName = parameter.GetType().FullName;
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

        private string UsersTable => $"{users}_{uId}";

        public string RunUpdateByPk()
        {
            try
            {
                return service.RunUpdateByPk(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(connection, _dialect.UpdateUserNameById(UsersTable, 1, "Alice"));
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
                return service.RunDeleteByPk(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 1, "Alice"));
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
                return service.RunRowCountAfterUpdate(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(connection, _dialect.UpdateUserNameById(UsersTable, 1, "Alice"));
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
                return service.RunUpsert(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(connection, _dialect.UpdateUserNameById(UsersTable, 1, "Alice"));
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
                    ExecuteNonQuery(connection, _dialect.DeleteUserById(UsersTable, 1));
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
                    ExecuteNonQuery(connection, _dialect.DeleteUserById(UsersTable, 1));
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
                    ExecuteNonQuery(connection, _dialect.DeleteUserById(UsersTable, 2));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(connection, _dialect.DeleteUserById(UsersTable, 1));
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

    private sealed class PreparedParameterProjectionState(
        DbConnection connection,
        ProviderSqlDialect dialect) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;

        public object? RunParameterProjection()
        {
            var sql = _dialect.SelectParameterProjection($@"
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

            return ExecuteScalar(connection, sql, command =>
            {
                AddParameter(command, "textValue", DbType.String, "benchmark");
                AddParameter(command, "ansiTextValue", DbType.AnsiString, "ansi");
                AddParameter(command, "ansiFixedTextValue", DbType.AnsiStringFixedLength, "fixed-ansi");
                AddParameter(command, "fixedTextValue", DbType.StringFixedLength, "fixed-text");
                AddParameter(command, "int16Value", DbType.Int16, (short)16);
                AddParameter(command, "int32Value", DbType.Int32, 32);
                AddParameter(command, "int64Value", DbType.Int64, 64L);
                AddParameter(command, "boolValue", DbType.Boolean, true);
                AddParameter(command, "decimalValue", DbType.Decimal, 12.34m);
                AddParameter(command, "doubleValue", DbType.Double, 56.78d);
                AddParameter(command, "timeSpanValue", DbType.Time, TimeSpan.FromHours(1.5));
                AddParameter(command, "dateTimeOffsetValue", DbType.DateTimeOffset, new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero));
                AddParameter(command, "dateTimeValue", DbType.DateTime, new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));
                AddParameter(command, "guidValue", DbType.Guid, Guid.Parse("11111111-2222-3333-4444-555555555555"));
                AddParameter(command, "binaryValue", DbType.Binary, new byte[] { 1, 2, 3, 4 });
                AddParameter(command, "dateValue", DbType.Date, new DateTime(2024, 1, 2));
                AddParameter(command, "currencyValue", DbType.Currency, 123.45m);
            });
        }

        private static void AddParameter(DbCommand command, string name, DbType dbType, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            var parameterTypeName = parameter.GetType().FullName;
            var isFirebirdParameter = parameterTypeName == "FirebirdSql.Data.FirebirdClient.FbParameter";
            var isDb2Parameter = parameterTypeName is "IBM.Data.Db2.DB2Parameter"
                or "IBM.Data.DB2.Core.DB2Parameter"
                or "IBM.Data.DB2.iSeries.iDB2Parameter";

            if (isFirebirdParameter && (dbType == DbType.Currency || dbType == DbType.DateTimeOffset))
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

        public void Dispose()
        {
            connection.Dispose();
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
            AddParameter(command, "resultCode", DbType.Int32, DBNull.Value, ParameterDirection.ReturnValue);

            var affected = command.ExecuteNonQuery();
            GC.KeepAlive(command.Parameters["counter"].Value);
            GC.KeepAlive(command.Parameters["message"].Value);
            GC.KeepAlive(command.Parameters["resultCode"].Value);
            return affected;
        }

        private static void AddParameter(
            DbCommand command,
            string name,
            DbType dbType,
            object? value,
            ParameterDirection direction)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = dbType;
            parameter.Direction = direction;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
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

        private string UsersTable => $"{users}_{uId}";

        public int RunReturningInsert()
        {
            try
            {
                return service.RunReturningInsert(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(connection, _dialect.DeleteUserById(UsersTable, 1));
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
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            var value = Convert.ToString(ExecuteScalar(connection, _dialect.SelectUserNameById(UsersTable, 1), transaction), CultureInfo.InvariantCulture);
            ExecuteNonQuery(connection, _dialect.UpdateUserNameById(UsersTable, 2, "Bob-v2"), transaction);
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
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            var count = Convert.ToInt32(ExecuteScalar(connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
            var second = Convert.ToString(ExecuteScalar(connection, _dialect.SelectUserNameById(UsersTable, 2), transaction), CultureInfo.InvariantCulture);
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
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            ExecuteNonQuery(connection, _dialect.UpdateUserNameById(UsersTable, 2, "Bob-v2"), transaction);
            ExecuteNonQuery(connection, _dialect.DeleteUserById(UsersTable, 1), transaction);
            var count = Convert.ToInt32(ExecuteScalar(connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
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
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            var first = Convert.ToInt32(ExecuteScalar(connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
            var second = ExecuteScalar(connection, _dialect.SelectUserNameById(UsersTable, 1), transaction);
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
            ExecuteNonQuery(connection, _dialect.InsertUsers(UsersTable, values), transaction);
            var count = Convert.ToInt32(ExecuteScalar(connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
            transaction.Rollback();

            if (count != rowCount)
            {
                throw new InvalidOperationException($"Expected {rowCount} rows for {_dialect.DisplayName}, got {count}.");
            }

            return count;
        }

        public int RunRowCountInBatch()
        {
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);

            var count = Convert.ToInt32(ExecuteScalar(connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
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
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            transaction.Commit();

            try
            {
                ExecuteNonQuery(connection, _dialect.DeleteUserById(UsersTable, 1));
                ExecuteNonQuery(connection, _dialect.DeleteUserById(UsersTable, 2));
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

    private sealed class PreparedTemporaryTableSourceState(
        DbConnection connection,
        TemporaryTableServiceTest<DbConnection> service,
        string users,
        string uId) : IDisposable
    {
        public List<int> RunCreateTemporaryTableAsSelectThenSelect()
        {
            var rows = service.RunCreateTemporaryTableAsSelectThenSelect(users, uId);
            return rows;
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

    private sealed class PreparedTemporaryUsersState(
        DbConnection connection,
        TemporaryTableServiceTest<DbConnection> service,
        ProviderSqlDialect dialect,
        Func<DbConnection> secondaryConnectionFactory,
        string users) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;

        public void RunTempTableRollback()
        {
            var usersTable = _dialect.TemporaryUsersTableName(users);
            using var tx = connection.BeginTransaction();
            ExecuteNonQuery(connection, $"INSERT INTO {usersTable} (Id, Name) VALUES (1, 'Alice')", tx);
            ExecuteNonQuery(connection, $"INSERT INTO {usersTable} (Id, Name) VALUES (2, 'Bob')", tx);
            tx.Rollback();

            var count = Convert.ToInt32(ExecuteScalar(connection, $"SELECT COUNT(*) FROM {usersTable}"), CultureInfo.InvariantCulture);
            if (count != 0)
            {
                throw new InvalidOperationException($"Unexpected temporary-table rollback rowcount for {_dialect.DisplayName}: {count}.");
            }
        }

        public int RunTemporaryTableCrossConnectionIsolation()
        {
            var usersTable = _dialect.TemporaryUsersTableName(users);
            using var tx = connection.BeginTransaction();
            ExecuteNonQuery(connection, $"INSERT INTO {usersTable} (Id, Name) VALUES (1, 'Alice')", tx);

            using var secondaryConnection = secondaryConnectionFactory();
            secondaryConnection.Open();

            try
            {
                var count = Convert.ToInt32(ExecuteScalar(secondaryConnection, _dialect.CountRows(usersTable)), CultureInfo.InvariantCulture);
                if (count != 0)
                {
                    throw new InvalidOperationException($"Unexpected temporary-table isolation rowcount for {_dialect.DisplayName}: {count}.");
                }

                tx.Rollback();
                return count;
            }
            catch (Exception ex)
            {
                tx.Rollback();
                if (IsMissingTemporaryTableException(ex))
                {
                    return 0;
                }

                throw;
            }
        }

        private static bool IsMissingTemporaryTableException(Exception ex)
        {
            if (ex is KeyNotFoundException)
            {
                return true;
            }

            var message = ex.GetBaseException().Message;
            return message.Contains("no such table", StringComparison.OrdinalIgnoreCase)
                || message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
                || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
                || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
                || message.Contains("invalid object name", StringComparison.OrdinalIgnoreCase)
                || message.Contains("not found", StringComparison.OrdinalIgnoreCase);
        }

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
        public DbConnection Connection => connection;

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
        public DbConnection Connection => connection;

        public QueryServiceTest<DbConnection> Service => service;

        public string Users => users;

        public string UId => uId;

        public string UsersTable => $"{users}_{uId}";

        public int RunBetweenLikeOrderByMatrix()
        {
            var count = CountReaderRows(
                connection,
                $"""
SELECT Name
FROM {UsersTable}
WHERE Id BETWEEN 1 AND 4
  AND Name LIKE 'A%'
ORDER BY Name
""");
            if (count != 2)
            {
                throw new InvalidOperationException($"Unexpected BETWEEN/LIKE/ORDER BY benchmark rowcount for {service.Dialect.DisplayName}: {count}.");
            }

            return count;
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

        public int RunSelectLeftJoinAntiJoin()
        {
            var count = Convert.ToInt32(
                ExecuteScalar(connection, service.Dialect.SelectLeftJoinAntiJoin(UsersTable, OrdersTable)),
                CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected LEFT JOIN anti-join benchmark count for {service.Dialect.DisplayName}: {count}.");
            }

            return count;
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
        private int _nextInsertId = 1;

        public object? RunDebugTraceBatch()
        {
            var usersTable = $"{users}_{uId}";
            var trace = service.RunDebugTraceBatch(usersTable, _nextInsertId, _nextInsertId + 1);
            _nextInsertId += 2;
            return trace;
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

}

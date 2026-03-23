using DbSqlLikeMem.TestTools.Benchmarks;
using DbSqlLikeMem.TestTools.DDL;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Schema;
using DbSqlLikeMem.TestTools.TemporaryTable;
using DbSqlLikeMem.TestTools.Query;
using System.Globalization;
using System.Collections.Generic;

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
                return new PreparedCreateSchemaState(connection, service, users, uId);
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

    private sealed class PreparedSelectByPkState : IDisposable
    {
        private readonly SelectByPKServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;

        public PreparedSelectByPkState(
            DbConnection connection,
            SelectByPKServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public SelectByPKServiceTest<DbConnection> Service => _service;

        public string Users => _users;

        public string UId => _uId;

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    var fallbackSql = _service.Dialect.DropTable(_users, _uId);
                    ExecuteNonQuery(_connection, fallbackSql);
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedSelectJoinState : IDisposable
    {
        private readonly DmlMutationServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _orders;
        private readonly string _uId;

        public PreparedSelectJoinState(
            DbConnection connection,
            DmlMutationServiceTest<DbConnection> service,
            string users,
            string orders,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _orders = orders;
            _uId = uId;
        }

        public DmlMutationServiceTest<DbConnection> Service => _service;

        public string Users => _users;

        public string Orders => _orders;

        public string UId => _uId;

        public string UsersTable => $"{_users}_{_uId}";

        public string OrdersTable => $"{_orders}_{_uId}";

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _orders, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_orders, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedCreateSchemaState : IDisposable
    {
        private readonly CreateTableServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;

        public PreparedCreateSchemaState(
            DbConnection connection,
            CreateTableServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public void RunCreateSchema()
        {
            try
            {
                _service.CreateScenario(_users, _uId);
                _service.RunTest(_users, _uId);
            }
            finally
            {
                try
                {
                    _service.DropScenario(_users, _uId);
                }
                catch
                {
                    SafeDropTable(_connection, _users, _uId);
                }
            }
        }

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    SafeDropTable(_connection, _users, _uId);
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedInsertUsersState : IDisposable
    {
        private readonly InsertUsersServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;
        private int _nextInsertId = 1;
        private int _rowCount;

        public PreparedInsertUsersState(
            DbConnection connection,
            InsertUsersServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public int RunSequentialInsert(int rowCount)
        {
            var startId = _nextInsertId;
            var expectedCount = _rowCount + rowCount;
            var count = _service.RunTest(_users, _uId, rowCount, startId, expectedCount);
            _nextInsertId += rowCount;
            _rowCount = expectedCount;
            return count;
        }

        public int RunParallelInsert(int rowCount)
        {
            var startId = _nextInsertId;
            var expectedCount = _rowCount + rowCount;
            var count = _service.RunParallelTest(_users, _uId, rowCount, startId, expectedCount);
            _nextInsertId += rowCount;
            _rowCount = expectedCount;
            return count;
        }

        public int RunRowCountAfterInsert()
        {
            var id = _nextInsertId;
            var affected = _service.RunRowCountAfterInsert(_users, _uId, id);
            _nextInsertId += 1;
            _rowCount += 1;
            return affected;
        }

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedCrudUsersState : IDisposable
    {
        private readonly DmlMutationServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly ProviderSqlDialect _dialect;
        private readonly string _users;
        private readonly string _uId;

        public PreparedCrudUsersState(
            DbConnection connection,
            DmlMutationServiceTest<DbConnection> service,
            ProviderSqlDialect dialect,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _dialect = dialect;
            _users = users;
            _uId = uId;
        }

        private string UsersTable => $"{_users}_{_uId}";

        public string RunUpdateByPk()
        {
            try
            {
                return _service.RunUpdateByPk(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(_connection, _dialect.UpdateUserNameById(UsersTable, 1, "Alice"));
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
                return _service.RunDeleteByPk(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 1, "Alice"));
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
                return _service.RunRowCountAfterUpdate(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(_connection, _dialect.UpdateUserNameById(UsersTable, 1, "Alice"));
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
                return _service.RunUpsert(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(_connection, _dialect.UpdateUserNameById(UsersTable, 1, "Alice"));
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
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedTransactionUsersState : IDisposable
    {
        private readonly DmlMutationServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly ProviderSqlDialect _dialect;
        private readonly string _users;
        private readonly string _uId;

        public PreparedTransactionUsersState(
            DbConnection connection,
            DmlMutationServiceTest<DbConnection> service,
            ProviderSqlDialect dialect,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _dialect = dialect;
            _users = users;
            _uId = uId;
        }

        private string UsersTable => $"{_users}_{_uId}";

        public int RunTransactionCommit()
        {
            try
            {
                return _service.RunTransactionCommit(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(_connection, _dialect.DeleteUserById(UsersTable, 1));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public int RunTransactionRollback()
            => _service.RunTransactionRollback(UsersTable);

        public int RunRollbackToSavepoint()
        {
            try
            {
                return _service.RunRollbackToSavepoint(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(_connection, _dialect.DeleteUserById(UsersTable, 1));
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
                return _service.RunNestedSavepointFlow(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(_connection, _dialect.DeleteUserById(UsersTable, 2));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(_connection, _dialect.DeleteUserById(UsersTable, 1));
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
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedNoopMutationState : IDisposable
    {
        private readonly DmlMutationServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;

        public PreparedNoopMutationState(
            DbConnection connection,
            DmlMutationServiceTest<DbConnection> service)
        {
            _connection = connection;
            _service = service;
        }

        public DmlMutationServiceTest<DbConnection> Service => _service;

        public void Dispose()
        {
            try
            {
                _service.DropScenario();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedNoopQueryState : IDisposable
    {
        private readonly QueryServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;

        public PreparedNoopQueryState(
            DbConnection connection,
            QueryServiceTest<DbConnection> service)
        {
            _connection = connection;
            _service = service;
        }

        public QueryServiceTest<DbConnection> Service => _service;

        public void Dispose()
        {
            try
            {
                _service.DropScenario();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedReturningInsertState : IDisposable
    {
        private readonly BatchServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly ProviderSqlDialect _dialect;
        private readonly string _users;
        private readonly string _uId;

        public PreparedReturningInsertState(
            DbConnection connection,
            BatchServiceTest<DbConnection> service,
            ProviderSqlDialect dialect,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _dialect = dialect;
            _users = users;
            _uId = uId;
        }

        private string UsersTable => $"{_users}_{_uId}";

        public int RunReturningInsert()
        {
            try
            {
                return _service.RunReturningInsert(UsersTable);
            }
            finally
            {
                try
                {
                    ExecuteNonQuery(_connection, _dialect.DeleteUserById(UsersTable, 1));
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
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedSequenceState : IDisposable
    {
        private readonly DmlMutationServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _sequence;

        public PreparedSequenceState(
            DbConnection connection,
            DmlMutationServiceTest<DbConnection> service,
            string sequence)
        {
            _connection = connection;
            _service = service;
            _sequence = sequence;
        }

        public object? RunSequenceNextValue()
            => _service.RunSequenceNextValue(_sequence);

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_sequence);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropSequence(_sequence));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedBatchUsersState : IDisposable
    {
        private readonly BatchServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly ProviderSqlDialect _dialect;
        private readonly string _users;
        private readonly string _uId;
        private readonly (int id, string name)[] _batch10Values;
        private readonly (int id, string name)[] _batch100Values;

        public PreparedBatchUsersState(
            DbConnection connection,
            BatchServiceTest<DbConnection> service,
            ProviderSqlDialect dialect,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _dialect = dialect;
            _users = users;
            _uId = uId;
            _batch10Values = CreateBatchValues(10);
            _batch100Values = CreateBatchValues(100);
        }

        private string UsersTable => $"{_users}_{_uId}";

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
            using var transaction = _connection.BeginTransaction();
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            var value = Convert.ToString(ExecuteScalar(_connection, _dialect.SelectUserNameById(UsersTable, 1), transaction), CultureInfo.InvariantCulture);
            ExecuteNonQuery(_connection, _dialect.UpdateUserNameById(UsersTable, 2, "Bob-v2"), transaction);
            transaction.Rollback();

            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected mixed-batch read result for {_dialect.DisplayName}: {value ?? "<null>"}.");
            }

            return value!;
        }

        public string RunBatchScalar()
        {
            using var transaction = _connection.BeginTransaction();
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            var count = Convert.ToInt32(ExecuteScalar(_connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
            var second = Convert.ToString(ExecuteScalar(_connection, _dialect.SelectUserNameById(UsersTable, 2), transaction), CultureInfo.InvariantCulture);
            transaction.Rollback();

            if (count != 2 || !string.Equals(second, "Bob", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected scalar batch result for {_dialect.DisplayName}: count={count}, second={second ?? "<null>"}.");
            }

            return second!;
        }

        public int RunBatchNonQuery()
        {
            using var transaction = _connection.BeginTransaction();
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            ExecuteNonQuery(_connection, _dialect.UpdateUserNameById(UsersTable, 2, "Bob-v2"), transaction);
            ExecuteNonQuery(_connection, _dialect.DeleteUserById(UsersTable, 1), transaction);
            var count = Convert.ToInt32(ExecuteScalar(_connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
            transaction.Rollback();

            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected non-query batch count for {_dialect.DisplayName}: {count}.");
            }

            return count;
        }

        public object? RunBatchReaderMultiResult()
        {
            using var transaction = _connection.BeginTransaction();
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            var first = Convert.ToInt32(ExecuteScalar(_connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
            var second = ExecuteScalar(_connection, _dialect.SelectUserNameById(UsersTable, 1), transaction);
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
            using var transaction = _connection.BeginTransaction();
            var values = rowCount == 10 ? _batch10Values : rowCount == 100 ? _batch100Values : CreateBatchValues(rowCount);
            ExecuteNonQuery(_connection, _dialect.InsertUsers(UsersTable, values), transaction);
            var count = Convert.ToInt32(ExecuteScalar(_connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
            transaction.Rollback();

            if (count != rowCount)
            {
                throw new InvalidOperationException($"Expected {rowCount} rows for {_dialect.DisplayName}, got {count}.");
            }

            return count;
        }

        public int RunRowCountInBatch()
        {
            using var transaction = _connection.BeginTransaction();
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);

            var count = Convert.ToInt32(ExecuteScalar(_connection, _dialect.CountRows(UsersTable), transaction), CultureInfo.InvariantCulture);
            transaction.Rollback();

            if (count != 2)
            {
                throw new InvalidOperationException($"Unexpected batch rowcount for {_dialect.DisplayName}: {count}.");
            }

            return count;
        }

        public string RunBatchTransactionControl()
        {
            using var transaction = _connection.BeginTransaction();
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 1, "Alice"), transaction);
            ExecuteNonQuery(_connection, _dialect.InsertUser(UsersTable, 2, "Bob"), transaction);
            transaction.Commit();

            try
            {
                ExecuteNonQuery(_connection, _dialect.DeleteUserById(UsersTable, 1));
                ExecuteNonQuery(_connection, _dialect.DeleteUserById(UsersTable, 2));
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
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedTemporaryTableSourceState : IDisposable
    {
        private readonly TemporaryTableServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;

        public PreparedTemporaryTableSourceState(
            DbConnection connection,
            TemporaryTableServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public List<int> RunCreateTemporaryTableAsSelectThenSelect()
        {
            var rows = _service.RunCreateTemporaryTableAsSelectThenSelect(_users, _uId);
            return rows;
        }

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedTemporaryUsersState : IDisposable
    {
        private readonly TemporaryTableServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly ProviderSqlDialect _dialect;
        private readonly Func<DbConnection> _secondaryConnectionFactory;
        private readonly string _users;

        public PreparedTemporaryUsersState(
            DbConnection connection,
            TemporaryTableServiceTest<DbConnection> service,
            ProviderSqlDialect dialect,
            Func<DbConnection> secondaryConnectionFactory,
            string users)
        {
            _connection = connection;
            _service = service;
            _dialect = dialect;
            _secondaryConnectionFactory = secondaryConnectionFactory;
            _users = users;
        }

        public void RunTempTableRollback()
        {
            var usersTable = _dialect.TemporaryUsersTableName(_users);
            using var tx = _connection.BeginTransaction();
            ExecuteNonQuery(_connection, $"INSERT INTO {usersTable} (Id, Name) VALUES (1, 'Alice')", tx);
            ExecuteNonQuery(_connection, $"INSERT INTO {usersTable} (Id, Name) VALUES (2, 'Bob')", tx);
            tx.Rollback();

            var count = Convert.ToInt32(ExecuteScalar(_connection, $"SELECT COUNT(*) FROM {usersTable}"), CultureInfo.InvariantCulture);
            if (count != 0)
            {
                throw new InvalidOperationException($"Unexpected temporary-table rollback rowcount for {_dialect.DisplayName}: {count}.");
            }
        }

        public int RunTemporaryTableCrossConnectionIsolation()
        {
            var usersTable = _dialect.TemporaryUsersTableName(_users);
            using var tx = _connection.BeginTransaction();
            ExecuteNonQuery(_connection, $"INSERT INTO {usersTable} (Id, Name) VALUES (1, 'Alice')", tx);

            using var secondaryConnection = _secondaryConnectionFactory();
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
                _service.DropScenario(_users);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTemporaryUsersTable(_users));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedSchemaSnapshotState : IDisposable
    {
        private readonly DbConnection _connection;
        private readonly SchemaSnapshotServiceTest<DbConnection> _service;

        public PreparedSchemaSnapshotState(
            DbConnection connection,
            SchemaSnapshotServiceTest<DbConnection> service)
        {
            _connection = connection;
            _service = service;
        }

        public DbConnection Connection => _connection;

        public SchemaSnapshotServiceTest<DbConnection> Service => _service;

        public void Dispose()
        {
            _connection.Dispose();
        }
    }

    private sealed class PreparedUsersQueryState : IDisposable
    {
        private readonly QueryServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;

        public PreparedUsersQueryState(
            DbConnection connection,
            QueryServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public QueryServiceTest<DbConnection> Service => _service;

        public string Users => _users;

        public string UId => _uId;

        public string UsersTable => $"{_users}_{_uId}";

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedUsersOrdersQueryState : IDisposable
    {
        private readonly QueryServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _orders;
        private readonly string _uId;

        public PreparedUsersOrdersQueryState(
            DbConnection connection,
            QueryServiceTest<DbConnection> service,
            string users,
            string orders,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _orders = orders;
            _uId = uId;
        }

        public QueryServiceTest<DbConnection> Service => _service;

        public string Users => _users;

        public string Orders => _orders;

        public string UId => _uId;

        public string UsersTable => $"{_users}_{_uId}";

        public string OrdersTable => $"{_orders}_{_uId}";

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _orders, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_orders, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedExecutionPlanState : IDisposable
    {
        private readonly ExecutionPlanServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;

        public PreparedExecutionPlanState(
            DbConnection connection,
            ExecutionPlanServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public ExecutionPlanServiceTest<DbConnection> Service => _service;

        public string UsersTable => $"{_users}_{_uId}";

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedExecutionPlanJoinState : IDisposable
    {
        private readonly ExecutionPlanServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _orders;
        private readonly string _uId;

        public PreparedExecutionPlanJoinState(
            DbConnection connection,
            ExecutionPlanServiceTest<DbConnection> service,
            string users,
            string orders,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _orders = orders;
            _uId = uId;
        }

        public ExecutionPlanServiceTest<DbConnection> Service => _service;

        public string UsersTable => $"{_users}_{_uId}";

        public string OrdersTable => $"{_orders}_{_uId}";

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _orders, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_orders, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedExecutionPlanDmlState : IDisposable
    {
        private readonly ExecutionPlanServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;
        private int _nextInsertId = 1;

        public PreparedExecutionPlanDmlState(
            DbConnection connection,
            ExecutionPlanServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public object? RunExecutionPlanDml()
        {
            var value = _service.RunExecutionPlanDml($"{_users}_{_uId}", _nextInsertId);
            _nextInsertId += 1;
            return value;
        }

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedDebugTraceSelectState : IDisposable
    {
        private readonly DebugTraceServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;

        public PreparedDebugTraceSelectState(
            DbConnection connection,
            DebugTraceServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public DebugTraceServiceTest<DbConnection> Service => _service;

        public string UsersTable => $"{_users}_{_uId}";

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

    private sealed class PreparedDebugTraceBatchState : IDisposable
    {
        private readonly DebugTraceServiceTest<DbConnection> _service;
        private readonly DbConnection _connection;
        private readonly string _users;
        private readonly string _uId;
        private int _nextInsertId = 1;

        public PreparedDebugTraceBatchState(
            DbConnection connection,
            DebugTraceServiceTest<DbConnection> service,
            string users,
            string uId)
        {
            _connection = connection;
            _service = service;
            _users = users;
            _uId = uId;
        }

        public object? RunDebugTraceBatch()
        {
            var usersTable = $"{_users}_{_uId}";
            var trace = _service.RunDebugTraceBatch(usersTable, _nextInsertId, _nextInsertId + 1);
            _nextInsertId += 2;
            return trace;
        }

        public void Dispose()
        {
            try
            {
                _service.DropScenario(_users, _uId);
            }
            catch
            {
                try
                {
                    ExecuteNonQuery(_connection, _service.Dialect.DropTable(_users, _uId));
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
            finally
            {
                _connection.Dispose();
            }
        }
    }

}

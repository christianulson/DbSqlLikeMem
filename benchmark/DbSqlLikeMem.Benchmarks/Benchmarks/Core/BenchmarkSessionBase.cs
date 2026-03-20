using IBM.Data.Db2;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System.Collections.Concurrent;
using System.Globalization;

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the common benchmark workflow shared by provider-specific benchmark sessions.
/// PT-br: Fornece o fluxo de benchmark comum compartilhado pelas sessões de benchmark específicas de cada provedor.
/// </summary>
/// <remarks>
/// EN: Derived types supply the connection factory and, when needed, override individual benchmark routines.
/// PT-br: Tipos derivados fornecem a fábrica de conexões e, quando necessário, sobrescrevem rotinas individuais de benchmark.
/// </remarks>
/// <param name="dialect">EN: The provider-specific SQL dialect used to generate benchmark commands. PT-br: O dialeto SQL específico do provedor usado para gerar os comandos de benchmark.</param>
/// <param name="engine">EN: The benchmark engine that identifies the runtime behind the session. PT-br: O mecanismo de benchmark que identifica o runtime por trás da sessão.</param>
public abstract class BenchmarkSessionBase(
    ProviderSqlDialect dialect,
    BenchmarkEngine engine
    ) : IBenchmarkSession
{
    private static int _objectCounter;

    /// <summary>
    /// EN: Gets the SQL dialect abstraction used to generate provider-specific statements for the current session.
    /// PT-br: Obtém a abstração de dialeto SQL usada para gerar comandos específicos do provedor para a sessão atual.
    /// </summary>
    protected ProviderSqlDialect Dialect { get; } = dialect;

    /// <summary>
    /// EN: Gets the provider identifier exposed by the current dialect.
    /// PT-br: Obtém o identificador do provedor exposto pelo dialeto atual.
    /// </summary>
    public BenchmarkProviderId Provider => Dialect.Provider;

    /// <summary>
    /// EN: Gets the benchmark engine used by the current session.
    /// PT-br: Obtém o mecanismo de benchmark usado pela sessão atual.
    /// </summary>
    public BenchmarkEngine Engine { get; } = engine;

    /// <summary>
    /// EN: Performs any session initialization required before the benchmarks start.
    /// PT-br: Executa a inicialização necessária da sessão antes do início dos benchmarks.
    /// </summary>
    public virtual void Initialize()
    {
    }

    public virtual void Execute(BenchmarkFeatureId feature)
    {
        try
        {
            RunFeature(feature);
        }
        catch (InvalidOperationException ex)
        {
            LogBenchmarkIssue("NA-IOE", feature, ex);
        }
        catch (NotSupportedException ex)
        {
            LogBenchmarkIssue("NA-NSE", feature, ex);
        }
        catch (DB2Exception ex)
        {
            LogBenchmarkIssue("NA-DB2E", feature, ex);
        }
        catch (SqlException ex)
        {
            LogBenchmarkIssue("NA-SqlE", feature, ex);
        }
        catch (MySqlException ex)
        {
            LogBenchmarkIssue("NA-MSE", feature, ex);
        }
        catch (NpgsqlException ex)
        {
            LogBenchmarkIssue("NA-NE", feature, ex);
        }
        catch (OracleException ex)
        {
            LogBenchmarkIssue("NA-OE", feature, ex);
        }
        catch (Exception ex)
        {
            LogBenchmarkIssue("NA", feature, ex);
        }
    }

    /// <summary>
    /// EN: Dispatches the requested benchmark feature to the corresponding benchmark routine.
    /// PT-br: Encaminha o recurso de benchmark solicitado para a rotina de benchmark correspondente.
    /// </summary>
    /// <param name="feature">EN: The benchmark feature to execute. PT-br: O recurso de benchmark a ser executado.</param>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public void RunFeature(BenchmarkFeatureId feature)
    {
        switch (feature)
        {
            case BenchmarkFeatureId.ConnectionOpen:
                RunConnectionOpen();
                break;
            case BenchmarkFeatureId.CreateSchema:
                RunCreateSchema();
                break;
            case BenchmarkFeatureId.InsertSingle:
                RunInsertSingle();
                break;
            case BenchmarkFeatureId.InsertBatch10:
                RunInsertBatch10();
                break;
            case BenchmarkFeatureId.InsertBatch100:
                RunInsertBatch100();
                break;
            case BenchmarkFeatureId.InsertBatch100Parallel:
                RunInsertBatch100Parallel();
                break;
            case BenchmarkFeatureId.SelectByPk:
                RunSelectByPk();
                break;
            case BenchmarkFeatureId.SelectJoin:
                RunSelectJoin();
                break;
            case BenchmarkFeatureId.UpdateByPk:
                RunUpdateByPk();
                break;
            case BenchmarkFeatureId.DeleteByPk:
                RunDeleteByPk();
                break;
            case BenchmarkFeatureId.TransactionCommit:
                RunTransactionCommit();
                break;
            case BenchmarkFeatureId.TransactionRollback:
                RunTransactionRollback();
                break;
            case BenchmarkFeatureId.SavepointCreate:
                RunSavepointCreate();
                break;
            case BenchmarkFeatureId.RollbackToSavepoint:
                RunRollbackToSavepoint();
                break;
            case BenchmarkFeatureId.ReleaseSavepoint:
                RunReleaseSavepoint();
                break;
            case BenchmarkFeatureId.NestedSavepointFlow:
                RunNestedSavepointFlow();
                break;
            case BenchmarkFeatureId.Upsert:
                RunUpsert();
                break;
            case BenchmarkFeatureId.SequenceNextValue:
                RunSequenceNextValue();
                break;
            case BenchmarkFeatureId.BatchInsert10:
                RunBatchInsert10();
                break;
            case BenchmarkFeatureId.BatchInsert100:
                RunBatchInsert100();
                break;
            case BenchmarkFeatureId.BatchMixedReadWrite:
                RunBatchMixedReadWrite();
                break;
            case BenchmarkFeatureId.BatchScalar:
                RunBatchScalar();
                break;
            case BenchmarkFeatureId.BatchNonQuery:
                RunBatchNonQuery();
                break;
            case BenchmarkFeatureId.StringAggregate:
                RunStringAggregate();
                break;
            case BenchmarkFeatureId.StringAggregateOrdered:
                RunStringAggregateOrdered();
                break;
            case BenchmarkFeatureId.StringAggregateDistinct:
                RunStringAggregateDistinct();
                break;
            case BenchmarkFeatureId.StringAggregateCustomSeparator:
                RunStringAggregateCustomSeparator();
                break;
            case BenchmarkFeatureId.StringAggregateLargeGroup:
                RunStringAggregateLargeGroup();
                break;
            case BenchmarkFeatureId.DateScalar:
                RunDateScalar();
                break;
            case BenchmarkFeatureId.JsonScalarRead:
                RunJsonScalarRead();
                break;
            case BenchmarkFeatureId.JsonPathRead:
                RunJsonPathRead();
                break;
            case BenchmarkFeatureId.TemporalCurrentTimestamp:
                RunTemporalCurrentTimestamp();
                break;
            case BenchmarkFeatureId.TemporalDateAdd:
                RunTemporalDateAdd();
                break;
            case BenchmarkFeatureId.TemporalNowWhere:
                RunTemporalNowWhere();
                break;
            case BenchmarkFeatureId.TemporalNowOrderBy:
                RunTemporalNowOrderBy();
                break;
            case BenchmarkFeatureId.RowCountAfterInsert:
                RunRowCountAfterInsert();
                break;
            case BenchmarkFeatureId.RowCountAfterUpdate:
                RunRowCountAfterUpdate();
                break;
            case BenchmarkFeatureId.RowCountAfterSelect:
                RunRowCountAfterSelect();
                break;
            case BenchmarkFeatureId.CteSimple:
                RunCteSimple();
                break;
            case BenchmarkFeatureId.WindowRowNumber:
                RunWindowRowNumber();
                break;
            case BenchmarkFeatureId.WindowLag:
                RunWindowLag();
                break;
            case BenchmarkFeatureId.BatchReaderMultiResult:
                RunBatchReaderMultiResult();
                break;
            case BenchmarkFeatureId.BatchTransactionControl:
                RunBatchTransactionControl();
                break;
            case BenchmarkFeatureId.ParseSimpleSelect:
                RunParseSimpleSelect();
                break;
            case BenchmarkFeatureId.ParseComplexJoin:
                RunParseComplexJoin();
                break;
            case BenchmarkFeatureId.ParseInsertReturning:
                RunParseInsertReturning();
                break;
            case BenchmarkFeatureId.ParseOnConflictDoUpdate:
                RunParseOnConflictDoUpdate();
                break;
            case BenchmarkFeatureId.ParseJsonExtract:
                RunParseJsonExtract();
                break;
            case BenchmarkFeatureId.ParseStringAggregateWithinGroup:
                RunParseStringAggregateWithinGroup();
                break;
            case BenchmarkFeatureId.ParseAutoDialectTopLimitFetch:
                RunParseAutoDialectTopLimitFetch();
                break;
            case BenchmarkFeatureId.ParseMultiStatementBatch:
                RunParseMultiStatementBatch();
                break;
            case BenchmarkFeatureId.JsonInsertCast:
                RunJsonInsertCast();
                break;
            case BenchmarkFeatureId.RowCountInBatch:
                RunRowCountInBatch();
                break;
            case BenchmarkFeatureId.PivotCount:
                RunPivotCount();
                break;
            case BenchmarkFeatureId.ReturningInsert:
                RunReturningInsert();
                break;
            case BenchmarkFeatureId.ReturningUpdate:
                RunReturningUpdate();
                break;
            case BenchmarkFeatureId.MergeBasic:
                RunMergeBasic();
                break;
            case BenchmarkFeatureId.PartitionPruningSelect:
                RunPartitionPruningSelect();
                break;
            case BenchmarkFeatureId.ExecutionPlan:
                RunExecutionPlan();
                break;
            case BenchmarkFeatureId.ExecutionPlanSelect:
                RunExecutionPlanSelect();
                break;
            case BenchmarkFeatureId.ExecutionPlanJoin:
                RunExecutionPlanJoin();
                break;
            case BenchmarkFeatureId.ExecutionPlanDml:
                RunExecutionPlanDml();
                break;
            case BenchmarkFeatureId.DebugTraceSelect:
                RunDebugTraceSelect();
                break;
            case BenchmarkFeatureId.DebugTraceBatch:
                RunDebugTraceBatch();
                break;
            case BenchmarkFeatureId.DebugTraceJson:
                RunDebugTraceJson();
                break;
            case BenchmarkFeatureId.LastExecutionPlansHistory:
                RunLastExecutionPlansHistory();
                break;
            case BenchmarkFeatureId.TempTableCreateAndUse:
                RunTempTableCreateAndUse();
                break;
            case BenchmarkFeatureId.TempTableRollback:
                RunTempTableRollback();
                break;
            case BenchmarkFeatureId.TempTableCrossConnectionIsolation:
                RunTempTableCrossConnectionIsolation();
                break;
            case BenchmarkFeatureId.ResetVolatileData:
                RunResetVolatileData();
                break;
            case BenchmarkFeatureId.ResetAllVolatileData:
                RunResetAllVolatileData();
                break;
            case BenchmarkFeatureId.ConnectionReopenAfterClose:
                RunConnectionReopenAfterClose();
                break;
            case BenchmarkFeatureId.SchemaSnapshotExport:
                RunSchemaSnapshotExport();
                break;
            case BenchmarkFeatureId.SchemaSnapshotToJson:
                RunSchemaSnapshotToJson();
                break;
            case BenchmarkFeatureId.SchemaSnapshotLoadJson:
                RunSchemaSnapshotLoadJson();
                break;
            case BenchmarkFeatureId.SchemaSnapshotApply:
                RunSchemaSnapshotApply();
                break;
            case BenchmarkFeatureId.SchemaSnapshotRoundTrip:
                RunSchemaSnapshotRoundTrip();
                break;
            case BenchmarkFeatureId.SchemaSnapshotCompare:
                RunSchemaSnapshotCompare();
                break;
            case BenchmarkFeatureId.FluentSchemaBuild:
                RunFluentSchemaBuild();
                break;
            case BenchmarkFeatureId.FluentSeed100:
                RunFluentSeed100();
                break;
            case BenchmarkFeatureId.FluentSeed1000:
                RunFluentSeed1000();
                break;
            case BenchmarkFeatureId.FluentScenarioCompose:
                RunFluentScenarioCompose();
                break;
            case BenchmarkFeatureId.MultiJoinAggregate:
                RunMultiJoinAggregate();
                break;
            case BenchmarkFeatureId.UnionAllProjection:
                RunUnionAllProjection();
                break;
            case BenchmarkFeatureId.GroupByHaving:
                RunGroupByHaving();
                break;
            case BenchmarkFeatureId.SelectCorrelatedCount:
                RunSelectCorrelatedCount();
                break;
            case BenchmarkFeatureId.SelectExistsPredicate:
                RunSelectExistsPredicate();
                break;
            case BenchmarkFeatureId.SelectScalarSubquery:
                RunSelectScalarSubquery();
                break;
            case BenchmarkFeatureId.DistinctProjection:
                RunDistinctProjection();
                break;
            case BenchmarkFeatureId.SelectInSubquery:
                RunSelectInSubquery();
                break;
            case BenchmarkFeatureId.OuterApplyProjection:
                RunOuterApplyProjection();
                break;
            case BenchmarkFeatureId.CrossApplyProjection:
                RunCrossApplyProjection();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(feature), feature, null);
        }
    }

    private static readonly object _logSync = new();

    private static readonly ConcurrentDictionary<string, int> Errors = [];

    protected virtual void LogBenchmarkIssue(string txt, BenchmarkFeatureId feature, Exception ex)
    {
        var root = ex.GetBaseException();
        var message = $"[{txt}-{root.GetType().Name}] {feature}: {root.Message} -- {ex.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        Console.WriteLine(message);

        lock (_logSync)
        {
            if (Errors.TryGetValue(root.Message, out int value)) {
                Errors[root.Message] = value + 1;
                return;
            }
            Errors.GetOrAdd(root.Message, 0);

            var file = Path.Combine("Logs", $"{GetType().Namespace}-errors.log");
            if (!File.Exists(file))
                File.Create(file).Dispose();
            File.AppendAllText(
                file,
                message + Environment.NewLine);
        }
    }

    /// <summary>
    /// EN: Releases any resources allocated by the benchmark session.
    /// PT-br: Libera os recursos alocados pela sessão de benchmark.
    /// </summary>
    public virtual void Dispose()
    {
    }

    /// <summary>
    /// EN: Creates a new provider-specific connection instance for the current benchmark session.
    /// PT-br: Cria uma nova instância de conexão específica do provedor para a sessão de benchmark atual.
    /// </summary>
    /// <returns>EN: A new provider-specific connection instance. PT-br: Uma nova instância de conexão específica do provedor.</returns>
    protected abstract DbConnection CreateConnection();

    /// <summary>
    /// EN: Generates a unique temporary table name for the users table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de usuários usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary users table name. PT-br: Um nome único de tabela temporária de usuários.</returns>
    protected virtual string NewUsersTableName() => $"USR_{NextToken()}";

    /// <summary>
    /// EN: Generates a unique temporary table name for the orders table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de pedidos usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary orders table name. PT-br: Um nome único de tabela temporária de pedidos.</returns>
    protected virtual string NewOrdersTableName() => $"ORD_{NextToken()}";

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

    /// <summary>
    /// EN: Creates the benchmark tables and then removes them as part of the schema creation measurement.
    /// PT-br: Cria as tabelas de benchmark e depois as remove como parte da medição de criação de esquema.
    /// </summary>
    protected virtual void RunCreateSchema()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();

        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Inserts a single user row and validates that the row was persisted.
    /// PT-br: Insere uma única linha de usuário e valida que a linha foi persistida.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertSingle()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Expected 1 row for {Dialect.DisplayName}, got {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunInsertBatch10()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            for (var i = 1; i <= 10; i++)
            {
                ExecuteNonQuery(connection, Dialect.InsertUser(users, i, $"User-{i}"));
            }

            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 10)
            {
                throw new InvalidOperationException($"Expected 10 rows for {Dialect.DisplayName}, got {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Inserts one hundred user rows and validates the final row count.
    /// PT-br: Insere cem linhas de usuário e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            for (var i = 1; i <= 100; i++)
            {
                ExecuteNonQuery(connection, Dialect.InsertUser(users, i, $"User-{i}"));
            }

            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 100)
            {
                throw new InvalidOperationException($"Expected 100 rows for {Dialect.DisplayName}, got {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Inserts one hundred user rows and validates the final row count.
    /// PT-br: Insere cem linhas de usuário e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100Parallel()
    {
        var users = NewUsersTableName();

        using var setupConnection = CreateConnection();
        setupConnection.Open();

        try
        {
            ExecuteNonQuery(setupConnection, Dialect.CreateUsersTable(users));

            var tasks = Enumerable.Range(1, 100)
                .Select(i => Task.Run(() =>
                {
                    using var connection = CreateConnection();
                    connection.Open();
                    ExecuteNonQuery(connection, Dialect.InsertUser(users, i, $"User-{i}"));
                }))
                .ToArray();

            Task.WhenAll(tasks).GetAwaiter().GetResult();

            var count = Convert.ToInt32(
                ExecuteScalar(setupConnection, Dialect.CountRows(users)),
                CultureInfo.InvariantCulture);

            if (count != 100)
            {
                LogBenchmarkIssue(
                    "NA",
                    BenchmarkFeatureId.InsertBatch100Parallel,
                    new InvalidOperationException($"Expected 100 rows for {Dialect.DisplayName}, got {count}."));
                return;
            }

            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(setupConnection, users);
        }
    }

    /// <summary>
    /// EN: Reads a user name by primary key and validates the returned value.
    /// PT-br: Lê um nome de usuário pela chave primária e valida o valor retornado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectByPk()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected select result for {Dialect.DisplayName}: {value ?? "<null>"}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes a join query between users and orders and validates the resulting count.
    /// PT-br: Executa uma consulta com junção entre usuários e pedidos e valida a contagem resultante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectJoin()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertOrder(orders, 10, 1, "A"));
            ExecuteNonQuery(connection, Dialect.InsertOrder(orders, 11, 1, "B"));
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountJoinForUser(users, orders, 1)), CultureInfo.InvariantCulture);
            if (value != 2)
            {
                throw new InvalidOperationException($"Unexpected join count for {Dialect.DisplayName}: {value}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Updates a user row by primary key and validates the stored value.
    /// PT-br: Atualiza uma linha de usuário pela chave primária e valida o valor armazenado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpdateByPk()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.UpdateUserNameById(users, 1, "Alice-v2"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
            if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected update result for {Dialect.DisplayName}: {value ?? "<null>"}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Deletes a user row by primary key and validates the remaining row count.
    /// PT-br: Exclui uma linha de usuário pela chave primária e valida a contagem de linhas restante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunDeleteByPk()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"));
            ExecuteNonQuery(connection, Dialect.DeleteUserById(users, 1));
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected delete count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, commits it, and validates the committed result.
    /// PT-br: Executa uma inserção dentro de uma transação, confirma a operação e valida o resultado confirmado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionCommit()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            transaction.Commit();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected commit count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, rolls it back, and validates that no rows remain.
    /// PT-br: Executa uma inserção dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionRollback()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            transaction.Rollback();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 0)
            {
                throw new InvalidOperationException($"Unexpected rollback count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunSavepointCreate()
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        using var connection = CreateConnection();
        connection.Open();
        using var transaction = connection.BeginTransaction();
        var savepoint = NewSavepointName();
        ExecuteNonQuery(connection, Dialect.Savepoint(savepoint), transaction);
        transaction.Rollback();
        GC.KeepAlive(savepoint);
    }

    protected virtual void RunRollbackToSavepoint()
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            var savepoint = NewSavepointName();
            ExecuteNonQuery(connection, Dialect.Savepoint(savepoint), transaction);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), transaction);
            ExecuteNonQuery(connection, Dialect.RollbackToSavepoint(savepoint), transaction);
            transaction.Commit();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected rollback-to-savepoint count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunReleaseSavepoint()
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        using var connection = CreateConnection();
        connection.Open();
        using var transaction = connection.BeginTransaction();
        var savepoint = NewSavepointName();
        ExecuteNonQuery(connection, Dialect.Savepoint(savepoint), transaction);
        ExecuteNonQuery(connection, Dialect.ReleaseSavepoint(savepoint), transaction);
        transaction.Rollback();
        GC.KeepAlive(savepoint);
    }

    /// <summary>
    /// EN: Executes the provider-specific upsert path and validates the updated value.
    /// PT-br: Executa o caminho de upsert específico do provedor e valida o valor atualizado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpsert()
    {
        if (!Dialect.SupportsUpsert)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the upsert benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.Upsert(users, 1, "Alice-v2"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
            if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected upsert result for {Dialect.DisplayName}: {value ?? "<null>"}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Creates a temporary sequence and reads its next value.
    /// PT-br: Cria uma sequência temporária e lê o seu próximo valor.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunSequenceNextValue()
    {
        if (!Dialect.SupportsSequence)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the sequence benchmark.");
        }

        var sequence = NewSequenceName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateSequence(sequence));
            var value = ExecuteScalar(connection, Dialect.NextSequenceValue(sequence));
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropSequence(connection, sequence);
        }
    }

    protected virtual void RunBatchInsert10()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUsers(users, [.. Enumerable.Range(1, 10).Select(i => (id:i,name: $"User-{i}"))]), transaction);
            
            transaction.Commit();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 10)
            {
                throw new InvalidOperationException($"Expected 10 rows for {Dialect.DisplayName}, got {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunBatchInsert100()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUsers(users, [.. Enumerable.Range(1, 100).Select(i => (id: i, name: $"User-{i}"))]), transaction);
            
            transaction.Commit();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 100)
            {
                throw new InvalidOperationException($"Expected 100 rows for {Dialect.DisplayName}, got {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunBatchMixedReadWrite()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), transaction);
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1), transaction), CultureInfo.InvariantCulture);
            ExecuteNonQuery(connection, Dialect.UpdateUserNameById(users, 2, "Bob-v2"), transaction);
            transaction.Commit();
            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected mixed-batch read result for {Dialect.DisplayName}: {value ?? "<null>"}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunBatchScalar()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), transaction);
            transaction.Commit();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            var second = Convert.ToString(ExecuteScalar(connection, Dialect.SelectUserNameById(users, 2)), CultureInfo.InvariantCulture);
            if (count != 2 || !string.Equals(second, "Bob", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected scalar batch result for {Dialect.DisplayName}: count={count}, second={second ?? "<null>"}.");
            }
            GC.KeepAlive(second);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunBatchNonQuery()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), transaction);
            ExecuteNonQuery(connection, Dialect.UpdateUserNameById(users, 2, "Bob-v2"), transaction);
            ExecuteNonQuery(connection, Dialect.DeleteUserById(users, 1), transaction);
            transaction.Commit();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected non-query batch count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes the provider-specific string aggregation query over sample user names.
    /// PT-br: Executa a consulta de agregação de strings específica do provedor sobre nomes de usuários de exemplo.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunStringAggregate()
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Charlie"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 3, "Bob"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.StringAggregate(users)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunStringAggregateOrdered()
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Charlie"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 3, "Bob"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.StringAggregateOrdered(users)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes the provider-specific scalar date/time query.
    /// PT-br: Executa a consulta escalar de data/hora específica do provedor.
    /// </summary>
    protected virtual void RunDateScalar()
    {
        using var connection = CreateConnection();
        connection.Open();
        var value = ExecuteScalar(connection, Dialect.DateScalar());
        GC.KeepAlive(value);
    }

    protected virtual void RunJsonScalarRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON scalar benchmark.");
        }

        using var connection = CreateConnection();
        connection.Open();
        var value = ExecuteScalar(connection, Dialect.JsonScalarRead("{\"name\":\"Alice\"}"));
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountAfterInsert()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            var affected = ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            if (affected < 1)
            {
                throw new InvalidOperationException($"Unexpected insert rowcount for {Dialect.DisplayName}: {affected}.");
            }
            GC.KeepAlive(affected);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunRowCountAfterUpdate()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var affected = ExecuteNonQuery(connection, Dialect.UpdateUserNameById(users, 1, "Alice-v2"));
            if (affected < 1)
            {
                throw new InvalidOperationException($"Unexpected update rowcount for {Dialect.DisplayName}: {affected}.");
            }
            GC.KeepAlive(affected);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes a SQL command that does not return a result set.
    /// PT-br: Executa um comando SQL que não retorna um conjunto de resultados.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT-br: A transação opcional associada à execução do comando.</param>
    protected static int ExecuteNonQuery(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }
        return command.ExecuteNonQuery();
    }

    protected static async Task<int> ExecuteNonQueryAsync(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }
        return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// EN: Executes a SQL command and returns its scalar result.
    /// PT-br: Executa um comando SQL e retorna o seu resultado escalar.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT-br: A transação opcional associada à execução do comando.</param>
    /// <returns>EN: The scalar value returned by the SQL command. PT-br: O valor escalar retornado pelo comando SQL.</returns>
    protected static object? ExecuteScalar(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }
        return command.ExecuteScalar();
    }

    protected static async Task<object?> ExecuteScalarAsync(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }
        return await command.ExecuteScalarAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// EN: Tries to drop a table using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma tabela usando uma limpeza de melhor esforço.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="tableName">EN: The table name targeted by the operation. PT-br: O nome da tabela alvo da operação.</param>
    protected void SafeDropTable(DbConnection connection, string tableName)
    {
        SafeExecute(connection, Dialect.DropTable(tableName));
    }

    /// <summary>
    /// EN: Tries to drop a sequence using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma sequência usando uma limpeza de melhor esforço.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sequenceName">EN: The sequence name targeted by the operation. PT-br: O nome da sequência alvo da operação.</param>
    protected void SafeDropSequence(DbConnection connection, string sequenceName)
    {
        SafeExecute(connection, Dialect.DropSequence(sequenceName));
    }

    /// <summary>
    /// EN: Executes a cleanup command while suppressing cleanup failures.
    /// PT-br: Executa um comando de limpeza suprimindo falhas durante a limpeza.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    protected static void SafeExecute(DbConnection connection, string sql)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Benchmark error: {ex.Message}");
        }
    }


    protected virtual void RunNestedSavepointFlow()
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            var sp1 = NewSavepointName();
            ExecuteNonQuery(connection, Dialect.Savepoint(sp1), transaction);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), transaction);
            var sp2 = NewSavepointName();
            ExecuteNonQuery(connection, Dialect.Savepoint(sp2), transaction);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 3, "Charlie"), transaction);
            ExecuteNonQuery(connection, Dialect.RollbackToSavepoint(sp2), transaction);
            ExecuteNonQuery(connection, Dialect.ReleaseSavepoint(sp1), transaction);
            transaction.Commit();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 2)
            {
                throw new InvalidOperationException($"Unexpected nested-savepoint count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunStringAggregateDistinct()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Bob"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 3, "Bob"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.StringAggregateDistinct(users)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunStringAggregateCustomSeparator()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Bob"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.StringAggregateCustomSeparator(users, ";")), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunStringAggregateLargeGroup()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            for (var i = 1; i <= 50; i++)
            {
                ExecuteNonQuery(connection, Dialect.InsertUser(users, i, $"User-{i}"));
            }
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.StringAggregateLargeGroup(users)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunTemporalCurrentTimestamp()
    {
        using var connection = CreateConnection();
        connection.Open();
        var value = ExecuteScalar(connection, Dialect.TemporalCurrentTimestamp());
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalDateAdd()
    {
        using var connection = CreateConnection();
        connection.Open();
        var value = ExecuteScalar(connection, Dialect.TemporalDateAdd());
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalNowWhere()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var value = ExecuteScalar(connection, Dialect.TemporalNowWhere(users));
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunTemporalNowOrderBy()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Bob"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            var value = ExecuteScalar(connection, Dialect.TemporalNowOrderBy(users));
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunJsonPathRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON path benchmark.");
        }

        using var connection = CreateConnection();
        connection.Open();
        var value = ExecuteScalar(connection, Dialect.JsonPathRead("{\"user\":{\"name\":\"Alice\"}}"));
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountAfterSelect()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"));
            var count = CountReaderRows(connection, $"SELECT * FROM {users}");
            if (count != 2)
            {
                throw new InvalidOperationException($"Unexpected select rowcount for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunCteSimple()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.CteSimple(users)), CultureInfo.InvariantCulture);
            if (value != 1)
            {
                throw new InvalidOperationException($"Unexpected CTE result for {Dialect.DisplayName}: {value}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunWindowRowNumber()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Bob"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 3, "Charlie"));
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.WindowRowNumber(users)), CultureInfo.InvariantCulture);
            if (value != 3)
            {
                throw new InvalidOperationException($"Unexpected ROW_NUMBER result for {Dialect.DisplayName}: {value}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunWindowLag()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Bob"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 3, "Charlie"));
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.WindowLag(users)), CultureInfo.InvariantCulture);
            if (value != 3)
            {
                throw new InvalidOperationException($"Unexpected LAG result for {Dialect.DisplayName}: {value}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }


    protected virtual void RunBatchReaderMultiResult()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"));
            var first = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            var second = ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1));
            GC.KeepAlive(first);
            GC.KeepAlive(second);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunBatchTransactionControl()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var tx = connection.BeginTransaction();
        var users = NewUsersTableName();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users), tx);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), tx);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), tx);
            tx.Commit();
            GC.KeepAlive(users);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunParseSimpleSelect()
    {
        var sql = "SELECT Name FROM Users WHERE Id = 1";
        var tokens = SimpleTokenize(sql);
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseComplexJoin()
    {
        var sql = "SELECT u.Name, COUNT(o.Id) FROM Users u LEFT JOIN Orders o ON o.UserId = u.Id WHERE u.Name LIKE 'A%' GROUP BY u.Name ORDER BY u.Name";
        var tokens = SimpleTokenize(sql);
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseInsertReturning()
    {
        var sql = "INSERT INTO Users(Id, Name) VALUES(1, 'Alice') RETURNING Id";
        var tokens = SimpleTokenize(sql);
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseOnConflictDoUpdate()
    {
        var sql = "INSERT INTO Users(Id, Name) VALUES(1, 'Alice') ON CONFLICT(Id) DO UPDATE SET Name = EXCLUDED.Name";
        var tokens = SimpleTokenize(sql);
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseJsonExtract()
    {
        var sql = "SELECT JSON_VALUE('{\"user\":{\"name\":\"Alice\"}}', '$.user.name')";
        var tokens = SimpleTokenize(sql);
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseStringAggregateWithinGroup()
    {
        var sql = "SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM Users";
        var tokens = SimpleTokenize(sql);
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseAutoDialectTopLimitFetch()
    {
        var sql = "SELECT * FROM Users ORDER BY Id OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY";
        var tokens = SimpleTokenize(sql);
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseMultiStatementBatch()
    {
        var sql = "INSERT INTO Users(Id, Name) VALUES(1, 'Alice'); UPDATE Users SET Name = 'Bob' WHERE Id = 1; SELECT Name FROM Users WHERE Id = 1;";
        var tokens = SimpleTokenize(sql);
        GC.KeepAlive(tokens);
    }

    protected virtual void RunJsonInsertCast()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON insert/cast benchmark.");
        }

        using var connection = CreateConnection();
        connection.Open();
        var value = ExecuteScalar(connection, Dialect.JsonScalarRead("{\"value\":42,\"text\":\"Alice\"}"));
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountInBatch()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"));
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 2)
            {
                throw new InvalidOperationException($"Unexpected batch rowcount for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunPivotCount()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"));
            var sql = $"SELECT SUM(CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END) + SUM(CASE WHEN Name LIKE 'B%' THEN 1 ELSE 0 END) FROM {users}";
            var value = Convert.ToInt32(ExecuteScalar(connection, sql), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunReturningInsert()
    {
        RunInsertSingle();
    }

    protected virtual void RunReturningUpdate()
    {
        RunUpdateByPk();
    }

    protected virtual void RunMergeBasic()
    {
        RunUpsert();
    }

    protected virtual void RunPartitionPruningSelect()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            for (var i = 1; i <= 20; i++)
            {
                ExecuteNonQuery(connection, Dialect.InsertUser(users, i, $"User{i:00}"));
            }
            var value = Convert.ToInt32(ExecuteScalar(connection, $"SELECT COUNT(*) FROM {users} WHERE Id BETWEEN 5 AND 10"), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }


    protected virtual void RunSelectExistsPredicate()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            SeedUsersAndOrders(connection, users, orders);
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.SelectExistsPredicate(users, orders)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunSelectCorrelatedCount()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            SeedUsersAndOrders(connection, users, orders);
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.SelectCorrelatedCount(users, orders)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunGroupByHaving()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            SeedUsersAndOrders(connection, users, orders);
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.GroupByHaving(users, orders)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunUnionAllProjection()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"));
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.UnionAllProjection(users)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunDistinctProjection()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 3, "Bob"));
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.DistinctProjection(users)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunMultiJoinAggregate()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            SeedUsersAndOrders(connection, users, orders);
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.MultiJoinAggregate(users, orders)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunSelectScalarSubquery()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            SeedUsersAndOrders(connection, users, orders);
            var value = ExecuteScalar(connection, Dialect.SelectScalarSubquery(users, orders));
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunSelectInSubquery()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            SeedUsersAndOrders(connection, users, orders);
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.SelectInSubquery(users, orders)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunCrossApplyProjection()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            SeedUsersAndOrders(connection, users, orders);
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.CrossApplyProjection(users, orders)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunOuterApplyProjection()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            SeedUsersAndOrders(connection, users, orders);
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.OuterApplyProjection(users, orders)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    private void SeedUsersAndOrders(DbConnection connection, string usersTable, string ordersTable)
    {
        ExecuteNonQuery(connection, Dialect.InsertUser(usersTable, 1, "Alice"));
        ExecuteNonQuery(connection, Dialect.InsertUser(usersTable, 2, "Bob"));
        ExecuteNonQuery(connection, Dialect.InsertUser(usersTable, 3, "Charlie"));
        ExecuteNonQuery(connection, Dialect.InsertOrder(ordersTable, 1, 1, "o-1"));
        ExecuteNonQuery(connection, Dialect.InsertOrder(ordersTable, 2, 1, "o-2"));
        ExecuteNonQuery(connection, Dialect.InsertOrder(ordersTable, 3, 2, "o-3"));
    }

    protected virtual void RunExecutionPlan()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            _ = ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1));
            var plan = TryReadDiagnosticValue(connection, "LastExecutionPlan");
            GC.KeepAlive(plan);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunExecutionPlanSelect()
    {
        RunExecutionPlan();
    }

    protected virtual void RunExecutionPlanJoin()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertOrder(orders, 1, 1, "order-1"));
            _ = ExecuteScalar(connection, Dialect.CountJoinForUser(users, orders, 1));
            var plan = TryReadDiagnosticValue(connection, "LastExecutionPlan");
            GC.KeepAlive(plan);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunExecutionPlanDml()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var plan = TryReadDiagnosticValue(connection, "LastExecutionPlan");
            GC.KeepAlive(plan);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunDebugTraceSelect()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            _ = ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1));
            var trace = TryReadDiagnosticValue(connection, "DebugSql") ?? Dialect.SelectUserNameById(users, 1);
            GC.KeepAlive(trace);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunDebugTraceBatch()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"));
            var trace = TryReadDiagnosticValue(connection, "DebugSqlBatch") ?? (Dialect.InsertUser(users, 1, "Alice") + ";" + Dialect.InsertUser(users, 2, "Bob"));
            GC.KeepAlive(trace);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunDebugTraceJson()
    {
        var payload = new Dictionary<string, object?> { ["provider"] = Dialect.DisplayName, ["engine"] = Engine.ToString(), ["timestamp"] = DateTime.UtcNow };
        var json = System.Text.Json.JsonSerializer.Serialize(payload);
        GC.KeepAlive(json);
    }

    protected virtual void RunLastExecutionPlansHistory()
    {
        using var connection = CreateConnection();
        connection.Open();
        var users = NewUsersTableName();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            _ = ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1));
            _ = ExecuteScalar(connection, Dialect.CountRows(users));
            var plans = TryReadDiagnosticValue(connection, "LastExecutionPlans");
            GC.KeepAlive(plans);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunTempTableCreateAndUse()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    protected virtual void RunTempTableRollback()
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support temp-table rollback benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        using var tx = connection.BeginTransaction();
        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users), tx);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), tx);
            ExecuteNonQuery(connection, Dialect.Savepoint(NewSavepointName()), tx);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), tx);
            tx.Rollback();
        }
        finally
        {
            try { SafeDropTable(connection, users); } catch {}
        }
    }

    protected virtual void RunTempTableCrossConnectionIsolation()
    {
        var users = NewUsersTableName();
        using var connection1 = CreateConnection();
        connection1.Open();
        using var connection2 = CreateConnection();
        connection2.Open();

        try
        {
            ExecuteNonQuery(connection1, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection1, Dialect.InsertUser(users, 1, "Alice"));
            var value = ExecuteScalar(connection2, Dialect.CountRows(users));
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection1, users);
        }
    }

    protected virtual void RunResetVolatileData()
    {
        using var connection = CreateConnection();
        connection.Open();
        TryInvokeIfExists(connection, "ResetVolatileData");
        GC.KeepAlive(connection.State);
    }

    protected virtual void RunResetAllVolatileData()
    {
        using var connection = CreateConnection();
        connection.Open();
        if (!TryInvokeIfExists(connection, "ResetAllVolatileData"))
        {
            TryInvokeIfExists(connection, "ResetVolatileData");
        }
        GC.KeepAlive(connection.State);
    }

    protected virtual void RunConnectionReopenAfterClose()
    {
        using var connection = CreateConnection();
        connection.Open();
        connection.Close();
        connection.Open();
        GC.KeepAlive(connection.State);
    }

    protected virtual void RunSchemaSnapshotExport()
    {
        using var connection = CreateConnection();
        connection.Open();
        var snapshot = TryInvokeSnapshot(connection, "ExportSchemaSnapshot") ?? TryInvokeSnapshot(connection, "GetSchemaSnapshot") ?? new { Provider = Dialect.DisplayName, Engine = Engine.ToString() };
        GC.KeepAlive(snapshot);
    }

    protected virtual void RunSchemaSnapshotToJson()
    {
        using var connection = CreateConnection();
        connection.Open();
        var snapshot = TryInvokeSnapshot(connection, "ExportSchemaSnapshot") ?? new { Provider = Dialect.DisplayName, Engine = Engine.ToString() };
        var json = System.Text.Json.JsonSerializer.Serialize(snapshot);
        GC.KeepAlive(json);
    }

    protected virtual void RunSchemaSnapshotLoadJson()
    {
        var json = "{\"provider\":\"" + Dialect.DisplayName + "\",\"version\":1}";
        var obj = System.Text.Json.JsonDocument.Parse(json);
        GC.KeepAlive(obj);
    }

    protected virtual void RunSchemaSnapshotApply()
    {
        using var connection = CreateConnection();
        connection.Open();
        var snapshot = TryInvokeSnapshot(connection, "ExportSchemaSnapshot") ?? new { Provider = Dialect.DisplayName, Engine = Engine.ToString() };
        var applied = TryInvokeWithArgIfExists(connection, "ApplySchemaSnapshot", snapshot);
        GC.KeepAlive(applied);
    }

    protected virtual void RunSchemaSnapshotRoundTrip()
    {
        using var connection = CreateConnection();
        connection.Open();
        var snapshot = TryInvokeSnapshot(connection, "ExportSchemaSnapshot") ?? new { Provider = Dialect.DisplayName, Engine = Engine.ToString() };
        var json = System.Text.Json.JsonSerializer.Serialize(snapshot);
        var obj = System.Text.Json.JsonDocument.Parse(json);
        GC.KeepAlive(obj);
    }

    protected virtual void RunSchemaSnapshotCompare()
    {
        using var connection = CreateConnection();
        connection.Open();
        var snapshot1 = TryInvokeSnapshot(connection, "ExportSchemaSnapshot") ?? new { Provider = Dialect.DisplayName, Engine = Engine.ToString(), Token = 1 };
        var snapshot2 = TryInvokeSnapshot(connection, "ExportSchemaSnapshot") ?? new { Provider = Dialect.DisplayName, Engine = Engine.ToString(), Token = 2 };
        var comparison = string.Equals(System.Text.Json.JsonSerializer.Serialize(snapshot1), System.Text.Json.JsonSerializer.Serialize(snapshot2), StringComparison.Ordinal);
        GC.KeepAlive(comparison);
    }

    protected virtual void RunFluentSchemaBuild()
    {
        var model = new
        {
            Tables = new[]
            {
                new { Name = "Users", Columns = new[] { "Id", "Name" } },
                new { Name = "Orders", Columns = new[] { "Id", "UserId", "Note" } }
            }
        };
        GC.KeepAlive(model);
    }

    protected virtual void RunFluentSeed100()
    {
        var rows = Enumerable.Range(1, 100).Select(i => new { Id = i, Name = $"User{i}" }).ToArray();
        GC.KeepAlive(rows);
    }

    protected virtual void RunFluentSeed1000()
    {
        var rows = Enumerable.Range(1, 1000).Select(i => new { Id = i, Name = $"User{i}" }).ToArray();
        GC.KeepAlive(rows);
    }

    protected virtual void RunFluentScenarioCompose()
    {
        var scenario = new
        {
            Schema = new[] { "Users", "Orders" },
            Seed = Enumerable.Range(1, 25).Select(i => $"User{i}").ToArray(),
            Query = "SELECT COUNT(*) FROM Users"
        };
        GC.KeepAlive(scenario);
    }

    protected static object? TryReadDiagnosticValue(object target, string memberName)
    {
        var type = target.GetType();
        var property = type.GetProperty(memberName);
        if (property is not null && property.GetIndexParameters().Length == 0)
        {
            return property.GetValue(target);
        }

        var field = type.GetField(memberName);
        if (field is not null)
        {
            return field.GetValue(target);
        }

        return null;
    }

    protected static bool TryInvokeIfExists(object target, string methodName)
    {
        var method = target.GetType().GetMethod(methodName, Type.EmptyTypes);
        if (method is null)
        {
            return false;
        }

        method.Invoke(target, null);
        return true;
    }

    protected static object? TryInvokeWithArgIfExists(object target, string methodName, object? arg)
    {
        var methods = target.GetType().GetMethods().Where(m => string.Equals(m.Name, methodName, StringComparison.Ordinal)).ToArray();
        foreach (var method in methods)
        {
            var parameters = method.GetParameters();
            if (parameters.Length == 1)
            {
                return method.Invoke(target, new[] { arg });
            }
        }
        return null;
    }

    protected static object? TryInvokeSnapshot(object target, string methodName)
    {
        var method = target.GetType().GetMethod(methodName, Type.EmptyTypes);
        return method?.Invoke(target, null);
    }

    protected static int SimpleTokenize(string sql)
    {
        var tokens = sql
            .Replace("(", " ")
            .Replace(")", " ")
            .Replace(",", " ")
            .Replace(";", " ")
            .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        return tokens.Length;
    }


    protected static int CountReaderRows(DbConnection connection, string sql, DbTransaction? transaction = null)
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

}

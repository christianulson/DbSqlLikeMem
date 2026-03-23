using IBM.Data.Db2;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using DbSqlLikeMem.TestTools.Benchmarks;
using DbSqlLikeMem.TestTools.DDL;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Schema;
using DbSqlLikeMem.TestTools.Query;
using System.Collections.Concurrent;

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
public abstract partial class BenchmarkSessionBase(
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
    public ProviderId Provider => Dialect.Provider;

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
    /// EN: Creates the benchmark users table through the shared CreateTable service and then removes it.
    /// PT-br: Cria a tabela de usuários do benchmark pelo service compartilhado de CreateTable e depois a remove.
    /// </summary>
    protected virtual void RunCreateSchema()
    {
        var uId = NextToken();
        var users = NewUsersTableName();

        using var connection = CreateConnection();
        connection.Open();
        var service = new CreateTableServiceTest<DbConnection>(
            connection,
            BenchmarkScenarioFactory.CreateTableScenario<DbConnection>(),
            Dialect);

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
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Inserts a single user row and validates that the row was persisted.
    /// PT-br: Insere uma única linha de usuário e valida que a linha foi persistida.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertSingle()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateInsertUsersService(connection);

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunTest(users, uId, 1);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunInsertBatch10()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateInsertUsersService(connection);

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunTest(users, uId, 10);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Inserts one hundred user rows and validates the final row count.
    /// PT-br: Insere cem linhas de usuário e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateInsertUsersService(connection);

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunTest(users, uId, 100);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Inserts one hundred user rows in parallel and validates the final row count.
    /// PT-br: Insere cem linhas de usuário em paralelo e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100Parallel()
    {
        var uId = NextToken();
        var users = NewUsersTableName();

        using var setupConnection = CreateConnection();
        setupConnection.Open();
        var service = CreateInsertUsersService(setupConnection, CreateConnection);

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunParallelTest(users, uId, 100);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(setupConnection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Reads a user name by primary key through the shared SelectByPK service and validates the returned value.
    /// PT-br: Lê um nome de usuário pela chave primária pelo service compartilhado SelectByPK e valida o valor retornado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectByPk()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = new SelectByPKServiceTest<DbConnection>(
            connection,
            BenchmarkScenarioFactory.CreateSelectTableScenario<DbConnection>(Dialect),
            Dialect);

        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunTest(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Executes a join query between users and orders and validates the resulting count.
    /// PT-br: Executa uma consulta com junção entre usuários e pedidos e valida a contagem resultante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectJoin()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(Dialect));

        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunSelectJoin(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Updates a user row by primary key and validates the stored value.
    /// PT-br: Atualiza uma linha de usuário pela chave primária e valida o valor armazenado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpdateByPk()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice")));

        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunUpdateByPk(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Deletes a user row by primary key and validates the remaining row count.
    /// PT-br: Exclui uma linha de usuário pela chave primária e valida a contagem de linhas restante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunDeleteByPk()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice"), (2, "Bob")));

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunDeleteByPk(users);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, commits it, and validates the committed result.
    /// PT-br: Executa uma inserção dentro de uma transação, confirma a operação e valida o resultado confirmado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionCommit()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect));
        var usersTable = $"{users}_{uId}";

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunTransactionCommit(usersTable);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, rolls it back, and validates that no rows remain.
    /// PT-br: Executa uma inserção dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionRollback()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect));
        var usersTable = $"{users}_{uId}";

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunTransactionRollback(usersTable);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunSavepointCreate()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
        service.CreateScenario();
        try
        {
            service.RunSavepointCreate();
        }
        finally
        {
            service.DropScenario();
        }
    }

    protected virtual void RunRollbackToSavepoint()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect));
        var usersTable = $"{users}_{uId}";

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunRollbackToSavepoint(usersTable);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunReleaseSavepoint()
    {
        if (!Dialect.SupportsReleaseSavepoints)
        {
            return;
        }

        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
        service.CreateScenario();
        try
        {
            service.RunReleaseSavepoint();
        }
        finally
        {
            service.DropScenario();
        }
    }

    /// <summary>
    /// EN: Executes the provider-specific upsert path and validates the updated value.
    /// PT-br: Executa o caminho de upsert específico do provedor e valida o valor atualizado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpsert()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice")));

        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunUpsert(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Creates a temporary sequence and reads its next value.
    /// PT-br: Cria uma sequência temporária e lê o seu próximo valor.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunSequenceNextValue()
    {
        var sequence = NewSequenceName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateSequenceScenario<DbConnection>(Dialect));

        try
        {
            service.CreateScenario(sequence);
            var value = service.RunSequenceNextValue(sequence);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(sequence);
            }
            catch
            {
                SafeDropSequence(connection, sequence);
            }
        }
    }

    protected virtual void RunBatchInsert10()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunBatchInsert10(users);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunBatchInsert100()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunBatchInsert100(users);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunBatchMixedReadWrite()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));

        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunBatchMixedReadWrite(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunBatchScalar()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));

        try
        {
            service.CreateScenario(users, uId);
            var second = service.RunBatchScalar(users);
            GC.KeepAlive(second);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunBatchNonQuery()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));

        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunBatchNonQuery(users);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Executes the provider-specific string aggregation query over sample user names.
    /// PT-br: Executa a consulta de agregação de strings específica do provedor sobre nomes de usuários de exemplo.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunStringAggregate()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Charlie"), (2, "Alice"), (3, "Bob")));

        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunStringAggregate(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunStringAggregateOrdered()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Charlie"), (2, "Alice"), (3, "Bob")));

        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunStringAggregateOrdered(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
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
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
        var value = service.RunDateScalar();
        GC.KeepAlive(value);
    }

    protected virtual void RunJsonScalarRead()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
        var value = service.RunJsonScalarRead();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountAfterInsert()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateInsertUsersService(connection);

        try
        {
            service.CreateScenario(users, uId);
            var affected = service.RunRowCountAfterInsert(users, uId);
            GC.KeepAlive(affected);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunRowCountAfterUpdate()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice")));

        try
        {
            service.CreateScenario(users, uId);
            var affected = service.RunRowCountAfterUpdate(users);
            GC.KeepAlive(affected);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    /// <summary>
    /// EN: Tries to drop a table using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma tabela usando uma limpeza de melhor esforço.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="tableName">EN: The table name targeted by the operation. PT-br: O nome da tabela alvo da operação.</param>
    protected void SafeDropTable(DbConnection connection, string tableName, string uId)
    {
        SafeExecute(connection, Dialect.DropTable(tableName, uId));
    }

    /// <summary>
    /// EN: Tries to drop a temporary table using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma tabela temporaria usando uma limpeza de melhor esforco.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexao de banco de dados usada para executar a operacao.</param>
    /// <param name="tableName">EN: The temporary table name targeted by the operation. PT-br: O nome da tabela temporaria alvo da operacao.</param>
    protected void SafeDropTemporaryTable(DbConnection connection, string tableName)
    {
        SafeExecute(connection, Dialect.DropTemporaryUsersTable(tableName));
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
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateMutationService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
        var usersTable = $"{users}_{uId}";
        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunNestedSavepointFlow(usersTable);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunStringAggregateDistinct()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Bob"), (2, "Alice"), (3, "Bob")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunStringAggregateDistinct(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunStringAggregateCustomSeparator()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Bob"), (2, "Alice")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunStringAggregateCustomSeparator(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunStringAggregateLargeGroup()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var seedRows = new (int id, string name)[50];
        for (var i = 1; i <= 50; i++)
        {
            seedRows[i - 1] = (i, $"User-{i}");
        }

        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, seedRows));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunStringAggregateLargeGroup(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunTemporalCurrentTimestamp()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
        var value = service.RunTemporalCurrentTimestamp();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalDateAdd()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
        var value = service.RunTemporalDateAdd();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalNowWhere()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunTemporalNowWhere(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunTemporalNowOrderBy()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Bob"), (2, "Alice")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunTemporalNowOrderBy(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunJsonPathRead()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
        var value = service.RunJsonPathRead();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountAfterSelect()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice"), (2, "Bob")));
        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunRowCountAfterSelect(users);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunCteSimple()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunCteSimple(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunWindowRowNumber()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Bob"), (2, "Alice"), (3, "Charlie")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunWindowRowNumber(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunWindowLag()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Bob"), (2, "Alice"), (3, "Charlie")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunWindowLag(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }


    protected virtual void RunBatchReaderMultiResult()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunBatchReaderMultiResult(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunBatchTransactionControl()
    {
        var uId = NextToken();
        using var connection = CreateConnection();
        connection.Open();
        var users = NewUsersTableName();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunBatchTransactionControl(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunParseSimpleSelect()
    {
        var tokens = ParseServiceTest.RunParseSimpleSelect();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseComplexJoin()
    {
        var tokens = ParseServiceTest.RunParseComplexJoin();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseInsertReturning()
    {
        var tokens = ParseServiceTest.RunParseInsertReturning();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseOnConflictDoUpdate()
    {
        var tokens = ParseServiceTest.RunParseOnConflictDoUpdate();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseJsonExtract()
    {
        var tokens = ParseServiceTest.RunParseJsonExtract();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseStringAggregateWithinGroup()
    {
        var tokens = ParseServiceTest.RunParseStringAggregateWithinGroup();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseAutoDialectTopLimitFetch()
    {
        var tokens = ParseServiceTest.RunParseAutoDialectTopLimitFetch();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseMultiStatementBatch()
    {
        var tokens = ParseServiceTest.RunParseMultiStatementBatch();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunJsonInsertCast()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>());
        var value = service.RunJsonInsertCast();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountInBatch()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var count = service.RunRowCountInBatch(users);
            GC.KeepAlive(count);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunPivotCount()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice"), (2, "Bob")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunPivotCount(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunReturningInsert()
    {
        if (Dialect.Provider != ProviderId.MariaDb)
        {
            RunInsertSingle();
            return;
        }

        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateBatchService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var rows = service.RunReturningInsert(users);
            GC.KeepAlive(rows);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
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
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var seedRows = new (int id, string name)[20];
        for (var i = 1; i <= 20; i++)
        {
            seedRows[i - 1] = (i, $"User{i:00}");
        }

        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, seedRows));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunPartitionPruningSelect(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }


    protected virtual void RunSelectExistsPredicate()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunSelectExistsPredicate(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunSelectCorrelatedCount()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunSelectCorrelatedCount(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunGroupByHaving()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunGroupByHaving(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunUnionAllProjection()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice"), (2, "Bob")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunUnionAllProjection(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunDistinctProjection()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice"), (2, "Alice"), (3, "Bob")));
        try
        {
            service.CreateScenario(users, uId);
            var value = service.RunDistinctProjection(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunMultiJoinAggregate()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunMultiJoinAggregate(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunSelectScalarSubquery()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunSelectScalarSubquery(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunSelectInSubquery()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunSelectInSubquery(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunCrossApplyProjection()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunCrossApplyProjection(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunOuterApplyProjection()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateQueryService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var value = service.RunOuterApplyProjection(users, orders);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunExecutionPlan()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateExecutionPlanService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice")));
        try
        {
            service.CreateScenario(users, uId);
            var plan = service.RunExecutionPlan(users);
            GC.KeepAlive(plan);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunExecutionPlanSelect()
    {
        RunExecutionPlan();
    }

    protected virtual void RunExecutionPlanJoin()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateExecutionPlanService(
            connection,
            BenchmarkScenarioFactory.CreateUsersOrdersScenario<DbConnection>(
                Dialect,
                [(1, "Alice")],
                [(1, 1, "order-1")]));
        try
        {
            service.CreateScenario(users, orders, uId);
            var plan = service.RunExecutionPlanJoin(users, orders);
            GC.KeepAlive(plan);
        }
        finally
        {
            try
            {
                service.DropScenario(users, orders, uId);
            }
            catch
            {
                SafeDropTable(connection, orders, uId);
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunExecutionPlanDml()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateExecutionPlanService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var plan = service.RunExecutionPlanDml(users);
            GC.KeepAlive(plan);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunDebugTraceSelect()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateDebugTraceService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice")));
        try
        {
            service.CreateScenario(users, uId);
            var trace = service.RunDebugTraceSelect(users);
            GC.KeepAlive(trace);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunDebugTraceBatch()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateDebugTraceService(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var trace = service.RunDebugTraceBatch(users);
            GC.KeepAlive(trace);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunDebugTraceJson()
    {
        var json = DebugTraceServiceTest<DbConnection>.RunDebugTraceJson(Dialect.DisplayName, Engine.ToString());
        GC.KeepAlive(json);
    }

    protected virtual void RunLastExecutionPlansHistory()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateExecutionPlanService(connection, BenchmarkScenarioFactory.CreateUsersScenario<DbConnection>(Dialect, (1, "Alice")));
        try
        {
            service.CreateScenario(users, uId);
            var plans = service.RunLastExecutionPlansHistory(users);
            GC.KeepAlive(plans);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunTempTableCreateAndUse()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateTemporaryTableService(connection, BenchmarkScenarioFactory.CreateTemporaryTableScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var rows = service.RunCreateTemporaryTableAsSelectThenSelect(users, uId);
            GC.KeepAlive(rows);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
    }

    protected virtual void RunTempTableRollback()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateTemporaryTableService(connection, BenchmarkScenarioFactory.CreateTemporaryUsersScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users);
            service.RunTempTableRollback(users);
        }
        finally
        {
            try
            {
                service.DropScenario(users);
            }
            catch
            {
                SafeDropTemporaryTable(connection, users);
            }
        }
    }

    protected virtual void RunTempTableCrossConnectionIsolation()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateTemporaryTableService(connection, BenchmarkScenarioFactory.CreateTemporaryUsersScenario<DbConnection>(Dialect), CreateConnection);
        try
        {
            service.CreateScenario(users);
            var value = service.RunTemporaryTableCrossConnectionIsolation(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users);
            }
            catch
            {
                SafeDropTemporaryTable(connection, users);
            }
        }
    }

    protected virtual void RunResetVolatileData()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateConnectionLifecycleService(connection);
        service.RunResetVolatileData();
    }

    protected virtual void RunResetAllVolatileData()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateConnectionLifecycleService(connection);
        service.RunResetAllVolatileData();
    }

    protected virtual void RunConnectionReopenAfterClose()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateConnectionLifecycleService(connection);
        service.RunConnectionReopenAfterClose();
    }

    protected virtual void RunSchemaSnapshotExport()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateSchemaSnapshotService(connection);
        var snapshot = service.RunSchemaSnapshotExport();
        GC.KeepAlive(snapshot);
    }

    protected virtual void RunSchemaSnapshotToJson()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateSchemaSnapshotService(connection);
        var json = service.RunSchemaSnapshotToJson();
        GC.KeepAlive(json);
    }

    protected virtual void RunSchemaSnapshotLoadJson()
    {
        var obj = SchemaSnapshotServiceTest<DbConnection>.RunSchemaSnapshotLoadJson(Dialect.DisplayName);
        GC.KeepAlive(obj);
    }

    protected virtual void RunSchemaSnapshotApply()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateSchemaSnapshotService(connection);
        var applied = service.RunSchemaSnapshotApply();
        GC.KeepAlive(applied);
    }

    protected virtual void RunSchemaSnapshotRoundTrip()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateSchemaSnapshotService(connection);
        var obj = service.RunSchemaSnapshotRoundTrip();
        GC.KeepAlive(obj);
    }

    protected virtual void RunSchemaSnapshotCompare()
    {
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateSchemaSnapshotService(connection);
        var comparison = service.RunSchemaSnapshotCompare();
        GC.KeepAlive(comparison);
    }

    protected virtual void RunFluentSchemaBuild()
    {
        var model = FluentServiceTest<DbConnection>.BuildFluentSchemaBuild();
        GC.KeepAlive(model);
    }

    protected virtual void RunFluentSeed100()
    {
        var rows = FluentServiceTest<DbConnection>.BuildFluentSeed100();
        GC.KeepAlive(rows);
    }

    protected virtual void RunFluentSeed1000()
    {
        var rows = FluentServiceTest<DbConnection>.BuildFluentSeed1000();
        GC.KeepAlive(rows);
    }

    protected virtual void RunFluentScenarioCompose()
    {
        var scenario = FluentServiceTest<DbConnection>.BuildFluentScenarioCompose();
        GC.KeepAlive(scenario);
    }

}

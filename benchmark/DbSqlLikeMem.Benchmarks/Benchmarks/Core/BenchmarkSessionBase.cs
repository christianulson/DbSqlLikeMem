using IBM.Data.Db2;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
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

    /// <summary>
    /// EN: Executes one benchmark feature and routes any provider-specific failure to the benchmark logger.
    /// PT-br: Executa um recurso de benchmark e encaminha qualquer falha especifica do provedor para o logger do benchmark.
    /// </summary>
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
            case BenchmarkFeatureId.CreateTableWithFK:
                RunCreateTableWithFK();
                break;
            case BenchmarkFeatureId.CreateTableWithFKInsert:
                RunCreateTableWithFKInsert();
                break;
            case BenchmarkFeatureId.DropTable:
                RunDropTable();
                break;
            case BenchmarkFeatureId.InsertSingle:
                RunInsertSingle();
                break;
            case BenchmarkFeatureId.InsertCustomStartId:
                RunInsertCustomStartId();
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
            case BenchmarkFeatureId.ParameterProjection:
                RunParameterProjection();
                break;
            case BenchmarkFeatureId.ParameterInsertSingle:
                RunParameterInsertSingle();
                break;
            case BenchmarkFeatureId.StoredProcedureCall:
                RunStoredProcedureCall();
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
            case BenchmarkFeatureId.WindowLead:
                RunWindowLead();
                break;
            case BenchmarkFeatureId.WindowRankDenseRank:
                RunWindowRankDenseRank();
                break;
            case BenchmarkFeatureId.WindowFirstLastValue:
                RunWindowFirstLastValue();
                break;
            case BenchmarkFeatureId.WindowNtile:
                RunWindowNtile();
                break;
            case BenchmarkFeatureId.WindowPercentRankCumeDist:
                RunWindowPercentRankCumeDist();
                break;
            case BenchmarkFeatureId.WindowNthValue:
                RunWindowNthValue();
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
            case BenchmarkFeatureId.SelectNotExistsPredicate:
                RunSelectNotExistsPredicate();
                break;
            case BenchmarkFeatureId.SelectLeftJoinAntiJoin:
                RunSelectLeftJoinAntiJoin();
                break;
            case BenchmarkFeatureId.SelectScalarSubquery:
                RunSelectScalarSubquery();
                break;
            case BenchmarkFeatureId.SelectScalarCaseMatrix:
                RunSelectScalarCaseMatrix();
                break;
            case BenchmarkFeatureId.DistinctProjection:
                RunDistinctProjection();
                break;
            case BenchmarkFeatureId.UnionDistinctProjection:
                RunUnionDistinctProjection();
                break;
            case BenchmarkFeatureId.SelectInSubquery:
                RunSelectInSubquery();
                break;
            case BenchmarkFeatureId.SelectNotInSubquery:
                RunSelectNotInSubquery();
                break;
            case BenchmarkFeatureId.SelectBetweenLikeOrderByMatrix:
                RunSelectBetweenLikeOrderByMatrix();
                break;
            case BenchmarkFeatureId.OuterApplyProjection:
                RunOuterApplyProjection();
                break;
            case BenchmarkFeatureId.CrossApplyProjection:
                RunCrossApplyProjection();
                break;
            case BenchmarkFeatureId.PagedNameProjection:
                RunPagedNameProjection();
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
        DisposePreparedStates();
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
        var state = GetPreparedCreateSchemaState();
        state.RunCreateSchema();
    }

    /// <summary>
    /// EN: Creates the benchmark users and orders tables with a foreign key and removes them after the run.
    /// PT-br: Cria as tabelas de usuarios e pedidos do benchmark com chave estrangeira e as remove apos a execucao.
    /// </summary>
    protected virtual void RunCreateTableWithFK()
    {
        var state = GetPreparedCreateTableWithFkState();
        state.RunCreateTableWithFk();
    }

    /// <summary>
    /// EN: Creates the benchmark foreign-key tables and inserts a referenced row.
    /// PT-br: Cria as tabelas com chave estrangeira do benchmark e insere uma linha referenciada.
    /// </summary>
    protected virtual void RunCreateTableWithFKInsert()
    {
        var state = GetPreparedCreateTableWithFkState();
        var count = state.RunCreateTableWithFkInsert();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Creates and drops the benchmark users table through the shared DDL drop workflow.
    /// PT-br: Cria e remove a tabela de usuarios do benchmark pelo fluxo compartilhado de remocao DDL.
    /// </summary>
    protected virtual void RunDropTable()
    {
        var state = GetPreparedDropTableState();
        state.RunDropTable();
    }

    /// <summary>
    /// EN: Inserts a single user row and validates that the row was persisted.
    /// PT-br: Insere uma única linha de usuário e valida que a linha foi persistida.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertSingle()
    {
        var state = GetPreparedInsertUsersState("InsertSingle");
        var count = state.RunSequentialInsert(1);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts three rows starting from a custom id and validates the persisted names.
    /// PT-br: Insere tres linhas iniciando em um id customizado e valida os nomes persistidos.
    /// </summary>
    protected virtual void RunInsertCustomStartId()
    {
        var state = GetPreparedInsertUsersState("InsertCustomStartId");
        var result = state.RunInsertCustomStartId();
        GC.KeepAlive(result);
    }

    protected virtual void RunInsertBatch10()
    {
        var state = GetPreparedInsertUsersState("InsertBatch10");
        var count = state.RunSequentialInsert(10);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts one hundred user rows and validates the final row count.
    /// PT-br: Insere cem linhas de usuário e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100()
    {
        var state = GetPreparedInsertUsersState("InsertBatch100");
        var count = state.RunSequentialInsert(100);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts one hundred user rows in parallel and validates the final row count.
    /// PT-br: Insere cem linhas de usuário em paralelo e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100Parallel()
    {
        var state = GetPreparedInsertUsersState("InsertBatch100Parallel");
        var count = state.RunParallelInsert(100);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Reads a user name by primary key through the shared SelectByPK service and validates the returned value.
    /// PT-br: Lê um nome de usuário pela chave primária pelo service compartilhado SelectByPK e valida o valor retornado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectByPk()
    {
        var state = GetPreparedSelectByPkState();
        var value = state.Service.RunTest(state.Users, state.UId);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a join query between users and orders and validates the resulting count.
    /// PT-br: Executa uma consulta com junção entre usuários e pedidos e valida a contagem resultante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectJoin()
    {
        var state = GetPreparedSelectJoinState();
        var value = state.Service.RunSelectJoin(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Updates a user row by primary key and validates the stored value.
    /// PT-br: Atualiza uma linha de usuário pela chave primária e valida o valor armazenado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpdateByPk()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var value = state.RunUpdateByPk();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Deletes a user row by primary key and validates the remaining row count.
    /// PT-br: Exclui uma linha de usuário pela chave primária e valida a contagem de linhas restante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunDeleteByPk()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var count = state.RunDeleteByPk();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, commits it, and validates the committed result.
    /// PT-br: Executa uma inserção dentro de uma transação, confirma a operação e valida o resultado confirmado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionCommit()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunTransactionCommit();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, rolls it back, and validates that no rows remain.
    /// PT-br: Executa uma inserção dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionRollback()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunTransactionRollback();
        GC.KeepAlive(count);
    }

    protected virtual void RunSavepointCreate()
    {
        var state = GetPreparedNoopMutationState("NoopMutation");
        state.Service.RunSavepointCreate();
    }

    protected virtual void RunRollbackToSavepoint()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunRollbackToSavepoint();
        GC.KeepAlive(count);
    }

    protected virtual void RunReleaseSavepoint()
    {
        if (!Dialect.SupportsReleaseSavepoints)
        {
            return;
        }

        var state = GetPreparedNoopMutationState("NoopMutation");
        state.Service.RunReleaseSavepoint();
    }

    /// <summary>
    /// EN: Executes the provider-specific upsert path and validates the updated value.
    /// PT-br: Executa o caminho de upsert específico do provedor e valida o valor atualizado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpsert()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var value = state.RunUpsert();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a parameter binding benchmark using a scalar projection query.
    /// PT-br: Executa um benchmark de binding de parametros usando uma consulta de projeção escalar.
    /// </summary>
    protected virtual void RunParameterProjection()
    {
        var state = GetPreparedParameterProjectionState("ParameterProjection");
        var value = state.RunParameterProjection();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a parameter binding benchmark using a single-row INSERT statement.
    /// PT-br: Executa um benchmark de binding de parametros usando uma instrucao INSERT de uma linha.
    /// </summary>
    protected virtual void RunParameterInsertSingle()
    {
        var state = GetPreparedInsertUsersState("ParameterInsertSingle");
        var count = state.RunParameterInsertSingle();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes a stored procedure call benchmark.
    /// PT-br: Executa um benchmark de chamada de procedimento armazenado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunStoredProcedureCall()
    {
        throw new NotSupportedException($"{Dialect.DisplayName} does not support stored procedure benchmarks.");
    }

    /// <summary>
    /// EN: Creates a temporary sequence and reads its next value.
    /// PT-br: Cria uma sequência temporária e lê o seu próximo valor.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunSequenceNextValue()
    {
        var state = GetPreparedSequenceState("SequenceNextValue");
        var value = state.RunSequenceNextValue();
        GC.KeepAlive(value);
    }

    protected virtual void RunBatchInsert10()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchInsert(10);
        GC.KeepAlive(count);
    }

    protected virtual void RunBatchInsert100()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchInsert(100);
        GC.KeepAlive(count);
    }

    protected virtual void RunBatchMixedReadWrite()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchMixedReadWrite();
        GC.KeepAlive(value);
    }

    protected virtual void RunBatchScalar()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var second = state.RunBatchScalar();
        GC.KeepAlive(second);
    }

    protected virtual void RunBatchNonQuery()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchNonQuery();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the provider-specific string aggregation query over sample user names.
    /// PT-br: Executa a consulta de agregação de strings específica do provedor sobre nomes de usuários de exemplo.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunStringAggregate()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregate",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregate(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunStringAggregateOrdered()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateOrdered",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateOrdered(state.UsersTable);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the provider-specific scalar date/time query.
    /// PT-br: Executa a consulta escalar de data/hora específica do provedor.
    /// </summary>
    protected virtual void RunDateScalar()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunDateScalar();
        GC.KeepAlive(value);
    }

    protected virtual void RunJsonScalarRead()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonScalarRead();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountAfterInsert()
    {
        var state = GetPreparedInsertUsersState("RowCountAfterInsert");
        var affected = state.RunRowCountAfterInsert();
        GC.KeepAlive(affected);
    }

    protected virtual void RunRowCountAfterUpdate()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var affected = state.RunRowCountAfterUpdate();
        GC.KeepAlive(affected);
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
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunNestedSavepointFlow();
        GC.KeepAlive(count);
    }

    protected virtual void RunStringAggregateDistinct()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateDistinct",
            (1, "Bob"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateDistinct(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunStringAggregateCustomSeparator()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateCustomSeparator",
            (1, "Bob"), (2, "Alice"));
        var value = state.Service.RunStringAggregateCustomSeparator(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunStringAggregateLargeGroup()
    {
        var seedRows = new (int id, string name)[50];
        for (var i = 1; i <= 50; i++)
        {
            seedRows[i - 1] = (i, $"User-{i}");
        }

        var state = GetPreparedUsersQueryState("StringAggregateLargeGroup", seedRows);
        var value = state.Service.RunStringAggregateLargeGroup(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalCurrentTimestamp()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunTemporalCurrentTimestamp();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalDateAdd()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunTemporalDateAdd();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalNowWhere()
    {
        var state = GetPreparedUsersQueryState("TemporalNowWhere", (1, "Alice"));
        var value = state.Service.RunTemporalNowWhere(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalNowOrderBy()
    {
        var state = GetPreparedUsersQueryState("TemporalNowOrderBy", (1, "Bob"), (2, "Alice"));
        var value = state.Service.RunTemporalNowOrderBy(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunJsonPathRead()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonPathRead();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountAfterSelect()
    {
        var state = GetPreparedUsersQueryState("RowCountAfterSelect", (1, "Alice"), (2, "Bob"));
        var count = state.Service.RunRowCountAfterSelect(state.UsersTable);
        GC.KeepAlive(count);
    }

    protected virtual void RunCteSimple()
    {
        var state = GetPreparedUsersQueryState("CteSimple", (1, "Alice"));
        var value = state.Service.RunCteSimple(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowRowNumber()
    {
        var state = GetPreparedUsersQueryState(
            "WindowRowNumber",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowRowNumber(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowLag()
    {
        var state = GetPreparedUsersQueryState(
            "WindowLag",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowLag(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowLead()
    {
        var state = GetPreparedUsersQueryState(
            "WindowLead",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowLead(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowRankDenseRank()
    {
        var state = GetPreparedUsersQueryState(
            "WindowRankDenseRank",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowRankDenseRank(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowFirstLastValue()
    {
        var state = GetPreparedUsersQueryState(
            "WindowFirstLastValue",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowFirstLastValue(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowNtile()
    {
        var state = GetPreparedUsersQueryState(
            "WindowNtile",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowNtile(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowPercentRankCumeDist()
    {
        var state = GetPreparedUsersQueryState(
            "WindowPercentRankCumeDist",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowPercentRankCumeDist(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowNthValue()
    {
        var state = GetPreparedUsersQueryState(
            "WindowNthValue",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowNthValue(state.UsersTable);
        GC.KeepAlive(value);
    }


    protected virtual void RunBatchReaderMultiResult()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchReaderMultiResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunBatchTransactionControl()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchTransactionControl();
        GC.KeepAlive(value);
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
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonInsertCast();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountInBatch()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunRowCountInBatch();
        GC.KeepAlive(count);
    }

    protected virtual void RunPivotCount()
    {
        var state = GetPreparedUsersQueryState("PivotCount", (1, "Alice"), (2, "Bob"));
        var value = state.Service.RunPivotCount(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunReturningInsert()
    {
        if (Dialect.Provider != ProviderId.MariaDb)
        {
            RunInsertSingle();
            return;
        }

        var state = GetPreparedReturningInsertState("ReturningInsert");
        var rows = state.RunReturningInsert();
        GC.KeepAlive(rows);
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
        var seedRows = new (int id, string name)[20];
        for (var i = 1; i <= 20; i++)
        {
            seedRows[i - 1] = (i, $"User{i:00}");
        }

        var state = GetPreparedUsersQueryState("PartitionPruningSelect", seedRows);
        var value = state.Service.RunPartitionPruningSelect(state.UsersTable);
        GC.KeepAlive(value);
    }


    protected virtual void RunSelectExistsPredicate()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectExistsPredicate(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectNotExistsPredicate()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectNotExistsPredicate(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a LEFT JOIN anti-join benchmark and validates the orphan row count.
    /// PT-br: Executa um benchmark de anti-join com LEFT JOIN e valida a contagem de linhas sem correspondencia.
    /// </summary>
    protected virtual void RunSelectLeftJoinAntiJoin()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.RunSelectLeftJoinAntiJoin();
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectCorrelatedCount()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectCorrelatedCount(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectScalarCaseMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectScalarCaseMatrix(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunGroupByHaving()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunGroupByHaving(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunUnionAllProjection()
    {
        var state = GetPreparedUsersQueryState("UnionAllProjection", (1, "Alice"), (2, "Bob"));
        var value = state.Service.RunUnionAllProjection(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunUnionDistinctProjection()
    {
        var state = GetPreparedUsersQueryState("UnionDistinctProjection", (1, "Alice"), (2, "Bob"), (3, "Charlie"));
        var value = state.Service.RunUnionDistinctProjection(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunDistinctProjection()
    {
        var state = GetPreparedUsersQueryState(
            "DistinctProjection",
            (1, "Alice"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunDistinctProjection(state.UsersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunMultiJoinAggregate()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunMultiJoinAggregate(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectScalarSubquery()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectScalarSubquery(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectInSubquery()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectInSubquery(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectNotInSubquery()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectNotInSubquery(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectBetweenLikeOrderByMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "BetweenLikeOrderByMatrix",
            (1, "Aaron"),
            (2, "Alice"),
            (3, "Bob"),
            (4, "Charlie"),
            (5, "Delta"));
        var value = state.RunBetweenLikeOrderByMatrix();
        GC.KeepAlive(value);
    }

    protected virtual void RunCrossApplyProjection()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunCrossApplyProjection(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunOuterApplyProjection()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunOuterApplyProjection(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(value);
    }

    protected virtual void RunPagedNameProjection()
    {
        var state = GetPreparedUsersQueryState(
            "PagedNameProjection",
            (1, "Charlie"),
            (2, "Bob"),
            (3, "Alice"),
            (4, "Delta"),
            (5, "Echo"));
        var count = CountReaderRows(state.Connection, Dialect.PagedNameProjection(state.UsersTable, 1, 2));
        GC.KeepAlive(count);
    }

    protected virtual void RunExecutionPlan()
    {
        var state = GetPreparedExecutionPlanState("ExecutionPlan", (1, "Alice"));
        var plan = state.Service.RunExecutionPlan(state.UsersTable);
        GC.KeepAlive(plan);
    }

    protected virtual void RunExecutionPlanSelect()
    {
        RunExecutionPlan();
    }

    protected virtual void RunExecutionPlanJoin()
    {
        var state = GetPreparedExecutionPlanJoinState(
            "ExecutionPlanJoin",
            [(1, "Alice")],
            [(1, 1, "order-1")]);
        var plan = state.Service.RunExecutionPlanJoin(state.UsersTable, state.OrdersTable);
        GC.KeepAlive(plan);
    }

    protected virtual void RunExecutionPlanDml()
    {
        var state = GetPreparedExecutionPlanDmlState("ExecutionPlanDml");
        var plan = state.RunExecutionPlanDml();
        GC.KeepAlive(plan);
    }

    protected virtual void RunDebugTraceSelect()
    {
        var state = GetPreparedDebugTraceSelectState("DebugTraceSelect", (1, "Alice"));
        var trace = state.Service.RunDebugTraceSelect(state.UsersTable);
        GC.KeepAlive(trace);
    }

    protected virtual void RunDebugTraceBatch()
    {
        var state = GetPreparedDebugTraceBatchState("DebugTraceBatch");
        var trace = state.RunDebugTraceBatch();
        GC.KeepAlive(trace);
    }

    protected virtual void RunDebugTraceJson()
    {
        var json = DebugTraceServiceTest<DbConnection>.RunDebugTraceJson(Dialect.DisplayName, Engine.ToString());
        GC.KeepAlive(json);
    }

    protected virtual void RunLastExecutionPlansHistory()
    {
        var state = GetPreparedExecutionPlanState("LastExecutionPlansHistory", (1, "Alice"));
        var plans = state.Service.RunLastExecutionPlansHistory(state.UsersTable);
        GC.KeepAlive(plans);
    }

    protected virtual void RunTempTableCreateAndUse()
    {
        var state = GetPreparedTemporaryTableSourceState("TempTableSource");
        var rows = state.RunCreateTemporaryTableAsSelectThenSelect();
        GC.KeepAlive(rows);
    }

    protected virtual void RunTempTableRollback()
    {
        var state = GetPreparedTemporaryUsersState("TempUsers");
        state.RunTempTableRollback();
    }

    protected virtual void RunTempTableCrossConnectionIsolation()
    {
        var state = GetPreparedTemporaryUsersState("TempUsersIsolation");
        var value = state.RunTemporaryTableCrossConnectionIsolation();
        GC.KeepAlive(value);
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
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var snapshot = state.Service.RunSchemaSnapshotExport();
        GC.KeepAlive(snapshot);
    }

    protected virtual void RunSchemaSnapshotToJson()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var json = state.Service.RunSchemaSnapshotToJson();
        GC.KeepAlive(json);
    }

    protected virtual void RunSchemaSnapshotLoadJson()
    {
        var obj = SchemaSnapshotServiceTest<DbConnection>.RunSchemaSnapshotLoadJson(Dialect.DisplayName);
        GC.KeepAlive(obj);
    }

    protected virtual void RunSchemaSnapshotApply()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var applied = state.Service.RunSchemaSnapshotApply();
        GC.KeepAlive(applied);
    }

    protected virtual void RunSchemaSnapshotRoundTrip()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var obj = state.Service.RunSchemaSnapshotRoundTrip();
        GC.KeepAlive(obj);
    }

    protected virtual void RunSchemaSnapshotCompare()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var comparison = state.Service.RunSchemaSnapshotCompare();
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

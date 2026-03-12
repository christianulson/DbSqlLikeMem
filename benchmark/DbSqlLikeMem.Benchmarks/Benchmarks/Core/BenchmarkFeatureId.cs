namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Enumerates the benchmark feature identifiers executed by the benchmark suites.
/// PT-br: Enumera os identificadores de recursos de benchmark executados pelas suítes de benchmark.
/// </summary>
public enum BenchmarkFeatureId
{
    /// <summary>EN: Opens a new connection. PT-br: Abre uma nova conexão.</summary>
    ConnectionOpen,
    /// <summary>EN: Creates the benchmark schema. PT-br: Cria o esquema de benchmark.</summary>
    CreateSchema,
    /// <summary>EN: Inserts a single row. PT-br: Insere uma única linha.</summary>
    InsertSingle,
    /// <summary>EN: Inserts ten rows sequentially. PT-br: Insere dez linhas sequencialmente.</summary>
    InsertBatch10,
    /// <summary>EN: Inserts one hundred rows sequentially. PT-br: Insere cem linhas sequencialmente.</summary>
    InsertBatch100,
    /// <summary>EN: Inserts one hundred rows in parallel. PT-br: Insere cem linhas em paralelo.</summary>
    InsertBatch100Parallel,
    /// <summary>EN: Reads one row by primary key. PT-br: Lê uma linha pela chave primária.</summary>
    SelectByPk,
    /// <summary>EN: Executes a join query. PT-br: Executa uma consulta com junção.</summary>
    SelectJoin,
    /// <summary>EN: Updates one row by primary key. PT-br: Atualiza uma linha pela chave primária.</summary>
    UpdateByPk,
    /// <summary>EN: Deletes one row by primary key. PT-br: Remove uma linha pela chave primária.</summary>
    DeleteByPk,
    /// <summary>EN: Commits a transaction. PT-br: Confirma uma transação.</summary>
    TransactionCommit,
    /// <summary>EN: Rolls back a transaction. PT-br: Desfaz uma transação.</summary>
    TransactionRollback,
    /// <summary>EN: Creates a savepoint. PT-br: Cria um savepoint.</summary>
    SavepointCreate,
    /// <summary>EN: Rolls back to a savepoint. PT-br: Faz rollback para um savepoint.</summary>
    RollbackToSavepoint,
    /// <summary>EN: Releases a savepoint. PT-br: Libera um savepoint.</summary>
    ReleaseSavepoint,
    /// <summary>EN: Executes a nested savepoint flow. PT-br: Executa um fluxo com savepoints aninhados.</summary>
    NestedSavepointFlow,
    /// <summary>EN: Executes the provider-specific upsert path. PT-br: Executa o caminho de upsert específico do provedor.</summary>
    Upsert,
    /// <summary>EN: Reads the next value from a sequence. PT-br: Lê o próximo valor de uma sequência.</summary>
    SequenceNextValue,
    /// <summary>EN: Executes a batch insert with ten statements. PT-br: Executa um insert em lote com dez comandos.</summary>
    BatchInsert10,
    /// <summary>EN: Executes a batch insert with one hundred statements. PT-br: Executa um insert em lote com cem comandos.</summary>
    BatchInsert100,
    /// <summary>EN: Executes a mixed read/write batch. PT-br: Executa um lote misto de leitura e escrita.</summary>
    BatchMixedReadWrite,
    /// <summary>EN: Executes a scalar batch flow. PT-br: Executa um fluxo de lote escalar.</summary>
    BatchScalar,
    /// <summary>EN: Executes a non-query batch flow. PT-br: Executa um fluxo de lote sem resultado.</summary>
    BatchNonQuery,
    /// <summary>EN: Executes string aggregation. PT-br: Executa agregação de strings.</summary>
    StringAggregate,
    /// <summary>EN: Executes ordered string aggregation. PT-br: Executa agregação de strings ordenada.</summary>
    StringAggregateOrdered,
    /// <summary>EN: Executes a scalar date/time query. PT-br: Executa uma consulta escalar de data/hora.</summary>
    DateScalar,
    /// <summary>EN: Reads a JSON scalar value. PT-br: Lê um valor escalar de JSON.</summary>
    JsonScalarRead,
    /// <summary>EN: Reads a nested JSON path value. PT-br: Lê um valor de caminho JSON aninhado.</summary>
    JsonPathRead,
    /// <summary>EN: Executes a current timestamp scalar query. PT-br: Executa uma consulta escalar de timestamp atual.</summary>
    TemporalCurrentTimestamp,
    /// <summary>EN: Executes a temporal date-add query. PT-br: Executa uma consulta temporal de soma de data.</summary>
    TemporalDateAdd,
    /// <summary>EN: Executes a temporal current-time predicate query. PT-br: Executa uma consulta temporal com predicado de tempo atual.</summary>
    TemporalNowWhere,
    /// <summary>EN: Executes a temporal current-time ordering query. PT-br: Executa uma consulta temporal com ordenação por tempo atual.</summary>
    TemporalNowOrderBy,
    /// <summary>EN: Executes a distinct string aggregation query. PT-br: Executa uma consulta de agregação distinta de strings.</summary>
    StringAggregateDistinct,
    /// <summary>EN: Executes a custom-separator string aggregation query. PT-br: Executa uma consulta de agregação de strings com separador customizado.</summary>
    StringAggregateCustomSeparator,
    /// <summary>EN: Executes a large-group string aggregation query. PT-br: Executa uma consulta de agregação de strings em grupo grande.</summary>
    StringAggregateLargeGroup,
    /// <summary>EN: Reads rowcount after select. PT-br: Lê a contagem de linhas após select.</summary>
    RowCountAfterSelect,
    /// <summary>EN: Executes a simple CTE query. PT-br: Executa uma consulta simples com CTE.</summary>
    CteSimple,
    /// <summary>EN: Executes a ROW_NUMBER window query. PT-br: Executa uma consulta de janela com ROW_NUMBER.</summary>
    WindowRowNumber,
    /// <summary>EN: Executes a LAG window query. PT-br: Executa uma consulta de janela com LAG.</summary>
    WindowLag,
    /// <summary>EN: Reads rowcount after insert. PT-br: Lê a contagem de linhas após insert.</summary>
    RowCountAfterInsert,
    /// <summary>EN: Reads rowcount after update. PT-br: Lê a contagem de linhas após update.</summary>
    RowCountAfterUpdate,
    /// <summary>EN: Publishes the last execution plan. PT-br: Publica o último plano de execução.</summary>
    ExecutionPlan,
    /// <summary>EN: Benchmark entry for BatchReaderMultiResult. PT-br: Entrada de benchmark para BatchReaderMultiResult.</summary>
    BatchReaderMultiResult,
    /// <summary>EN: Benchmark entry for BatchTransactionControl. PT-br: Entrada de benchmark para BatchTransactionControl.</summary>
    BatchTransactionControl,
    /// <summary>EN: Benchmark entry for ParseSimpleSelect. PT-br: Entrada de benchmark para ParseSimpleSelect.</summary>
    ParseSimpleSelect,
    /// <summary>EN: Benchmark entry for ParseComplexJoin. PT-br: Entrada de benchmark para ParseComplexJoin.</summary>
    ParseComplexJoin,
    /// <summary>EN: Benchmark entry for ParseInsertReturning. PT-br: Entrada de benchmark para ParseInsertReturning.</summary>
    ParseInsertReturning,
    /// <summary>EN: Benchmark entry for ParseOnConflictDoUpdate. PT-br: Entrada de benchmark para ParseOnConflictDoUpdate.</summary>
    ParseOnConflictDoUpdate,
    /// <summary>EN: Benchmark entry for ParseJsonExtract. PT-br: Entrada de benchmark para ParseJsonExtract.</summary>
    ParseJsonExtract,
    /// <summary>EN: Benchmark entry for ParseStringAggregateWithinGroup. PT-br: Entrada de benchmark para ParseStringAggregateWithinGroup.</summary>
    ParseStringAggregateWithinGroup,
    /// <summary>EN: Benchmark entry for ParseAutoDialectTopLimitFetch. PT-br: Entrada de benchmark para ParseAutoDialectTopLimitFetch.</summary>
    ParseAutoDialectTopLimitFetch,
    /// <summary>EN: Benchmark entry for ParseMultiStatementBatch. PT-br: Entrada de benchmark para ParseMultiStatementBatch.</summary>
    ParseMultiStatementBatch,
    /// <summary>EN: Benchmark entry for JsonInsertCast. PT-br: Entrada de benchmark para JsonInsertCast.</summary>
    JsonInsertCast,
    /// <summary>EN: Benchmark entry for RowCountInBatch. PT-br: Entrada de benchmark para RowCountInBatch.</summary>
    RowCountInBatch,
    /// <summary>EN: Benchmark entry for PivotCount. PT-br: Entrada de benchmark para PivotCount.</summary>
    PivotCount,
    /// <summary>EN: Benchmark entry for ReturningInsert. PT-br: Entrada de benchmark para ReturningInsert.</summary>
    ReturningInsert,
    /// <summary>EN: Benchmark entry for ReturningUpdate. PT-br: Entrada de benchmark para ReturningUpdate.</summary>
    ReturningUpdate,
    /// <summary>EN: Benchmark entry for MergeBasic. PT-br: Entrada de benchmark para MergeBasic.</summary>
    MergeBasic,
    /// <summary>EN: Benchmark entry for PartitionPruningSelect. PT-br: Entrada de benchmark para PartitionPruningSelect.</summary>
    PartitionPruningSelect,
    /// <summary>EN: Benchmark entry for ExecutionPlan. PT-br: Entrada de benchmark para ExecutionPlan.</summary>
    ExecutionPlan,
    /// <summary>EN: Benchmark entry for ExecutionPlanSelect. PT-br: Entrada de benchmark para ExecutionPlanSelect.</summary>
    ExecutionPlanSelect,
    /// <summary>EN: Benchmark entry for ExecutionPlanJoin. PT-br: Entrada de benchmark para ExecutionPlanJoin.</summary>
    ExecutionPlanJoin,
    /// <summary>EN: Benchmark entry for ExecutionPlanDml. PT-br: Entrada de benchmark para ExecutionPlanDml.</summary>
    ExecutionPlanDml,
    /// <summary>EN: Benchmark entry for DebugTraceSelect. PT-br: Entrada de benchmark para DebugTraceSelect.</summary>
    DebugTraceSelect,
    /// <summary>EN: Benchmark entry for DebugTraceBatch. PT-br: Entrada de benchmark para DebugTraceBatch.</summary>
    DebugTraceBatch,
    /// <summary>EN: Benchmark entry for DebugTraceJson. PT-br: Entrada de benchmark para DebugTraceJson.</summary>
    DebugTraceJson,
    /// <summary>EN: Benchmark entry for LastExecutionPlansHistory. PT-br: Entrada de benchmark para LastExecutionPlansHistory.</summary>
    LastExecutionPlansHistory,
    /// <summary>EN: Benchmark entry for TempTableCreateAndUse. PT-br: Entrada de benchmark para TempTableCreateAndUse.</summary>
    TempTableCreateAndUse,
    /// <summary>EN: Benchmark entry for TempTableRollback. PT-br: Entrada de benchmark para TempTableRollback.</summary>
    TempTableRollback,
    /// <summary>EN: Benchmark entry for TempTableCrossConnectionIsolation. PT-br: Entrada de benchmark para TempTableCrossConnectionIsolation.</summary>
    TempTableCrossConnectionIsolation,
    /// <summary>EN: Benchmark entry for ResetVolatileData. PT-br: Entrada de benchmark para ResetVolatileData.</summary>
    ResetVolatileData,
    /// <summary>EN: Benchmark entry for ResetAllVolatileData. PT-br: Entrada de benchmark para ResetAllVolatileData.</summary>
    ResetAllVolatileData,
    /// <summary>EN: Benchmark entry for ConnectionReopenAfterClose. PT-br: Entrada de benchmark para ConnectionReopenAfterClose.</summary>
    ConnectionReopenAfterClose,
    /// <summary>EN: Benchmark entry for SchemaSnapshotExport. PT-br: Entrada de benchmark para SchemaSnapshotExport.</summary>
    SchemaSnapshotExport,
    /// <summary>EN: Benchmark entry for SchemaSnapshotToJson. PT-br: Entrada de benchmark para SchemaSnapshotToJson.</summary>
    SchemaSnapshotToJson,
    /// <summary>EN: Benchmark entry for SchemaSnapshotLoadJson. PT-br: Entrada de benchmark para SchemaSnapshotLoadJson.</summary>
    SchemaSnapshotLoadJson,
    /// <summary>EN: Benchmark entry for SchemaSnapshotApply. PT-br: Entrada de benchmark para SchemaSnapshotApply.</summary>
    SchemaSnapshotApply,
    /// <summary>EN: Benchmark entry for SchemaSnapshotRoundTrip. PT-br: Entrada de benchmark para SchemaSnapshotRoundTrip.</summary>
    SchemaSnapshotRoundTrip,
    /// <summary>EN: Benchmark entry for SchemaSnapshotCompare. PT-br: Entrada de benchmark para SchemaSnapshotCompare.</summary>
    SchemaSnapshotCompare,
    /// <summary>EN: Benchmark entry for FluentSchemaBuild. PT-br: Entrada de benchmark para FluentSchemaBuild.</summary>
    FluentSchemaBuild,
    /// <summary>EN: Benchmark entry for FluentSeed100. PT-br: Entrada de benchmark para FluentSeed100.</summary>
    FluentSeed100,
    /// <summary>EN: Benchmark entry for FluentSeed1000. PT-br: Entrada de benchmark para FluentSeed1000.</summary>
    FluentSeed1000,
    /// <summary>EN: Benchmark entry for FluentScenarioCompose. PT-br: Entrada de benchmark para FluentScenarioCompose.</summary>
    FluentScenarioCompose,
}

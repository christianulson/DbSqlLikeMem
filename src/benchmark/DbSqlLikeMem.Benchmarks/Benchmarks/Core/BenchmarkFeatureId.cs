namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Enumerates the benchmark feature identifiers executed by the benchmark suites.
/// PT: Enumera os identificadores de recursos de benchmark executados pelas suites de benchmark.
/// </summary>
public enum BenchmarkFeatureId
{
    /// <summary>
    /// EN: Opens a new connection.
    /// PT: Abre uma nova conexao.
    /// </summary>
    ConnectionOpen,

    /// <summary>
    /// EN: Creates the benchmark schema.
    /// PT: Cria o esquema de benchmark.
    /// </summary>
    CreateSchema,

    /// <summary>
    /// EN: Creates the benchmark users and orders tables with a foreign key.
    /// PT: Cria as tabelas de usuarios e pedidos do benchmark com chave estrangeira.
    /// </summary>
    CreateTableWithFK,

    /// <summary>
    /// EN: Creates the benchmark foreign-key tables and inserts a referenced row.
    /// PT: Cria as tabelas com chave estrangeira do benchmark e insere uma linha referenciada.
    /// </summary>
    CreateTableWithFKInsert,

    /// <summary>
    /// EN: Drops the benchmark users table.
    /// PT: Remove a tabela de usuarios do benchmark.
    /// </summary>
    DropTable,

    /// <summary>
    /// EN: Inserts a single row.
    /// PT: Insere uma unica linha.
    /// </summary>
    InsertSingle,

    /// <summary>
    /// EN: Inserts rows starting from a custom id.
    /// PT: Insere linhas iniciando em um id customizado.
    /// </summary>
    InsertCustomStartId,

    /// <summary>
    /// EN: Inserts ten rows sequentially.
    /// PT: Insere dez linhas sequencialmente.
    /// </summary>
    InsertBatch10,

    /// <summary>
    /// EN: Inserts one hundred rows sequentially.
    /// PT: Insere cem linhas sequencialmente.
    /// </summary>
    InsertBatch100,

    /// <summary>
    /// EN: Inserts one hundred rows in parallel.
    /// PT: Insere cem linhas em paralelo.
    /// </summary>
    InsertBatch100Parallel,

    /// <summary>
    /// EN: Reads one row by primary key.
    /// PT: Le uma linha pela chave primaria.
    /// </summary>
    SelectByPk,

    /// <summary>
    /// EN: Executes a join query.
    /// PT: Executa uma consulta com juncao.
    /// </summary>
    SelectJoin,

    /// <summary>
    /// EN: Updates one row by primary key.
    /// PT: Atualiza uma linha pela chave primaria.
    /// </summary>
    UpdateByPk,

    /// <summary>
    /// EN: Executes an update/delete round trip.
    /// PT: Executa um ciclo de update/delete.
    /// </summary>
    UpdateDeleteRoundTrip,

    /// <summary>
    /// EN: Deletes one row by primary key.
    /// PT: Remove uma linha pela chave primaria.
    /// </summary>
    DeleteByPk,

    /// <summary>
    /// EN: Commits a transaction.
    /// PT: Confirma uma transacao.
    /// </summary>
    TransactionCommit,

    /// <summary>
    /// EN: Rolls back a transaction.
    /// PT: Desfaz uma transacao.
    /// </summary>
    TransactionRollback,

    /// <summary>
    /// EN: Executes an update/delete workflow inside a transaction.
    /// PT: Executa um fluxo de update/delete dentro de uma transacao.
    /// </summary>
    TransactionalUpdateDeleteCommit,

    /// <summary>
    /// EN: Executes typed parameter inserts inside a committed transaction.
    /// PT: Executa inserts tipados com parametros dentro de uma transacao confirmada.
    /// </summary>
    ParameterTransactionCommit,

    /// <summary>
    /// EN: Executes typed parameter inserts inside a rolled-back transaction.
    /// PT: Executa inserts tipados com parametros dentro de uma transacao revertida.
    /// </summary>
    ParameterTransactionRollback,

    /// <summary>
    /// EN: Creates a savepoint.
    /// PT: Cria um savepoint.
    /// </summary>
    SavepointCreate,

    /// <summary>
    /// EN: Rolls back to a savepoint.
    /// PT: Faz rollback para um savepoint.
    /// </summary>
    RollbackToSavepoint,

    /// <summary>
    /// EN: Releases a savepoint.
    /// PT: Libera um savepoint.
    /// </summary>
    ReleaseSavepoint,

    /// <summary>
    /// EN: Executes a nested savepoint flow.
    /// PT: Executa um fluxo com savepoints aninhados.
    /// </summary>
    NestedSavepointFlow,

    /// <summary>
    /// EN: Executes the provider-specific upsert path.
    /// PT: Executa o caminho de upsert especifico do provedor.
    /// </summary>
    Upsert,

    /// <summary>
    /// EN: Reads the next value from a sequence.
    /// PT: Le o proximo valor de uma sequencia.
    /// </summary>
    SequenceNextValue,

    /// <summary>
    /// EN: Executes a batch insert with ten statements.
    /// PT: Executa um insert em lote com dez comandos.
    /// </summary>
    BatchInsert10,

    /// <summary>
    /// EN: Executes a batch insert with one hundred statements.
    /// PT: Executa um insert em lote com cem comandos.
    /// </summary>
    BatchInsert100,

    /// <summary>
    /// EN: Executes a mixed read/write batch.
    /// PT: Executa um lote misto de leitura e escrita.
    /// </summary>
    BatchMixedReadWrite,

    /// <summary>
    /// EN: Executes a scalar batch flow.
    /// PT: Executa um fluxo de lote escalar.
    /// </summary>
    BatchScalar,

    /// <summary>
    /// EN: Executes a non-query batch flow.
    /// PT: Executa um fluxo de lote sem resultado.
    /// </summary>
    BatchNonQuery,

    /// <summary>
    /// EN: Executes string aggregation.
    /// PT: Executa agregacao de strings.
    /// </summary>
    StringAggregate,

    /// <summary>
    /// EN: Executes ordered string aggregation.
    /// PT: Executa agregacao de strings ordenada.
    /// </summary>
    StringAggregateOrdered,

    /// <summary>
    /// EN: Executes a scalar date/time query.
    /// PT: Executa uma consulta escalar de data/hora.
    /// </summary>
    DateScalar,

    /// <summary>
    /// EN: Reads a JSON scalar value.
    /// PT: Le um valor escalar de JSON.
    /// </summary>
    JsonScalarRead,

    /// <summary>
    /// EN: Reads a nested JSON path value.
    /// PT: Le um valor de caminho JSON aninhado.
    /// </summary>
    JsonPathRead,

    /// <summary>
    /// EN: Executes a current timestamp scalar query.
    /// PT: Executa uma consulta escalar de timestamp atual.
    /// </summary>
    TemporalCurrentTimestamp,

    /// <summary>
    /// EN: Executes a temporal date-add query.
    /// PT: Executa uma consulta temporal de soma de data.
    /// </summary>
    TemporalDateAdd,

    /// <summary>
    /// EN: Executes a temporal current-time predicate query.
    /// PT: Executa uma consulta temporal com predicado de tempo atual.
    /// </summary>
    TemporalNowWhere,

    /// <summary>
    /// EN: Executes a temporal current-time ordering query.
    /// PT: Executa uma consulta temporal com ordenacao por tempo atual.
    /// </summary>
    TemporalNowOrderBy,

    /// <summary>
    /// EN: Executes a distinct string aggregation query.
    /// PT: Executa uma consulta de agregacao distinta de strings.
    /// </summary>
    StringAggregateDistinct,

    /// <summary>
    /// EN: Executes a custom-separator string aggregation query.
    /// PT: Executa uma consulta de agregacao de strings com separador customizado.
    /// </summary>
    StringAggregateCustomSeparator,

    /// <summary>
    /// EN: Executes a large-group string aggregation query.
    /// PT: Executa uma consulta de agregacao de strings em grupo grande.
    /// </summary>
    StringAggregateLargeGroup,

    /// <summary>
    /// EN: Reads rowcount after select.
    /// PT: Le a contagem de linhas apos select.
    /// </summary>
    RowCountAfterSelect,

    /// <summary>
    /// EN: Executes a simple CTE query.
    /// PT: Executa uma consulta simples com CTE.
    /// </summary>
    CteSimple,

    /// <summary>
    /// EN: Executes a ROW_NUMBER window query.
    /// PT: Executa uma consulta de janela com ROW_NUMBER.
    /// </summary>
    WindowRowNumber,

    /// <summary>
    /// EN: Executes a LAG window query.
    /// PT: Executa uma consulta de janela com LAG.
    /// </summary>
    WindowLag,

    /// <summary>
    /// EN: Executes a LEAD window query.
    /// PT: Executa uma consulta de janela com LEAD.
    /// </summary>
    WindowLead,

    /// <summary>
    /// EN: Executes a ranking window query with dense-rank and rank.
    /// PT: Executa uma consulta de janela de ranking com dense-rank e rank.
    /// </summary>
    WindowRankDenseRank,

    /// <summary>
    /// EN: Executes a FIRST_VALUE and LAST_VALUE window query.
    /// PT: Executa uma consulta de janela com FIRST_VALUE e LAST_VALUE.
    /// </summary>
    WindowFirstLastValue,

    /// <summary>
    /// EN: Executes an NTILE window query.
    /// PT: Executa uma consulta de janela com NTILE.
    /// </summary>
    WindowNtile,

    /// <summary>
    /// EN: Executes a PERCENT_RANK and CUME_DIST window query.
    /// PT: Executa uma consulta de janela com PERCENT_RANK e CUME_DIST.
    /// </summary>
    WindowPercentRankCumeDist,

    /// <summary>
    /// EN: Executes an NTH_VALUE window query.
    /// PT: Executa uma consulta de janela com NTH_VALUE.
    /// </summary>
    WindowNthValue,

    /// <summary>
    /// EN: Executes an EXISTS predicate query.
    /// PT: Executa uma consulta com predicado EXISTS.
    /// </summary>
    SelectExistsPredicate,

    /// <summary>
    /// EN: Executes a NOT EXISTS predicate query.
    /// PT: Executa uma consulta com predicado NOT EXISTS.
    /// </summary>
    SelectNotExistsPredicate,

    /// <summary>
    /// EN: Executes a LEFT JOIN anti-join query.
    /// PT: Executa uma consulta anti-join com LEFT JOIN.
    /// </summary>
    SelectLeftJoinAntiJoin,

    /// <summary>
    /// EN: Executes a correlated subquery count flow.
    /// PT: Executa um fluxo com subconsulta correlacionada de contagem.
    /// </summary>
    SelectCorrelatedCount,

    /// <summary>
    /// EN: Executes a scalar subquery and CASE matrix query.
    /// PT: Executa uma consulta matricial com subconsulta escalar e CASE.
    /// </summary>
    SelectScalarCaseMatrix,

    /// <summary>
    /// EN: Executes a GROUP BY HAVING query.
    /// PT: Executa uma consulta GROUP BY HAVING.
    /// </summary>
    GroupByHaving,

    /// <summary>
    /// EN: Executes a UNION ALL projection query.
    /// PT: Executa uma consulta de projecao com UNION ALL.
    /// </summary>
    UnionAllProjection,

    /// <summary>
    /// EN: Executes a UNION projection query.
    /// PT: Executa uma consulta de projeção com UNION.
    /// </summary>
    UnionDistinctProjection,

    /// <summary>
    /// EN: Executes a DISTINCT projection query.
    /// PT: Executa uma consulta de projeacao DISTINCT.
    /// </summary>
    DistinctProjection,

    /// <summary>
    /// EN: Executes a multi-join aggregate query.
    /// PT: Executa uma consulta agregada com multiplos joins.
    /// </summary>
    MultiJoinAggregate,

    /// <summary>
    /// EN: Executes a scalar subquery projection.
    /// PT: Executa uma projecao com subconsulta escalar.
    /// </summary>
    SelectScalarSubquery,

    /// <summary>
    /// EN: Executes an IN subquery predicate.
    /// PT: Executa um predicado IN com subconsulta.
    /// </summary>
    SelectInSubquery,

    /// <summary>
    /// EN: Executes a NOT IN subquery predicate.
    /// PT: Executa um predicado NOT IN com subconsulta.
    /// </summary>
    SelectNotInSubquery,

    /// <summary>
    /// EN: Executes a combined BETWEEN, LIKE, and ORDER BY query.
    /// PT: Executa uma consulta combinada com BETWEEN, LIKE e ORDER BY.
    /// </summary>
    SelectBetweenLikeOrderByMatrix,

    /// <summary>
    /// EN: Executes a CROSS APPLY style projection.
    /// PT: Executa uma projecao no estilo CROSS APPLY.
    /// </summary>
    CrossApplyProjection,

    /// <summary>
    /// EN: Executes an OUTER APPLY style projection.
    /// PT: Executa uma projecao no estilo OUTER APPLY.
    /// </summary>
    OuterApplyProjection,

    /// <summary>
    /// EN: Executes a paged name projection query.
    /// PT: Executa uma consulta de projeção paginada de nomes.
    /// </summary>
    PagedNameProjection,

    /// <summary>
    /// EN: Reads rowcount after insert.
    /// PT: Le a contagem de linhas apos insert.
    /// </summary>
    RowCountAfterInsert,

    /// <summary>
    /// EN: Reads rowcount after update.
    /// PT: Le a contagem de linhas apos update.
    /// </summary>
    RowCountAfterUpdate,

    /// <summary>
    /// EN: Benchmark entry for batch reader multi-result.
    /// PT: Entrada de benchmark para batch reader multi-result.
    /// </summary>
    BatchReaderMultiResult,

    /// <summary>
    /// EN: Benchmark entry for batch transaction control.
    /// PT: Entrada de benchmark para batch transaction control.
    /// </summary>
    BatchTransactionControl,

    /// <summary>
    /// EN: Benchmark entry for parse simple select.
    /// PT: Entrada de benchmark para parse simple select.
    /// </summary>
    ParseSimpleSelect,

    /// <summary>
    /// EN: Benchmark entry for parse complex join.
    /// PT: Entrada de benchmark para parse complex join.
    /// </summary>
    ParseComplexJoin,

    /// <summary>
    /// EN: Benchmark entry for parse insert returning.
    /// PT: Entrada de benchmark para parse insert returning.
    /// </summary>
    ParseInsertReturning,

    /// <summary>
    /// EN: Benchmark entry for parse on conflict do update.
    /// PT: Entrada de benchmark para parse on conflict do update.
    /// </summary>
    ParseOnConflictDoUpdate,

    /// <summary>
    /// EN: Benchmark entry for parse JSON extract.
    /// PT: Entrada de benchmark para parse JSON extract.
    /// </summary>
    ParseJsonExtract,

    /// <summary>
    /// EN: Benchmark entry for parse string aggregate within group.
    /// PT: Entrada de benchmark para parse string aggregate within group.
    /// </summary>
    ParseStringAggregateWithinGroup,

    /// <summary>
    /// EN: Benchmark entry for parse auto-dialect TOP/LIMIT/FETCH.
    /// PT: Entrada de benchmark para parse auto-dialect TOP/LIMIT/FETCH.
    /// </summary>
    ParseAutoDialectTopLimitFetch,

    /// <summary>
    /// EN: Benchmark entry for parse multi-statement batch.
    /// PT: Entrada de benchmark para parse multi-statement batch.
    /// </summary>
    ParseMultiStatementBatch,

    /// <summary>
    /// EN: Benchmark entry for JSON insert cast.
    /// PT: Entrada de benchmark para JSON insert cast.
    /// </summary>
    JsonInsertCast,

    /// <summary>
    /// EN: Benchmark entry for rowcount in batch.
    /// PT: Entrada de benchmark para rowcount in batch.
    /// </summary>
    RowCountInBatch,

    /// <summary>
    /// EN: Benchmark entry for pivot count.
    /// PT: Entrada de benchmark para pivot count.
    /// </summary>
    PivotCount,

    /// <summary>
    /// EN: Benchmark entry for returning insert.
    /// PT: Entrada de benchmark para returning insert.
    /// </summary>
    ReturningInsert,

    /// <summary>
    /// EN: Benchmark entry for returning update.
    /// PT: Entrada de benchmark para returning update.
    /// </summary>
    ReturningUpdate,

    /// <summary>
    /// EN: Benchmark entry for merge basic.
    /// PT: Entrada de benchmark para merge basic.
    /// </summary>
    MergeBasic,

    /// <summary>
    /// EN: Benchmark entry for partition pruning select.
    /// PT: Entrada de benchmark para partition pruning select.
    /// </summary>
    PartitionPruningSelect,

    /// <summary>
    /// EN: Benchmark entry for execution plan.
    /// PT: Entrada de benchmark para execution plan.
    /// </summary>
    ExecutionPlan,

    /// <summary>
    /// EN: Benchmark entry for execution plan select.
    /// PT: Entrada de benchmark para execution plan select.
    /// </summary>
    ExecutionPlanSelect,

    /// <summary>
    /// EN: Benchmark entry for execution plan join.
    /// PT: Entrada de benchmark para execution plan join.
    /// </summary>
    ExecutionPlanJoin,

    /// <summary>
    /// EN: Benchmark entry for execution plan DML.
    /// PT: Entrada de benchmark para execution plan DML.
    /// </summary>
    ExecutionPlanDml,

    /// <summary>
    /// EN: Benchmark entry for debug trace select.
    /// PT: Entrada de benchmark para debug trace select.
    /// </summary>
    DebugTraceSelect,

    /// <summary>
    /// EN: Benchmark entry for debug trace batch.
    /// PT: Entrada de benchmark para debug trace batch.
    /// </summary>
    DebugTraceBatch,

    /// <summary>
    /// EN: Benchmark entry for debug trace JSON.
    /// PT: Entrada de benchmark para debug trace JSON.
    /// </summary>
    DebugTraceJson,

    /// <summary>
    /// EN: Benchmark entry for last execution plans history.
    /// PT: Entrada de benchmark para last execution plans history.
    /// </summary>
    LastExecutionPlansHistory,

    /// <summary>
    /// EN: Benchmark entry for temp table create and use.
    /// PT: Entrada de benchmark para temp table create and use.
    /// </summary>
    TempTableCreateAndUse,

    /// <summary>
    /// EN: Benchmark entry for temp table rollback.
    /// PT: Entrada de benchmark para temp table rollback.
    /// </summary>
    TempTableRollback,

    /// <summary>
    /// EN: Benchmark entry for temp table cross-connection isolation.
    /// PT: Entrada de benchmark para temp table cross-connection isolation.
    /// </summary>
    TempTableCrossConnectionIsolation,

    /// <summary>
    /// EN: Benchmark entry for reset volatile data.
    /// PT: Entrada de benchmark para reset volatile data.
    /// </summary>
    ResetVolatileData,

    /// <summary>
    /// EN: Benchmark entry for reset all volatile data.
    /// PT: Entrada de benchmark para reset all volatile data.
    /// </summary>
    ResetAllVolatileData,

    /// <summary>
    /// EN: Benchmark entry for connection reopen after close.
    /// PT: Entrada de benchmark para connection reopen after close.
    /// </summary>
    ConnectionReopenAfterClose,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot export.
    /// PT: Entrada de benchmark para schema snapshot export.
    /// </summary>
    SchemaSnapshotExport,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot to JSON.
    /// PT: Entrada de benchmark para schema snapshot to JSON.
    /// </summary>
    SchemaSnapshotToJson,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot load JSON.
    /// PT: Entrada de benchmark para schema snapshot load JSON.
    /// </summary>
    SchemaSnapshotLoadJson,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot apply.
    /// PT: Entrada de benchmark para schema snapshot apply.
    /// </summary>
    SchemaSnapshotApply,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot round-trip.
    /// PT: Entrada de benchmark para schema snapshot round-trip.
    /// </summary>
    SchemaSnapshotRoundTrip,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot compare.
    /// PT: Entrada de benchmark para schema snapshot compare.
    /// </summary>
    SchemaSnapshotCompare,

    /// <summary>
    /// EN: Benchmark entry for fluent schema build.
    /// PT: Entrada de benchmark para fluent schema build.
    /// </summary>
    FluentSchemaBuild,

    /// <summary>
    /// EN: Benchmark entry for fluent seed 100.
    /// PT: Entrada de benchmark para fluent seed 100.
    /// </summary>
    FluentSeed100,

    /// <summary>
    /// EN: Benchmark entry for fluent seed 1000.
    /// PT: Entrada de benchmark para fluent seed 1000.
    /// </summary>
    FluentSeed1000,

    /// <summary>
    /// EN: Benchmark entry for fluent scenario compose.
    /// PT: Entrada de benchmark para fluent scenario compose.
    /// </summary>
    FluentScenarioCompose,

    /// <summary>
    /// EN: Executes a parameter projection benchmark.
    /// PT: Executa um benchmark de projeção parametrizada.
    /// </summary>
    ParameterProjection,

    /// <summary>
    /// EN: Executes a parameterized single-row insert benchmark.
    /// PT: Executa um benchmark de insercao parametrizada de uma linha.
    /// </summary>
    ParameterInsertSingle,

    /// <summary>
    /// EN: Executes a stored procedure call benchmark.
    /// PT: Executa um benchmark de chamada de procedimento armazenado.
    /// </summary>
    StoredProcedureCall,

    /// <summary>
    /// EN: Executes a Firebird EXECUTE BLOCK benchmark with SQLSTATE handling.
    /// PT: Executa um benchmark Firebird de EXECUTE BLOCK com tratamento de SQLSTATE.
    /// </summary>
    ExecuteBlockSqlState23000,
}

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Enumerates the benchmark feature identifiers executed by the benchmark suites.
/// PT-br: Enumera os identificadores de recursos de benchmark executados pelas suites de benchmark.
/// </summary>
public enum BenchmarkFeatureId
{
    /// <summary>
    /// EN: Opens a new connection.
    /// PT-br: Abre uma nova conexao.
    /// </summary>
    ConnectionOpen,

    /// <summary>
    /// EN: Creates the benchmark schema.
    /// PT-br: Cria o esquema de benchmark.
    /// </summary>
    CreateSchema,

    /// <summary>
    /// EN: Creates the benchmark table.
    /// PT-br: Cria a tabela de benchmark.
    /// </summary>
    CreateTable,

    /// <summary>
    /// EN: Creates the benchmark users and orders tables with a foreign key.
    /// PT-br: Cria as tabelas de usuarios e pedidos do benchmark com chave estrangeira.
    /// </summary>
    CreateTableWithFK,

    /// <summary>
    /// EN: Creates the benchmark foreign-key tables and inserts a referenced row.
    /// PT-br: Cria as tabelas com chave estrangeira do benchmark e insere uma linha referenciada.
    /// </summary>
    CreateTableWithFKInsert,

    /// <summary>
    /// EN: Executes the insert-in-table-with-FK benchmark.
    /// PT-br: Executa o benchmark de insert na tabela com chave estrangeira.
    /// </summary>
    InsertInTableWithFK,

    /// <summary>
    /// EN: Drops the benchmark users table.
    /// PT-br: Remove a tabela de usuarios do benchmark.
    /// </summary>
    DropTable,

    /// <summary>
    /// EN: Inserts a single row.
    /// PT-br: Insere uma unica linha.
    /// </summary>
    InsertSingle,

    /// <summary>
    /// EN: Inserts rows starting from a custom id.
    /// PT-br: Insere linhas iniciando em um id customizado.
    /// </summary>
    InsertCustomStartId,

    /// <summary>
    /// EN: Inserts a row that uses default-backed columns.
    /// PT-br: Insere uma linha que usa colunas apoiadas por default.
    /// </summary>
    InsertDefaultColumns,

    /// <summary>
    /// EN: Inserts a row that omits nullable columns.
    /// PT-br: Insere uma linha que omite colunas anulaveis.
    /// </summary>
    InsertNullableColumns,

    /// <summary>
    /// EN: Attempts an insert that omits a required NOT NULL column.
    /// PT-br: Tenta um insert que omite uma coluna NOT NULL obrigatoria.
    /// </summary>
    InsertNotNullWithoutDefault,

    /// <summary>
    /// EN: Inserts a row that satisfies the check constraints.
    /// PT-br: Insere uma linha que satisfaz as restricoes check.
    /// </summary>
    CheckConstraintsValidInsert,

    /// <summary>
    /// EN: Attempts an insert that violates a check constraint.
    /// PT-br: Tenta um insert que viola uma restricao check.
    /// </summary>
    CheckConstraintsInvalidInsert,

    /// <summary>
    /// EN: Attempts an update that violates a check constraint.
    /// PT-br: Tenta um update que viola uma restricao check.
    /// </summary>
    CheckConstraintsInvalidUpdate,

    /// <summary>
    /// EN: Inserts ten rows sequentially.
    /// PT-br: Insere dez linhas sequencialmente.
    /// </summary>
    InsertBatch10,

    /// <summary>
    /// EN: Inserts one hundred rows sequentially.
    /// PT-br: Insere cem linhas sequencialmente.
    /// </summary>
    InsertBatch100,

    /// <summary>
    /// EN: Inserts one hundred rows in parallel.
    /// PT-br: Insere cem linhas em paralelo.
    /// </summary>
    InsertBatch100Parallel,

    /// <summary>
    /// EN: Reads one row by primary key.
    /// PT-br: Le uma linha pela chave primaria.
    /// </summary>
    SelectByPk,

    /// <summary>
    /// EN: Executes a join query.
    /// PT-br: Executa uma consulta com juncao.
    /// </summary>
    SelectJoin,

    /// <summary>
    /// EN: Executes the relational composite benchmark.
    /// PT-br: Executa o benchmark composto relacional.
    /// </summary>
    RelationalComposite,

    /// <summary>
    /// EN: Executes the join-count benchmark.
    /// PT-br: Executa o benchmark de contagem do join.
    /// </summary>
    SelectJoinCount,

    /// <summary>
    /// EN: Executes the APPLY projection benchmark.
    /// PT-br: Executa o benchmark de projeção APPLY.
    /// </summary>
    SelectApplyProjection,

    /// <summary>
    /// EN: Executes the window-functions benchmark.
    /// PT-br: Executa o benchmark de funcoes de janela.
    /// </summary>
    SelectWindowFunctions,

    /// <summary>
    /// EN: Executes the scalar-subquery CASE matrix benchmark.
    /// PT-br: Executa o benchmark da matriz CASE com subconsulta escalar.
    /// </summary>
    SelectScalarSubqueryCaseMatrix,

    /// <summary>
    /// EN: Executes the range-and-pivot benchmark.
    /// PT-br: Executa o benchmark de faixa e pivot.
    /// </summary>
    SelectRangeAndPivot,

    /// <summary>
    /// EN: Executes an IN-list predicate benchmark.
    /// PT-br: Executa um benchmark de predicado IN com lista.
    /// </summary>
    InListPredicate,

    /// <summary>
    /// EN: Executes a BETWEEN predicate benchmark.
    /// PT-br: Executa um benchmark de predicado BETWEEN.
    /// </summary>
    BetweenPredicate,

    /// <summary>
    /// EN: Executes a LIKE predicate benchmark.
    /// PT-br: Executa um benchmark de predicado LIKE.
    /// </summary>
    LikePredicate,

    /// <summary>
    /// EN: Executes a NOT LIKE predicate benchmark.
    /// PT-br: Executa um benchmark de predicado NOT LIKE.
    /// </summary>
    NotLikePredicate,

    /// <summary>
    /// EN: Executes a not-equal predicate benchmark.
    /// PT-br: Executa um benchmark de predicado diferente de.
    /// </summary>
    NotEqualPredicate,

    /// <summary>
    /// EN: Executes an equality predicate benchmark.
    /// PT-br: Executa um benchmark de predicado de igualdade.
    /// </summary>
    EqualPredicate,

    /// <summary>
    /// EN: Executes a greater-than predicate benchmark.
    /// PT-br: Executa um benchmark de predicado maior que.
    /// </summary>
    GreaterThanPredicate,

    /// <summary>
    /// EN: Executes a less-than predicate benchmark.
    /// PT-br: Executa um benchmark de predicado menor que.
    /// </summary>
    LessThanPredicate,

    /// <summary>
    /// EN: Executes a greater-than-or-equal predicate benchmark.
    /// PT-br: Executa um benchmark de predicado maior ou igual.
    /// </summary>
    GreaterThanOrEqualPredicate,

    /// <summary>
    /// EN: Executes a less-than-or-equal predicate benchmark.
    /// PT-br: Executa um benchmark de predicado menor ou igual.
    /// </summary>
    LessThanOrEqualPredicate,

    /// <summary>
    /// EN: Executes an IN subquery benchmark with a NULL branch.
    /// PT-br: Executa um benchmark de subconsulta IN com ramo NULL.
    /// </summary>
    NotInSubqueryNull,

    /// <summary>
    /// EN: Counts all rows returned by a select over the seeded table.
    /// PT-br: Conta todas as linhas retornadas por um select na tabela semeada.
    /// </summary>
    AllRowsCount,

    /// <summary>
    /// EN: Captures the full rowset snapshot returned by a select over the seeded table.
    /// PT-br: Captura o snapshot completo do conjunto de linhas retornado por um select na tabela semeada.
    /// </summary>
    AllRowsSnapshot,

    /// <summary>
    /// EN: Executes a CTE with a MATERIALIZED hint.
    /// PT-br: Executa uma CTE com hint MATERIALIZED.
    /// </summary>
    CteMaterializedHint,

    /// <summary>
    /// EN: Executes a DISTINCT ON projection.
    /// PT-br: Executa uma projecao DISTINCT ON.
    /// </summary>
    DistinctOnProjection,

    /// <summary>
    /// EN: Executes an ORDER BY Name matrix.
    /// PT-br: Executa uma matriz ORDER BY Name.
    /// </summary>
    OrderByNameMatrix,

    /// <summary>
    /// EN: Executes an ORDER BY ordinal matrix.
    /// PT-br: Executa uma matriz ORDER BY ordinal.
    /// </summary>
    OrderByOrdinalMatrix,

    /// <summary>
    /// EN: Executes an ORDER BY Name descending matrix.
    /// PT-br: Executa uma matriz ORDER BY Name descendente.
    /// </summary>
    OrderByNameDescendingMatrix,

    /// <summary>
    /// EN: Executes a paginated name matrix.
    /// PT-br: Executa uma matriz paginada por nome.
    /// </summary>
    NamePaginationMatrix,

    /// <summary>
    /// EN: Executes a GROUP BY name-initial matrix.
    /// PT-br: Executa uma matriz GROUP BY de inicial do nome.
    /// </summary>
    GroupByNameInitialMatrix,

    /// <summary>
    /// EN: Executes a GROUP BY name-having matrix.
    /// PT-br: Executa uma matriz GROUP BY com HAVING por nome.
    /// </summary>
    GroupByNameHavingMatrix,

    /// <summary>
    /// EN: Executes a GROUP BY ordinal matrix.
    /// PT-br: Executa uma matriz GROUP BY por ordinal.
    /// </summary>
    GroupByOrdinalMatrix,

    /// <summary>
    /// EN: Executes a DISTINCT order-by-ordinal matrix.
    /// PT-br: Executa uma matriz DISTINCT com ORDER BY ordinal.
    /// </summary>
    DistinctOrderByOrdinalMatrix,

    /// <summary>
    /// EN: Executes a DISTINCT text-filter order-by-ordinal matrix.
    /// PT-br: Executa uma matriz DISTINCT com filtro de texto e ORDER BY ordinal.
    /// </summary>
    DistinctLikeOrderByOrdinalMatrix,

    /// <summary>
    /// EN: Executes a joined typed-expression matrix.
    /// PT-br: Executa uma matriz com expressoes tipadas em join.
    /// </summary>
    JoinTypedExpressionMatrix,

    /// <summary>
    /// EN: Executes a joined null-aggregate matrix.
    /// PT-br: Executa uma matriz agregada com null em join.
    /// </summary>
    JoinNullAggregateMatrix,

    /// <summary>
    /// EN: Executes a joined cast-null matrix.
    /// PT-br: Executa uma matriz com cast e null em join.
    /// </summary>
    JoinCastNullMatrix,

    /// <summary>
    /// EN: Executes a joined cast-text comparison matrix.
    /// PT-br: Executa uma matriz com cast e comparacao textual em join.
    /// </summary>
    JoinCastTextComparisonMatrix,

    /// <summary>
    /// EN: Executes a joined HAVING cast matrix.
    /// PT-br: Executa uma matriz HAVING com cast em join.
    /// </summary>
    JoinHavingCastMatrix,

    /// <summary>
    /// EN: Executes a joined length-and-numeric matrix.
    /// PT-br: Executa uma matriz com comprimento e numericos em join.
    /// </summary>
    JoinLengthNumericMatrix,

    /// <summary>
    /// EN: Executes a joined text-case-length matrix.
    /// PT-br: Executa uma matriz com caixa, texto e comprimento em join.
    /// </summary>
    JoinTextCaseLengthMatrix,

    /// <summary>
    /// EN: Executes a joined distinct-case matrix.
    /// PT-br: Executa uma matriz DISTINCT com CASE em join.
    /// </summary>
    JoinDistinctCaseMatrix,

    /// <summary>
    /// EN: Executes a joined distinct-HAVING matrix.
    /// PT-br: Executa uma matriz DISTINCT com HAVING em join.
    /// </summary>
    JoinDistinctHavingMatrix,

    /// <summary>
    /// EN: Executes a STRING_SPLIT projection.
    /// PT-br: Executa uma projecao STRING_SPLIT.
    /// </summary>
    StringSplitProjection,

    /// <summary>
    /// EN: Executes a FOR JSON PATH projection.
    /// PT-br: Executa uma projecao FOR JSON PATH.
    /// </summary>
    ForJsonPathProjection,

    /// <summary>
    /// EN: Executes a joined window and temporal matrix.
    /// PT-br: Executa uma matriz com join, janela e temporal.
    /// </summary>
    JoinWindowTemporalMatrix,

    /// <summary>
    /// EN: Executes a joined temporal matrix.
    /// PT-br: Executa uma matriz temporal em join.
    /// </summary>
    JoinTemporalMatrix,

    /// <summary>
    /// EN: Executes a joined window matrix.
    /// PT-br: Executa uma matriz de janela em join.
    /// </summary>
    JoinWindowMatrix,

    /// <summary>
    /// EN: Executes a joined window and aggregate temporal matrix.
    /// PT-br: Executa uma matriz com janela, agregacao e temporal.
    /// </summary>
    JoinWindowAggregateTemporalMatrix,

    /// <summary>
    /// EN: Executes the APPLY and temporal composite benchmark.
    /// PT-br: Executa o benchmark composto de APPLY e temporal.
    /// </summary>
    ApplyTemporalComposite,

    /// <summary>
    /// EN: Executes the APPLY and window-temporal composite benchmark.
    /// PT-br: Executa o benchmark composto de APPLY e janela-temporal.
    /// </summary>
    ApplyWindowTemporalComposite,

    /// <summary>
    /// EN: Updates one row by primary key.
    /// PT-br: Atualiza uma linha pela chave primaria.
    /// </summary>
    UpdateByPk,

    /// <summary>
    /// EN: Executes an update/delete round trip.
    /// PT-br: Executa um ciclo de update/delete.
    /// </summary>
    UpdateDeleteRoundTrip,

    /// <summary>
    /// EN: Executes the parameter update/delete round-trip benchmark.
    /// PT-br: Executa o benchmark de roundtrip de update/delete com parametros.
    /// </summary>
    ParameterUpdateDeleteRoundTrip,

    /// <summary>
    /// EN: Deletes one row by primary key.
    /// PT-br: Remove uma linha pela chave primaria.
    /// </summary>
    DeleteByPk,

    /// <summary>
    /// EN: Commits a transaction.
    /// PT-br: Confirma uma transacao.
    /// </summary>
    TransactionCommit,

    /// <summary>
    /// EN: Rolls back a transaction.
    /// PT-br: Desfaz uma transacao.
    /// </summary>
    TransactionRollback,

    /// <summary>
    /// EN: Executes an update/delete workflow inside a transaction.
    /// PT-br: Executa um fluxo de update/delete dentro de uma transacao.
    /// </summary>
    TransactionalUpdateDeleteCommit,

    /// <summary>
    /// EN: Executes typed parameter inserts inside a committed transaction.
    /// PT-br: Executa inserts tipados com parametros dentro de uma transacao confirmada.
    /// </summary>
    ParameterTransactionCommit,

    /// <summary>
    /// EN: Executes typed parameter inserts inside a rolled-back transaction.
    /// PT-br: Executa inserts tipados com parametros dentro de uma transacao revertida.
    /// </summary>
    ParameterTransactionRollback,

    /// <summary>
    /// EN: Creates a savepoint.
    /// PT-br: Cria um savepoint.
    /// </summary>
    SavepointCreate,

    /// <summary>
    /// EN: Rolls back to a savepoint.
    /// PT-br: Faz rollback para um savepoint.
    /// </summary>
    RollbackToSavepoint,

    /// <summary>
    /// EN: Releases a savepoint.
    /// PT-br: Libera um savepoint.
    /// </summary>
    ReleaseSavepoint,

    /// <summary>
    /// EN: Executes a nested savepoint flow.
    /// PT-br: Executa um fluxo com savepoints aninhados.
    /// </summary>
    NestedSavepointFlow,

    /// <summary>
    /// EN: Executes the provider-specific upsert path.
    /// PT-br: Executa o caminho de upsert especifico do provedor.
    /// </summary>
    Upsert,

    /// <summary>
    /// EN: Executes the merge insert-then-update benchmark.
    /// PT-br: Executa o benchmark de merge de inserir e depois atualizar.
    /// </summary>
    MergeInsertThenUpdate,

    /// <summary>
    /// EN: Executes the upsert insert-then-update benchmark.
    /// PT-br: Executa o benchmark de upsert de inserir e depois atualizar.
    /// </summary>
    UpsertInsertThenUpdate,

    /// <summary>
    /// EN: Reads the next value from a sequence.
    /// PT-br: Le o proximo valor de uma sequencia.
    /// </summary>
    SequenceNextValue,

    /// <summary>
    /// EN: Reads the current value from a sequence.
    /// PT-br: Le o valor atual de uma sequencia.
    /// </summary>
    SequenceCurrentValue,

    /// <summary>
    /// EN: Inserts rows using sequence-generated keys and reads them back.
    /// PT-br: Insere linhas usando chaves geradas por sequence e as le de volta.
    /// </summary>
    SequenceInsertRoundTrip,

    /// <summary>
    /// EN: Inserts rows using a sequence expression in the values clause.
    /// PT-br: Insere linhas usando uma expressao de sequence na clausula VALUES.
    /// </summary>
    SequenceInsertExpression,

    /// <summary>
    /// EN: Projects the next sequence value in a select query.
    /// PT-br: Projeta o proximo valor da sequence em uma consulta select.
    /// </summary>
    SequenceSelectProjection,

    /// <summary>
    /// EN: Uses a sequence expression inside a filtered query.
    /// PT-br: Usa uma expressao de sequence dentro de uma consulta filtrada.
    /// </summary>
    SequenceExpressionFilter,

    /// <summary>
    /// EN: Evaluates sequence values inside CASE and WHERE predicates.
    /// PT-br: Avalia valores de sequence dentro de predicados CASE e WHERE.
    /// </summary>
    SequenceCaseWhereMatrix,

    /// <summary>
    /// EN: Combines sequence values with temporal expressions.
    /// PT-br: Combina valores de sequence com expressoes temporais.
    /// </summary>
    SequenceTemporalMatrix,

    /// <summary>
    /// EN: Joins sequence-driven rows and aggregates the result.
    /// PT-br: Faz join de linhas guiadas por sequence e agrega o resultado.
    /// </summary>
    SequenceJoinAggregate,

    /// <summary>
    /// EN: Executes a batch insert with ten statements.
    /// PT-br: Executa um insert em lote com dez comandos.
    /// </summary>
    BatchInsert10,

    /// <summary>
    /// EN: Executes a batch insert with one hundred statements.
    /// PT-br: Executa um insert em lote com cem comandos.
    /// </summary>
    BatchInsert100,

    /// <summary>
    /// EN: Executes a mixed read/write batch.
    /// PT-br: Executa um lote misto de leitura e escrita.
    /// </summary>
    BatchMixedReadWrite,

    /// <summary>
    /// EN: Executes a scalar batch flow.
    /// PT-br: Executa um fluxo de lote escalar.
    /// </summary>
    BatchScalar,

    /// <summary>
    /// EN: Executes a non-query batch flow.
    /// PT-br: Executa um fluxo de lote sem resultado.
    /// </summary>
    BatchNonQuery,

    /// <summary>
    /// EN: Executes string aggregation.
    /// PT-br: Executa agregacao de strings.
    /// </summary>
    StringAggregate,

    /// <summary>
    /// EN: Executes ordered string aggregation.
    /// PT-br: Executa agregacao de strings ordenada.
    /// </summary>
    StringAggregateOrdered,

    /// <summary>
    /// EN: Executes a scalar date/time query.
    /// PT-br: Executa uma consulta escalar de data/hora.
    /// </summary>
    DateScalar,

    /// <summary>
    /// EN: Executes the shared math functions benchmark.
    /// PT-br: Executa o benchmark compartilhado de funcoes matematicas.
    /// </summary>
    MathFunctions,

    /// <summary>
    /// EN: Executes the shared math log-base benchmark.
    /// PT-br: Executa o benchmark compartilhado de logaritmo com base explicita.
    /// </summary>
    MathLogBaseFunction,

    /// <summary>
    /// EN: Executes the shared math log2 benchmark.
    /// PT-br: Executa o benchmark compartilhado de logaritmo de base 2.
    /// </summary>
    MathLog2Function,

    /// <summary>
    /// EN: Executes the shared math pi benchmark.
    /// PT-br: Executa o benchmark compartilhado de pi.
    /// </summary>
    MathPiFunction,

    /// <summary>
    /// EN: Executes the shared math random benchmark.
    /// PT-br: Executa o benchmark compartilhado de numero aleatorio.
    /// </summary>
    MathRandFunction,

    /// <summary>
    /// EN: Executes the shared math remainder benchmark.
    /// PT-br: Executa o benchmark compartilhado de resto.
    /// </summary>
    MathRemainderFunction,

    /// <summary>
    /// EN: Executes the shared math truncation benchmark.
    /// PT-br: Executa o benchmark compartilhado de truncamento numerico.
    /// </summary>
    MathTruncFunction,

    /// <summary>
    /// EN: Executes the shared math cotangent benchmark.
    /// PT-br: Executa o benchmark compartilhado de cotangente.
    /// </summary>
    MathCotFunction,

    /// <summary>
    /// EN: Executes the MySQL utility math benchmark.
    /// PT-br: Executa o benchmark de utilitarios matematicos da familia MySQL.
    /// </summary>
    MySqlUtilityMathFunctions,

    /// <summary>
    /// EN: Executes the shared greatest/least/mod benchmark.
    /// PT-br: Executa o benchmark compartilhado de greatest/least/mod.
    /// </summary>
    GreatestLeastModFunctions,

    /// <summary>
    /// EN: Executes the DB2 alias math benchmark.
    /// PT-br: Executa o benchmark de aliases matematicos do DB2.
    /// </summary>
    Db2AliasMathFunctions,

    /// <summary>
    /// EN: Executes the Firebird alias math benchmark.
    /// PT-br: Executa o benchmark de aliases matematicos do Firebird.
    /// </summary>
    FirebirdAliasMathFunctions,

    /// <summary>
    /// EN: Executes the shared transcendental math benchmark.
    /// PT-br: Executa o benchmark compartilhado de matematica transcendental.
    /// </summary>
    MathTranscendentalFunctions,

    /// <summary>
    /// EN: Executes the SQL Server FORMAT benchmark.
    /// PT-br: Executa o benchmark FORMAT do SQL Server.
    /// </summary>
    Format,

    /// <summary>
    /// EN: Executes the SQL Server FORMATMESSAGE benchmark.
    /// PT-br: Executa o benchmark FORMATMESSAGE do SQL Server.
    /// </summary>
    FormatMessage,

    /// <summary>
    /// EN: Executes the SQL Server ISJSON benchmark.
    /// PT-br: Executa o benchmark ISJSON do SQL Server.
    /// </summary>
    IsJson,

    /// <summary>
    /// EN: Executes the SQL Server STRING_ESCAPE benchmark.
    /// PT-br: Executa o benchmark STRING_ESCAPE do SQL Server.
    /// </summary>
    StringEscape,

    /// <summary>
    /// EN: Executes the SQL Server TRANSLATE benchmark.
    /// PT-br: Executa o benchmark TRANSLATE do SQL Server.
    /// </summary>
    Translate,

    /// <summary>
    /// EN: Reads a JSON scalar value.
    /// PT-br: Le um valor escalar de JSON.
    /// </summary>
    JsonScalarRead,

    /// <summary>
    /// EN: Reads a nested JSON path value.
    /// PT-br: Le um valor de caminho JSON aninhado.
    /// </summary>
    JsonPathRead,

    /// <summary>
    /// EN: Reads a JSON path value that is missing in the document.
    /// PT-br: Le um valor de caminho JSON ausente no documento.
    /// </summary>
    JsonMissingPathRead,

    /// <summary>
    /// EN: Reads a missing JSON path value and returns null.
    /// PT-br: Le um valor de caminho JSON ausente e retorna nulo.
    /// </summary>
    JsonMissingPathReturnsNull,

    /// <summary>
    /// EN: Reads a raw root fragment with JSON_QUERY.
    /// PT-br: Le um fragmento bruto de raiz com JSON_QUERY.
    /// </summary>
    JsonQueryRootFragment,

    /// <summary>
    /// EN: Replaces a nested JSON value with JSON_MODIFY.
    /// PT-br: Substitui um valor JSON aninhado com JSON_MODIFY.
    /// </summary>
    JsonModifyReplace,

    /// <summary>
    /// EN: Executes a JSON typed field matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de campos tipados com JSON.
    /// </summary>
    JsonTypedFieldMatrix,

    /// <summary>
    /// EN: Executes a json_each benchmark over a JSON array.
    /// PT-br: Executa um benchmark json_each sobre um array JSON.
    /// </summary>
    JsonEachFromArray,

    /// <summary>
    /// EN: Executes a json_each benchmark over a JSON object.
    /// PT-br: Executa um benchmark json_each sobre um objeto JSON.
    /// </summary>
    JsonEachFromObject,

    /// <summary>
    /// EN: Executes a json_tree benchmark over JSON.
    /// PT-br: Executa um benchmark json_tree sobre JSON.
    /// </summary>
    JsonTreeStructure,

    /// <summary>
    /// EN: Executes an OPENJSON benchmark over a JSON array.
    /// PT-br: Executa um benchmark OPENJSON sobre um array JSON.
    /// </summary>
    OpenJsonArray,

    /// <summary>
    /// EN: Executes a current timestamp scalar query.
    /// PT-br: Executa uma consulta escalar de timestamp atual.
    /// </summary>
    TemporalCurrentTimestamp,

    /// <summary>
    /// EN: Executes a temporal date-add query.
    /// PT-br: Executa uma consulta temporal de soma de data.
    /// </summary>
    TemporalDateAdd,

    /// <summary>
    /// EN: Executes a temporal current-time predicate query.
    /// PT-br: Executa uma consulta temporal com predicado de tempo atual.
    /// </summary>
    TemporalNowWhere,

    /// <summary>
    /// EN: Executes a temporal current-time ordering query.
    /// PT-br: Executa uma consulta temporal com ordenacao por tempo atual.
    /// </summary>
    TemporalNowOrderBy,

    /// <summary>
    /// EN: Executes the scalar temporal matrix benchmark.
    /// PT-br: Executa a matriz temporal escalar.
    /// </summary>
    ScalarTemporalMatrix,

    /// <summary>
    /// EN: Executes a temporal field matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de campos temporais.
    /// </summary>
    TemporalFieldMatrix,

    /// <summary>
    /// EN: Executes a temporal comparison matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de comparacao temporal.
    /// </summary>
    TemporalComparisonMatrix,

    /// <summary>
    /// EN: Executes a temporal arithmetic matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de aritmetica temporal.
    /// </summary>
    TemporalArithmeticMatrix,

    /// <summary>
    /// EN: Executes a DATETRUNC benchmark.
    /// PT-br: Executa um benchmark DATETRUNC.
    /// </summary>
    TemporalDateTrunc,

    /// <summary>
    /// EN: Executes a time-zone offset benchmark.
    /// PT-br: Executa um benchmark de fuso horario.
    /// </summary>
    TemporalTimeZoneOffset,

    /// <summary>
    /// EN: Executes a FROMPARTS benchmark.
    /// PT-br: Executa um benchmark FROMPARTS.
    /// </summary>
    TemporalFromParts,

    /// <summary>
    /// EN: Executes an EOMONTH benchmark.
    /// PT-br: Executa um benchmark EOMONTH.
    /// </summary>
    TemporalEndOfMonth,

    /// <summary>
    /// EN: Executes a DATEDIFF_BIG benchmark.
    /// PT-br: Executa um benchmark DATEDIFF_BIG.
    /// </summary>
    TemporalDateDiffBig,

    /// <summary>
    /// EN: Executes SQL Server metadata functions.
    /// PT-br: Executa funcoes de metadata do SQL Server.
    /// </summary>
    SqlServerMetadataFunctions,

    /// <summary>
    /// EN: Executes SCOPE_IDENTITY.
    /// PT-br: Executa SCOPE_IDENTITY.
    /// </summary>
    ScopeIdentity,

    /// <summary>
    /// EN: Executes SQL Server system functions.
    /// PT-br: Executa funcoes de sistema do SQL Server.
    /// </summary>
    SqlServerSystemFunctions,

    /// <summary>
    /// EN: Executes SQL Server special functions.
    /// PT-br: Executa funcoes especiais do SQL Server.
    /// </summary>
    SqlServerSpecialFunctions,

    /// <summary>
    /// EN: Executes SQL Server context functions.
    /// PT-br: Executa funcoes de contexto do SQL Server.
    /// </summary>
    SqlServerContextFunctions,

    /// <summary>
    /// EN: Executes SQL Server transaction state functions.
    /// PT-br: Executa funcoes de estado de transacao do SQL Server.
    /// </summary>
    SqlServerTransactionStateFunctions,

    /// <summary>
    /// EN: Executes SQL Server session functions.
    /// PT-br: Executa funcoes de sessao do SQL Server.
    /// </summary>
    SqlServerSessionFunctions,

    /// <summary>
    /// EN: Executes SQL Server string functions.
    /// PT-br: Executa funcoes de string do SQL Server.
    /// </summary>
    StringBasicFunctions,

    /// <summary>
    /// EN: Executes SQL Server string utility functions.
    /// PT-br: Executa funcoes de utilitarios de string do SQL Server.
    /// </summary>
    StringUtilityFunctions,

    /// <summary>
    /// EN: Executes SQL Server string metadata functions.
    /// PT-br: Executa funcoes de metadados de string do SQL Server.
    /// </summary>
    StringMetadataFunctions,

    /// <summary>
    /// EN: Executes SQL Server PARSE-family functions.
    /// PT-br: Executa funcoes da familia PARSE do SQL Server.
    /// </summary>
    ParseFamily,

    /// <summary>
    /// EN: Executes SQL Server SOUNDEX functions.
    /// PT-br: Executa funcoes SOUNDEX do SQL Server.
    /// </summary>
    Soundex,

    /// <summary>
    /// EN: Executes SQL Server compression functions.
    /// PT-br: Executa funcoes de compressao do SQL Server.
    /// </summary>
    Compression,

    /// <summary>
    /// EN: Executes APPROX_COUNT_DISTINCT.
    /// PT-br: Executa APPROX_COUNT_DISTINCT.
    /// </summary>
    ApproxCountDistinct,

    /// <summary>
    /// EN: Executes PERCENTILE_CONT and PERCENTILE_DISC.
    /// PT-br: Executa PERCENTILE_CONT e PERCENTILE_DISC.
    /// </summary>
    PercentileAggregateFunctions,

    /// <summary>
    /// EN: Executes SQL Server aggregate functions.
    /// PT-br: Executa funcoes de agregacao do SQL Server.
    /// </summary>
    SqlServerAggregateFunctions,

    /// <summary>
    /// EN: Executes a distinct string aggregation query.
    /// PT-br: Executa uma consulta de agregacao distinta de strings.
    /// </summary>
    StringAggregateDistinct,

    /// <summary>
    /// EN: Executes a custom-separator string aggregation query.
    /// PT-br: Executa uma consulta de agregacao de strings com separador customizado.
    /// </summary>
    StringAggregateCustomSeparator,

    /// <summary>
    /// EN: Executes a large-group string aggregation query.
    /// PT-br: Executa uma consulta de agregacao de strings em grupo grande.
    /// </summary>
    StringAggregateLargeGroup,

    /// <summary>
    /// EN: Executes a string aggregation summary matrix.
    /// PT-br: Executa uma matriz resumo de agregacao de strings.
    /// </summary>
    StringAggregateSummaryMatrix,

    /// <summary>
    /// EN: Executes a grouped string aggregation matrix.
    /// PT-br: Executa uma matriz agrupada de agregacao de strings.
    /// </summary>
    StringAggregateGroupCaseMatrix,

    /// <summary>
    /// EN: Executes a string aggregation summary matrix alias.
    /// PT-br: Executa um alias da matriz resumo de agregacao de strings.
    /// </summary>
    StringAggregationSummaryMatrix,

    /// <summary>
    /// EN: Executes a grouped string aggregation matrix alias.
    /// PT-br: Executa um alias da matriz agrupada de agregacao de strings.
    /// </summary>
    StringAggregationGroupCaseMatrix,

    /// <summary>
    /// EN: Executes the string aggregation variants benchmark.
    /// PT-br: Executa o benchmark das variantes de agregacao de strings.
    /// </summary>
    StringAggregationVariants,

    /// <summary>
    /// EN: Reads rowcount after select.
    /// PT-br: Le a contagem de linhas apos select.
    /// </summary>
    RowCountAfterSelect,

    /// <summary>
    /// EN: Executes a simple CTE query.
    /// PT-br: Executa uma consulta simples com CTE.
    /// </summary>
    CteSimple,

    /// <summary>
    /// EN: Executes a ROW_NUMBER window query.
    /// PT-br: Executa uma consulta de janela com ROW_NUMBER.
    /// </summary>
    WindowRowNumber,

    /// <summary>
    /// EN: Executes a LAG window query.
    /// PT-br: Executa uma consulta de janela com LAG.
    /// </summary>
    WindowLag,

    /// <summary>
    /// EN: Executes a LEAD window query.
    /// PT-br: Executa uma consulta de janela com LEAD.
    /// </summary>
    WindowLead,

    /// <summary>
    /// EN: Executes a ranking window query with dense-rank and rank.
    /// PT-br: Executa uma consulta de janela de ranking com dense-rank e rank.
    /// </summary>
    WindowRankDenseRank,

    /// <summary>
    /// EN: Executes a FIRST_VALUE and LAST_VALUE window query.
    /// PT-br: Executa uma consulta de janela com FIRST_VALUE e LAST_VALUE.
    /// </summary>
    WindowFirstLastValue,

    /// <summary>
    /// EN: Executes an NTILE window query.
    /// PT-br: Executa uma consulta de janela com NTILE.
    /// </summary>
    WindowNtile,

    /// <summary>
    /// EN: Executes a PERCENT_RANK and CUME_DIST window query.
    /// PT-br: Executa uma consulta de janela com PERCENT_RANK e CUME_DIST.
    /// </summary>
    WindowPercentRankCumeDist,

    /// <summary>
    /// EN: Executes an NTH_VALUE window query.
    /// PT-br: Executa uma consulta de janela com NTH_VALUE.
    /// </summary>
    WindowNthValue,

    /// <summary>
    /// EN: Executes an EXISTS predicate query.
    /// PT-br: Executa uma consulta com predicado EXISTS.
    /// </summary>
    SelectExistsPredicate,

    /// <summary>
    /// EN: Executes a NOT EXISTS predicate query.
    /// PT-br: Executa uma consulta com predicado NOT EXISTS.
    /// </summary>
    SelectNotExistsPredicate,

    /// <summary>
    /// EN: Executes a LEFT JOIN anti-join query.
    /// PT-br: Executa uma consulta anti-join com LEFT JOIN.
    /// </summary>
    SelectLeftJoinAntiJoin,

    /// <summary>
    /// EN: Executes a correlated subquery count flow.
    /// PT-br: Executa um fluxo com subconsulta correlacionada de contagem.
    /// </summary>
    SelectCorrelatedCount,

    /// <summary>
    /// EN: Executes a scalar subquery and CASE matrix query.
    /// PT-br: Executa uma consulta matricial com subconsulta escalar e CASE.
    /// </summary>
    SelectScalarCaseMatrix,

    /// <summary>
    /// EN: Executes a GROUP BY HAVING query.
    /// PT-br: Executa uma consulta GROUP BY HAVING.
    /// </summary>
    GroupByHaving,

    /// <summary>
    /// EN: Executes a UNION ALL projection query.
    /// PT-br: Executa uma consulta de projecao com UNION ALL.
    /// </summary>
    UnionAllProjection,

    /// <summary>
    /// EN: Executes a UNION projection query.
    /// PT-br: Executa uma consulta de projeção com UNION.
    /// </summary>
    UnionDistinctProjection,

    /// <summary>
    /// EN: Executes a DISTINCT projection query.
    /// PT-br: Executa uma consulta de projeacao DISTINCT.
    /// </summary>
    DistinctProjection,

    /// <summary>
    /// EN: Executes a multi-join aggregate query.
    /// PT-br: Executa uma consulta agregada com multiplos joins.
    /// </summary>
    MultiJoinAggregate,

    /// <summary>
    /// EN: Executes a scalar subquery projection.
    /// PT-br: Executa uma projecao com subconsulta escalar.
    /// </summary>
    SelectScalarSubquery,

    /// <summary>
    /// EN: Executes an IN subquery predicate.
    /// PT-br: Executa um predicado IN com subconsulta.
    /// </summary>
    SelectInSubquery,

    /// <summary>
    /// EN: Executes a NOT IN subquery predicate.
    /// PT-br: Executa um predicado NOT IN com subconsulta.
    /// </summary>
    SelectNotInSubquery,

    /// <summary>
    /// EN: Executes a combined BETWEEN, LIKE, and ORDER BY query.
    /// PT-br: Executa uma consulta combinada com BETWEEN, LIKE e ORDER BY.
    /// </summary>
    SelectBetweenLikeOrderByMatrix,

    /// <summary>
    /// EN: Executes a CROSS APPLY style projection.
    /// PT-br: Executa uma projecao no estilo CROSS APPLY.
    /// </summary>
    CrossApplyProjection,

    /// <summary>
    /// EN: Executes an OUTER APPLY style projection.
    /// PT-br: Executa uma projecao no estilo OUTER APPLY.
    /// </summary>
    OuterApplyProjection,

    /// <summary>
    /// EN: Executes a paged name projection query.
    /// PT-br: Executa uma consulta de projeção paginada de nomes.
    /// </summary>
    PagedNameProjection,

    /// <summary>
    /// EN: Reads rowcount after insert.
    /// PT-br: Le a contagem de linhas apos insert.
    /// </summary>
    RowCountAfterInsert,

    /// <summary>
    /// EN: Reads rowcount in a batch insert flow.
    /// PT-br: Le a contagem de linhas em um fluxo de insert em lote.
    /// </summary>
    BatchRowCountInBatch,

    /// <summary>
    /// EN: Reads rowcount after update.
    /// PT-br: Le a contagem de linhas apos update.
    /// </summary>
    RowCountAfterUpdate,

    /// <summary>
    /// EN: Benchmark entry for batch reader multi-result.
    /// PT-br: Entrada de benchmark para batch reader multi-result.
    /// </summary>
    BatchReaderMultiResult,

    /// <summary>
    /// EN: Benchmark entry for batch transaction control.
    /// PT-br: Entrada de benchmark para batch transaction control.
    /// </summary>
    BatchTransactionControl,

    /// <summary>
    /// EN: Benchmark entry for parse simple select.
    /// PT-br: Entrada de benchmark para parse simple select.
    /// </summary>
    ParseSimpleSelect,

    /// <summary>
    /// EN: Benchmark entry for parse complex join.
    /// PT-br: Entrada de benchmark para parse complex join.
    /// </summary>
    ParseComplexJoin,

    /// <summary>
    /// EN: Benchmark entry for parse insert returning.
    /// PT-br: Entrada de benchmark para parse insert returning.
    /// </summary>
    ParseInsertReturning,

    /// <summary>
    /// EN: Benchmark entry for parse on conflict do update.
    /// PT-br: Entrada de benchmark para parse on conflict do update.
    /// </summary>
    ParseOnConflictDoUpdate,

    /// <summary>
    /// EN: Benchmark entry for parse JSON extract.
    /// PT-br: Entrada de benchmark para parse JSON extract.
    /// </summary>
    ParseJsonExtract,

    /// <summary>
    /// EN: Benchmark entry for parse string aggregate within group.
    /// PT-br: Entrada de benchmark para parse string aggregate within group.
    /// </summary>
    ParseStringAggregateWithinGroup,

    /// <summary>
    /// EN: Benchmark entry for parse auto-dialect TOP/LIMIT/FETCH.
    /// PT-br: Entrada de benchmark para parse auto-dialect TOP/LIMIT/FETCH.
    /// </summary>
    ParseAutoDialectTopLimitFetch,

    /// <summary>
    /// EN: Benchmark entry for parse multi-statement batch.
    /// PT-br: Entrada de benchmark para parse multi-statement batch.
    /// </summary>
    ParseMultiStatementBatch,

    /// <summary>
    /// EN: Benchmark entry for JSON insert cast.
    /// PT-br: Entrada de benchmark para JSON insert cast.
    /// </summary>
    JsonInsertCast,

    /// <summary>
    /// EN: Executes the JSON insert cast benchmark and returns null when the provider does.
    /// PT-br: Executa o benchmark de insert e cast de JSON e retorna nulo quando o provedor retorna.
    /// </summary>
    JsonInsertCastReturnsNull,

    /// <summary>
    /// EN: Benchmark entry for rowcount in batch.
    /// PT-br: Entrada de benchmark para rowcount in batch.
    /// </summary>
    RowCountInBatch,

    /// <summary>
    /// EN: Benchmark entry for pivot count.
    /// PT-br: Entrada de benchmark para pivot count.
    /// </summary>
    PivotCount,

    /// <summary>
    /// EN: Benchmark entry for returning insert.
    /// PT-br: Entrada de benchmark para returning insert.
    /// </summary>
    ReturningInsert,

    /// <summary>
    /// EN: Benchmark entry for batch returning insert.
    /// PT-br: Entrada de benchmark para batch returning insert.
    /// </summary>
    BatchReturningInsert,

    /// <summary>
    /// EN: Benchmark entry for returning update.
    /// PT-br: Entrada de benchmark para returning update.
    /// </summary>
    ReturningUpdate,

    /// <summary>
    /// EN: Benchmark entry for merge basic.
    /// PT-br: Entrada de benchmark para merge basic.
    /// </summary>
    MergeBasic,

    /// <summary>
    /// EN: Benchmark entry for partition pruning select.
    /// PT-br: Entrada de benchmark para partition pruning select.
    /// </summary>
    PartitionPruningSelect,

    /// <summary>
    /// EN: Benchmark entry for execution plan.
    /// PT-br: Entrada de benchmark para execution plan.
    /// </summary>
    ExecutionPlan,

    /// <summary>
    /// EN: Benchmark entry for execution plan select.
    /// PT-br: Entrada de benchmark para execution plan select.
    /// </summary>
    ExecutionPlanSelect,

    /// <summary>
    /// EN: Benchmark entry for execution plan join.
    /// PT-br: Entrada de benchmark para execution plan join.
    /// </summary>
    ExecutionPlanJoin,

    /// <summary>
    /// EN: Benchmark entry for execution plan DML.
    /// PT-br: Entrada de benchmark para execution plan DML.
    /// </summary>
    ExecutionPlanDml,

    /// <summary>
    /// EN: Benchmark entry for debug trace select.
    /// PT-br: Entrada de benchmark para debug trace select.
    /// </summary>
    DebugTraceSelect,

    /// <summary>
    /// EN: Benchmark entry for debug trace batch.
    /// PT-br: Entrada de benchmark para debug trace batch.
    /// </summary>
    DebugTraceBatch,

    /// <summary>
    /// EN: Benchmark entry for debug trace JSON.
    /// PT-br: Entrada de benchmark para debug trace JSON.
    /// </summary>
    DebugTraceJson,

    /// <summary>
    /// EN: Benchmark entry for last execution plans history.
    /// PT-br: Entrada de benchmark para last execution plans history.
    /// </summary>
    LastExecutionPlansHistory,

    /// <summary>
    /// EN: Benchmark entry for temp table create and use.
    /// PT-br: Entrada de benchmark para temp table create and use.
    /// </summary>
    TempTableCreateAndUse,

    /// <summary>
    /// EN: Benchmark entry for temp table rollback.
    /// PT-br: Entrada de benchmark para temp table rollback.
    /// </summary>
    TempTableRollback,

    /// <summary>
    /// EN: Benchmark entry for temp table cross-connection isolation.
    /// PT-br: Entrada de benchmark para temp table cross-connection isolation.
    /// </summary>
    TempTableCrossConnectionIsolation,

    /// <summary>
    /// EN: Benchmark entry for reset volatile data.
    /// PT-br: Entrada de benchmark para reset volatile data.
    /// </summary>
    ResetVolatileData,

    /// <summary>
    /// EN: Benchmark entry for reset all volatile data.
    /// PT-br: Entrada de benchmark para reset all volatile data.
    /// </summary>
    ResetAllVolatileData,

    /// <summary>
    /// EN: Benchmark entry for connection reopen after close.
    /// PT-br: Entrada de benchmark para connection reopen after close.
    /// </summary>
    ConnectionReopenAfterClose,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot export.
    /// PT-br: Entrada de benchmark para schema snapshot export.
    /// </summary>
    SchemaSnapshotExport,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot to JSON.
    /// PT-br: Entrada de benchmark para schema snapshot to JSON.
    /// </summary>
    SchemaSnapshotToJson,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot load JSON.
    /// PT-br: Entrada de benchmark para schema snapshot load JSON.
    /// </summary>
    SchemaSnapshotLoadJson,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot apply.
    /// PT-br: Entrada de benchmark para schema snapshot apply.
    /// </summary>
    SchemaSnapshotApply,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot round-trip.
    /// PT-br: Entrada de benchmark para schema snapshot round-trip.
    /// </summary>
    SchemaSnapshotRoundTrip,

    /// <summary>
    /// EN: Benchmark entry for schema snapshot compare.
    /// PT-br: Entrada de benchmark para schema snapshot compare.
    /// </summary>
    SchemaSnapshotCompare,

    /// <summary>
    /// EN: Benchmark entry for fluent schema build.
    /// PT-br: Entrada de benchmark para fluent schema build.
    /// </summary>
    FluentSchemaBuild,

    /// <summary>
    /// EN: Benchmark entry for fluent seed 100.
    /// PT-br: Entrada de benchmark para fluent seed 100.
    /// </summary>
    FluentSeed100,

    /// <summary>
    /// EN: Benchmark entry for fluent seed 1000.
    /// PT-br: Entrada de benchmark para fluent seed 1000.
    /// </summary>
    FluentSeed1000,

    /// <summary>
    /// EN: Benchmark entry for fluent scenario compose.
    /// PT-br: Entrada de benchmark para fluent scenario compose.
    /// </summary>
    FluentScenarioCompose,

    /// <summary>
    /// EN: Executes a parameter projection benchmark.
    /// PT-br: Executa um benchmark de projeção parametrizada.
    /// </summary>
    ParameterProjection,

    /// <summary>
    /// EN: Executes a parameterized single-row insert benchmark.
    /// PT-br: Executa um benchmark de insercao parametrizada de uma linha.
    /// </summary>
    ParameterInsertSingle,

    /// <summary>
    /// EN: Executes a parameter insert round-trip benchmark.
    /// PT-br: Executa o benchmark de roundtrip de insert com parametros.
    /// </summary>
    ParameterInsertRoundTrip,

    /// <summary>
    /// EN: Executes a parameter insert round-trip benchmark with null values.
    /// PT-br: Executa o benchmark de roundtrip de insert com parametros e valores nulos.
    /// </summary>
    ParameterInsertNullRoundTrip,

    /// <summary>
    /// EN: Executes a parameterized name lookup benchmark.
    /// PT-br: Executa um benchmark de consulta parametrizada por nome.
    /// </summary>
    ParameterSelectByNameMatrix,

    /// <summary>
    /// EN: Executes a parameterized id lookup benchmark.
    /// PT-br: Executa um benchmark de consulta parametrizada por id.
    /// </summary>
    ParameterSelectByIdMatrix,

    /// <summary>
    /// EN: Executes a typed parameter round-trip benchmark.
    /// PT-br: Executa um benchmark de roundtrip de parametros tipados.
    /// </summary>
    ParameterRoundTripMatrix,

    /// <summary>
    /// EN: Executes a typed parameter projection benchmark.
    /// PT-br: Executa um benchmark de projeção de parametros tipados.
    /// </summary>
    ParameterTypeMatrix,

    /// <summary>
    /// EN: Executes a typed date and currency parameter benchmark.
    /// PT-br: Executa um benchmark de data e moeda com parametros tipados.
    /// </summary>
    ParameterDateCurrencyMatrix,

    /// <summary>
    /// EN: Executes a typed field storage matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de armazenamento tipado.
    /// </summary>
    TypedFieldStorageMatrix,

    /// <summary>
    /// EN: Executes a typed field function matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de funcoes tipadas.
    /// </summary>
    TypedFieldFunctionMatrix,

    /// <summary>
    /// EN: Executes a typed field calculation matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de calculo tipado.
    /// </summary>
    TypedFieldCalculationMatrix,

    /// <summary>
    /// EN: Executes a typed field and function blend benchmark.
    /// PT-br: Executa um benchmark de mistura de campos tipados e funcoes.
    /// </summary>
    TypedFieldAndFunctionBlend,

    /// <summary>
    /// EN: Executes a typed field compound predicate matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de predicados compostos com campos tipados.
    /// </summary>
    TypedFieldCompoundPredicateMatrix,

    /// <summary>
    /// EN: Executes a typed field cast calculation matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de calculo com casts em campos tipados.
    /// </summary>
    TypedFieldCastCalculationMatrix,

    /// <summary>
    /// EN: Executes a typed field null comparison matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de comparacao com null em campos tipados.
    /// </summary>
    TypedFieldNullComparisonMatrix,

    /// <summary>
    /// EN: Executes a typed field text length matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de comprimento de texto em campos tipados.
    /// </summary>
    TypedFieldTextLengthMatrix,

    /// <summary>
    /// EN: Executes a typed field text case matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de caixa de texto em campos tipados.
    /// </summary>
    TypedFieldTextCaseMatrix,

    /// <summary>
    /// EN: Executes a typed field predicate matrix benchmark.
    /// PT-br: Executa um benchmark da matriz de predicados em campos tipados.
    /// </summary>
    TypedFieldPredicateMatrix,

    /// <summary>
    /// EN: Executes a stored procedure call benchmark.
    /// PT-br: Executa um benchmark de chamada de procedimento armazenado.
    /// </summary>
    StoredProcedureCall,

    /// <summary>
    /// EN: Executes a Firebird EXECUTE BLOCK benchmark with SQLSTATE handling.
    /// PT-br: Executa um benchmark Firebird de EXECUTE BLOCK com tratamento de SQLSTATE.
    /// </summary>
    ExecuteBlockSqlState23000,
}

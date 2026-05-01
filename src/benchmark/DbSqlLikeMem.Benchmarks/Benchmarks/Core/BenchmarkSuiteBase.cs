namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Defines the base BenchmarkDotNet suite lifecycle shared by all provider-specific suites.
/// PT-br: Define o ciclo de vida base do BenchmarkDotNet compartilhado por todas as suítes específicas de provedor.
/// </summary>
[MemoryDiagnoser]
public abstract class BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Gets the benchmark session instance created for the current suite.
    /// PT: Obtem a instancia de sessao de benchmark criada para a suite atual.
    /// </summary>
    protected IBenchmarkSession Session { get; private set; } = null!;

    /// <summary>
    /// EN: Creates the benchmark session used by the suite.
    /// PT: Cria a sessao de benchmark usada pela suite.
    /// </summary>
    protected abstract IBenchmarkSession CreateSession();

    private bool _sessionReady;
    private Exception? _setupException;

    /// <summary>
    /// EN: Prepares the benchmark session before the runs start.
    /// PT: Prepara a sessao de benchmark antes do inicio das execucoes.
    /// </summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        Session = CreateSession();
        try
        {
            Session.Initialize();
            _sessionReady = true;
            _setupException = null;
        }
        catch (Exception ex)
        {
            _sessionReady = false;
            _setupException = ex;
            LogSetupIssue(ex);
        }
    }

    /// <summary>
    /// EN: Releases the benchmark session after the runs finish.
    /// PT: Libera a sessao de benchmark depois que as execucoes terminam.
    /// </summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Session.Dispose();
    }

    /// <summary>
    /// EN: Prepares the benchmark session before the runs start.
    /// PT: Prepara a sessao de benchmark antes do inicio das execucoes.
    /// </summary>
    [IterationSetup]
    public void IterationSetup()
    {
    }

    /// <summary>
    /// EN: Releases the benchmark session after the runs finish.
    /// PT: Libera a sessao de benchmark depois que as execucoes terminam.
    /// </summary>
    [IterationCleanup]
    public void IterationCleanup()
    {
    }


    /// <summary>
    /// EN: Executes one benchmark feature through the current session.
    /// PT: Executa um recurso de benchmark pela sessao atual.
    /// </summary>
    public void Run(BenchmarkFeatureId feature)
    {
        if (!_sessionReady)
        {
            var reason = _setupException?.GetBaseException()?.ToString() ?? "Session not initialized.";
            LogBenchmarkIssue(
                feature,
                new InvalidOperationException($"Session not initialized. Setup failure: {reason}"));
            return;
        }

        Session.Execute(feature);
    }

    /// <summary>
    /// EN: Executes a connection-open benchmark.
    /// PT: Executa um benchmark de abertura de conexao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ConnectionOpen() => Run(BenchmarkFeatureId.ConnectionOpen);

    /// <summary>
    /// EN: Executes a schema-creation benchmark.
    /// PT: Executa um benchmark de criacao de esquema.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void CreateSchema() => Run(BenchmarkFeatureId.CreateSchema);

    /// <summary>
    /// EN: Executes a table-creation benchmark.
    /// PT: Executa um benchmark de criacao de tabela.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void CreateTable() => Run(BenchmarkFeatureId.CreateTable);

    /// <summary>
    /// EN: Executes a benchmark that creates the users and orders tables with a foreign key.
    /// PT: Executa um benchmark que cria as tabelas de usuarios e pedidos com chave estrangeira.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void CreateTableWithFK() => Run(BenchmarkFeatureId.CreateTableWithFK);

    /// <summary>
    /// EN: Executes a benchmark that creates the foreign-key tables and inserts a referenced row.
    /// PT: Executa um benchmark que cria as tabelas com chave estrangeira e insere uma linha referenciada.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void CreateTableWithFKInsert() => Run(BenchmarkFeatureId.CreateTableWithFKInsert);

    /// <summary>
    /// EN: Executes the insert-in-table-with-FK benchmark.
    /// PT: Executa o benchmark de insert na tabela com chave estrangeira.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void InsertInTableWithFK() => Run(BenchmarkFeatureId.InsertInTableWithFK);

    /// <summary>
    /// EN: Executes a benchmark that drops the users table created by the DDL workflow.
    /// PT: Executa um benchmark que remove a tabela de usuarios criada pelo fluxo de DDL.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void DropTable() => Run(BenchmarkFeatureId.DropTable);

    /// <summary>
    /// EN: Executes a single-row insert benchmark.
    /// PT: Executa um benchmark de insercao de uma linha.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertSingle() => Run(BenchmarkFeatureId.InsertSingle);

    /// <summary>
    /// EN: Executes an insert benchmark that starts from a custom id.
    /// PT: Executa um benchmark de insercao que inicia em um id customizado.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertCustomStartId() => Run(BenchmarkFeatureId.InsertCustomStartId);

    /// <summary>
    /// EN: Executes an insert benchmark that uses default-backed columns.
    /// PT: Executa um benchmark de insert que usa colunas apoiadas por default.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertDefaultColumns() => Run(BenchmarkFeatureId.InsertDefaultColumns);

    /// <summary>
    /// EN: Executes an insert benchmark that omits nullable columns.
    /// PT: Executa um benchmark de insert que omite colunas anulaveis.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertNullableColumns() => Run(BenchmarkFeatureId.InsertNullableColumns);

    /// <summary>
    /// EN: Executes an insert benchmark that omits a required NOT NULL column.
    /// PT: Executa um benchmark de insert que omite uma coluna NOT NULL obrigatoria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertNotNullWithoutDefault() => Run(BenchmarkFeatureId.InsertNotNullWithoutDefault);

    /// <summary>
    /// EN: Executes a benchmark that inserts a row satisfying the configured CHECK constraints.
    /// PT: Executa um benchmark que insere uma linha que satisfaz as restricoes CHECK configuradas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void CheckConstraintsValidInsert() => Run(BenchmarkFeatureId.CheckConstraintsValidInsert);

    /// <summary>
    /// EN: Executes a benchmark that attempts an insert violating a CHECK constraint.
    /// PT: Executa um benchmark que tenta um insert violando uma restricao CHECK.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void CheckConstraintsInvalidInsert() => Run(BenchmarkFeatureId.CheckConstraintsInvalidInsert);

    /// <summary>
    /// EN: Executes a benchmark that attempts an update violating a CHECK constraint.
    /// PT: Executa um benchmark que tenta um update violando uma restricao CHECK.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void CheckConstraintsInvalidUpdate() => Run(BenchmarkFeatureId.CheckConstraintsInvalidUpdate);

    /// <summary>
    /// EN: Executes a 100-row batch insert benchmark.
    /// PT: Executa um benchmark de insercao em lote de 100 linhas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertBatch100() => Run(BenchmarkFeatureId.InsertBatch100);

    /// <summary>
    /// EN: Executes a parallel 100-row batch insert benchmark.
    /// PT: Executa um benchmark de insercao em lote paralelo de 100 linhas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void InsertBatch100Parallel() => Run(BenchmarkFeatureId.InsertBatch100Parallel);

    /// <summary>
    /// EN: Executes a primary-key lookup benchmark.
    /// PT: Executa um benchmark de consulta por chave primaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectByPk() => Run(BenchmarkFeatureId.SelectByPk);

    /// <summary>
    /// EN: Executes a join-query benchmark.
    /// PT: Executa um benchmark de consulta com join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectJoin() => Run(BenchmarkFeatureId.SelectJoin);

    /// <summary>
    /// EN: Executes the join-count benchmark.
    /// PT: Executa o benchmark de contagem do join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectJoinCount() => Run(BenchmarkFeatureId.SelectJoinCount);

    /// <summary>
    /// EN: Executes the relational composite benchmark.
    /// PT: Executa o benchmark composto relacional.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void RelationalComposite() => Run(BenchmarkFeatureId.RelationalComposite);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectRelationalComposite() => Run(BenchmarkFeatureId.RelationalComposite);

    /// <summary>
    /// EN: Executes the APPLY projection benchmark.
    /// PT: Executa o benchmark de projeção APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectApplyProjection() => Run(BenchmarkFeatureId.SelectApplyProjection);

    /// <summary>
    /// EN: Executes the window-functions benchmark.
    /// PT: Executa o benchmark de funcoes de janela.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowFunctions() => Run(BenchmarkFeatureId.SelectWindowFunctions);

    /// <summary>
    /// EN: Executes the scalar-subquery CASE matrix benchmark.
    /// PT: Executa o benchmark da matriz CASE com subconsulta escalar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectScalarSubqueryCaseMatrix() => Run(BenchmarkFeatureId.SelectScalarSubqueryCaseMatrix);

    /// <summary>
    /// EN: Executes the range-and-pivot benchmark.
    /// PT: Executa o benchmark de faixa e pivot.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectRangeAndPivot() => Run(BenchmarkFeatureId.SelectRangeAndPivot);

    /// <summary>
    /// EN: Executes the IN-list predicate benchmark.
    /// PT: Executa o benchmark de predicado IN com lista.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void InListPredicate() => Run(BenchmarkFeatureId.InListPredicate);

    /// <summary>
    /// EN: Executes the BETWEEN predicate benchmark.
    /// PT: Executa o benchmark de predicado BETWEEN.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void BetweenPredicate() => Run(BenchmarkFeatureId.BetweenPredicate);

    /// <summary>
    /// EN: Executes the LIKE predicate benchmark.
    /// PT: Executa o benchmark de predicado LIKE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void LikePredicate() => Run(BenchmarkFeatureId.LikePredicate);

    /// <summary>
    /// EN: Executes the NOT LIKE predicate benchmark.
    /// PT: Executa o benchmark de predicado NOT LIKE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void NotLikePredicate() => Run(BenchmarkFeatureId.NotLikePredicate);

    /// <summary>
    /// EN: Executes the not-equal predicate benchmark.
    /// PT: Executa o benchmark de predicado diferente de.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void NotEqualPredicate() => Run(BenchmarkFeatureId.NotEqualPredicate);

    /// <summary>
    /// EN: Executes the equality predicate benchmark.
    /// PT: Executa o benchmark de predicado de igualdade.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void EqualPredicate() => Run(BenchmarkFeatureId.EqualPredicate);

    /// <summary>
    /// EN: Executes the greater-than predicate benchmark.
    /// PT: Executa o benchmark de predicado maior que.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void GreaterThanPredicate() => Run(BenchmarkFeatureId.GreaterThanPredicate);

    /// <summary>
    /// EN: Executes the less-than predicate benchmark.
    /// PT: Executa o benchmark de predicado menor que.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void LessThanPredicate() => Run(BenchmarkFeatureId.LessThanPredicate);

    /// <summary>
    /// EN: Executes the greater-than-or-equal predicate benchmark.
    /// PT: Executa o benchmark de predicado maior ou igual.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void GreaterThanOrEqualPredicate() => Run(BenchmarkFeatureId.GreaterThanOrEqualPredicate);

    /// <summary>
    /// EN: Executes the less-than-or-equal predicate benchmark.
    /// PT: Executa o benchmark de predicado menor ou igual.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void LessThanOrEqualPredicate() => Run(BenchmarkFeatureId.LessThanOrEqualPredicate);

    /// <summary>
    /// EN: Executes the NOT IN subquery with NULL benchmark.
    /// PT: Executa o benchmark de subconsulta NOT IN com NULL.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void NotInSubqueryNull() => Run(BenchmarkFeatureId.NotInSubqueryNull);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectInListPredicate() => Run(BenchmarkFeatureId.InListPredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectBetweenPredicate() => Run(BenchmarkFeatureId.BetweenPredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectLikePredicate() => Run(BenchmarkFeatureId.LikePredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectNotLikePredicate() => Run(BenchmarkFeatureId.NotLikePredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectNotEqualPredicate() => Run(BenchmarkFeatureId.NotEqualPredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectEqualPredicate() => Run(BenchmarkFeatureId.EqualPredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGreaterThanPredicate() => Run(BenchmarkFeatureId.GreaterThanPredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectLessThanPredicate() => Run(BenchmarkFeatureId.LessThanPredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGreaterThanOrEqualPredicate() => Run(BenchmarkFeatureId.GreaterThanOrEqualPredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectLessThanOrEqualPredicate() => Run(BenchmarkFeatureId.LessThanOrEqualPredicate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectNotInSubqueryNull() => Run(BenchmarkFeatureId.NotInSubqueryNull);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectBetweenLikeOrderBy() => Run(BenchmarkFeatureId.SelectBetweenLikeOrderByMatrix);

    /// <summary>
    /// EN: Executes a benchmark that counts all rows returned by the seeded select table.
    /// PT: Executa um benchmark que conta todas as linhas retornadas pela tabela de select semeada.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void AllRowsCount() => Run(BenchmarkFeatureId.AllRowsCount);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectAllRowsCount() => Run(BenchmarkFeatureId.AllRowsCount);

    /// <summary>
    /// EN: Executes a benchmark that captures the full select snapshot from the seeded table.
    /// PT: Executa um benchmark que captura o snapshot completo do select na tabela semeada.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void AllRowsSnapshot() => Run(BenchmarkFeatureId.AllRowsSnapshot);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectAllRowsSnapshot() => Run(BenchmarkFeatureId.AllRowsSnapshot);

    /// <summary>
    /// EN: Executes the MATERIALIZED CTE benchmark.
    /// PT: Executa o benchmark de CTE MATERIALIZED.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void CteMaterializedHint() => Run(BenchmarkFeatureId.CteMaterializedHint);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectCteMaterializedHint() => Run(BenchmarkFeatureId.CteMaterializedHint);

    /// <summary>
    /// EN: Executes the DISTINCT ON projection benchmark.
    /// PT: Executa o benchmark de projecao DISTINCT ON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void DistinctOnProjection() => Run(BenchmarkFeatureId.DistinctOnProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectDistinctOnProjection() => Run(BenchmarkFeatureId.DistinctOnProjection);

    /// <summary>
    /// EN: Executes the ORDER BY Name matrix benchmark.
    /// PT: Executa o benchmark da matriz ORDER BY Name.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void OrderByNameMatrix() => Run(BenchmarkFeatureId.OrderByNameMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectOrderByName() => Run(BenchmarkFeatureId.OrderByNameMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectOrderByNameMatrix() => Run(BenchmarkFeatureId.OrderByNameMatrix);

    /// <summary>
    /// EN: Executes the ORDER BY ordinal matrix benchmark.
    /// PT: Executa o benchmark da matriz ORDER BY ordinal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void OrderByOrdinalMatrix() => Run(BenchmarkFeatureId.OrderByOrdinalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectOrderByOrdinal() => Run(BenchmarkFeatureId.OrderByOrdinalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectOrderByOrdinalMatrix() => Run(BenchmarkFeatureId.OrderByOrdinalMatrix);

    /// <summary>
    /// EN: Executes the ORDER BY Name descending matrix benchmark.
    /// PT: Executa o benchmark da matriz ORDER BY Name descendente.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void OrderByNameDescendingMatrix() => Run(BenchmarkFeatureId.OrderByNameDescendingMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectOrderByNameDescending() => Run(BenchmarkFeatureId.OrderByNameDescendingMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectOrderByNameDescendingMatrix() => Run(BenchmarkFeatureId.OrderByNameDescendingMatrix);

    /// <summary>
    /// EN: Executes the name pagination matrix benchmark.
    /// PT: Executa o benchmark da matriz de paginacao por nome.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void NamePaginationMatrix() => Run(BenchmarkFeatureId.NamePaginationMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectNamePaginationMatrix() => Run(BenchmarkFeatureId.NamePaginationMatrix);

    /// <summary>
    /// EN: Executes the GROUP BY name initial matrix benchmark.
    /// PT: Executa o benchmark da matriz GROUP BY por inicial do nome.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void GroupByNameInitialMatrix() => Run(BenchmarkFeatureId.GroupByNameInitialMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGroupByNameInitialMatrix() => Run(BenchmarkFeatureId.GroupByNameInitialMatrix);

    /// <summary>
    /// EN: Executes the GROUP BY name HAVING matrix benchmark.
    /// PT: Executa o benchmark da matriz GROUP BY com HAVING por nome.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void GroupByNameHavingMatrix() => Run(BenchmarkFeatureId.GroupByNameHavingMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGroupByNameHaving() => Run(BenchmarkFeatureId.GroupByNameHavingMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGroupByNameHavingMatrix() => Run(BenchmarkFeatureId.GroupByNameHavingMatrix);

    /// <summary>
    /// EN: Executes the GROUP BY ordinal matrix benchmark.
    /// PT: Executa o benchmark da matriz GROUP BY por ordinal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void GroupByOrdinalMatrix() => Run(BenchmarkFeatureId.GroupByOrdinalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGroupByOrdinal() => Run(BenchmarkFeatureId.GroupByOrdinalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGroupByOrdinalMatrix() => Run(BenchmarkFeatureId.GroupByOrdinalMatrix);

    /// <summary>
    /// EN: Executes the DISTINCT order-by-ordinal matrix benchmark.
    /// PT: Executa o benchmark da matriz DISTINCT com ORDER BY ordinal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void DistinctOrderByOrdinalMatrix() => Run(BenchmarkFeatureId.DistinctOrderByOrdinalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectDistinctOrderByOrdinal() => Run(BenchmarkFeatureId.DistinctOrderByOrdinalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectDistinctOrderByOrdinalMatrix() => Run(BenchmarkFeatureId.DistinctOrderByOrdinalMatrix);

    /// <summary>
    /// EN: Executes the DISTINCT text-filter order-by-ordinal matrix benchmark.
    /// PT: Executa o benchmark da matriz DISTINCT com filtro de texto e ORDER BY ordinal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void DistinctLikeOrderByOrdinalMatrix() => Run(BenchmarkFeatureId.DistinctLikeOrderByOrdinalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectDistinctLikeOrderByOrdinal() => Run(BenchmarkFeatureId.DistinctLikeOrderByOrdinalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectDistinctLikeOrderByOrdinalMatrix() => Run(BenchmarkFeatureId.DistinctLikeOrderByOrdinalMatrix);

    /// <summary>
    /// EN: Executes the joined typed-expression matrix benchmark.
    /// PT: Executa o benchmark da matriz com expressoes tipadas em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinTypedExpressionMatrix() => Run(BenchmarkFeatureId.JoinTypedExpressionMatrix);

    /// <summary>
    /// EN: Executes the joined null-aggregate matrix benchmark.
    /// PT: Executa o benchmark da matriz agregada com null em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinNullAggregateMatrix() => Run(BenchmarkFeatureId.JoinNullAggregateMatrix);

    /// <summary>
    /// EN: Executes the joined cast-null matrix benchmark.
    /// PT: Executa o benchmark da matriz com cast e null em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinCastNullMatrix() => Run(BenchmarkFeatureId.JoinCastNullMatrix);

    /// <summary>
    /// EN: Executes the joined cast-text comparison matrix benchmark.
    /// PT: Executa o benchmark da matriz com cast e comparacao textual em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinCastTextComparisonMatrix() => Run(BenchmarkFeatureId.JoinCastTextComparisonMatrix);

    /// <summary>
    /// EN: Executes the joined HAVING cast matrix benchmark.
    /// PT: Executa o benchmark da matriz HAVING com cast em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinHavingCastMatrix() => Run(BenchmarkFeatureId.JoinHavingCastMatrix);

    /// <summary>
    /// EN: Executes the joined length-and-numeric matrix benchmark.
    /// PT: Executa o benchmark da matriz com comprimento e numericos em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinLengthNumericMatrix() => Run(BenchmarkFeatureId.JoinLengthNumericMatrix);

    /// <summary>
    /// EN: Executes the joined text-case-length matrix benchmark.
    /// PT: Executa o benchmark da matriz com caixa, texto e comprimento em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinTextCaseLengthMatrix() => Run(BenchmarkFeatureId.JoinTextCaseLengthMatrix);

    /// <summary>
    /// EN: Executes the joined distinct-case matrix benchmark.
    /// PT: Executa o benchmark da matriz DISTINCT com CASE em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinDistinctCaseMatrix() => Run(BenchmarkFeatureId.JoinDistinctCaseMatrix);

    /// <summary>
    /// EN: Executes the joined distinct-HAVING matrix benchmark.
    /// PT: Executa o benchmark da matriz DISTINCT com HAVING em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinDistinctHavingMatrix() => Run(BenchmarkFeatureId.JoinDistinctHavingMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinTypedExpressionMatrix() => Run(BenchmarkFeatureId.JoinTypedExpressionMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinNullAggregateMatrix() => Run(BenchmarkFeatureId.JoinNullAggregateMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinCastNullMatrix() => Run(BenchmarkFeatureId.JoinCastNullMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinCastTextComparisonMatrix() => Run(BenchmarkFeatureId.JoinCastTextComparisonMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinHavingCastMatrix() => Run(BenchmarkFeatureId.JoinHavingCastMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinLengthNumericMatrix() => Run(BenchmarkFeatureId.JoinLengthNumericMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinTextCaseLengthMatrix() => Run(BenchmarkFeatureId.JoinTextCaseLengthMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinDistinctCaseMatrix() => Run(BenchmarkFeatureId.JoinDistinctCaseMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinDistinctHavingMatrix() => Run(BenchmarkFeatureId.JoinDistinctHavingMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinTemporalMatrix() => Run(BenchmarkFeatureId.JoinTemporalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinWindowMatrix() => Run(BenchmarkFeatureId.JoinWindowMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinWindowTemporalMatrix() => Run(BenchmarkFeatureId.JoinWindowTemporalMatrix);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectJoinWindowAggregateTemporalMatrix() => Run(BenchmarkFeatureId.JoinWindowAggregateTemporalMatrix);

    /// <summary>
    /// EN: Executes the STRING_SPLIT projection benchmark.
    /// PT: Executa o benchmark de projecao STRING_SPLIT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void StringSplitProjection() => Run(BenchmarkFeatureId.StringSplitProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectStringSplitFunction() => Run(BenchmarkFeatureId.StringSplitProjection);

    /// <summary>
    /// EN: Executes the FOR JSON PATH projection benchmark.
    /// PT: Executa o benchmark de projecao FOR JSON PATH.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ForJsonPathProjection() => Run(BenchmarkFeatureId.ForJsonPathProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectForJsonPath() => Run(BenchmarkFeatureId.ForJsonPathProjection);

    /// <summary>
    /// EN: Executes the joined window and temporal matrix benchmark.
    /// PT: Executa o benchmark da matriz com janela e temporal em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinWindowTemporalMatrix() => Run(BenchmarkFeatureId.JoinWindowTemporalMatrix);

    /// <summary>
    /// EN: Executes the joined temporal matrix benchmark.
    /// PT: Executa o benchmark da matriz temporal em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinTemporalMatrix() => Run(BenchmarkFeatureId.JoinTemporalMatrix);

    /// <summary>
    /// EN: Executes the joined window matrix benchmark.
    /// PT: Executa o benchmark da matriz de janela em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinWindowMatrix() => Run(BenchmarkFeatureId.JoinWindowMatrix);

    /// <summary>
    /// EN: Executes the joined window, aggregate, and temporal matrix benchmark.
    /// PT: Executa o benchmark da matriz com janela, agregacao e temporal em join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void JoinWindowAggregateTemporalMatrix() => Run(BenchmarkFeatureId.JoinWindowAggregateTemporalMatrix);

    /// <summary>
    /// EN: Executes the APPLY and temporal composite benchmark.
    /// PT: Executa o benchmark composto de APPLY e temporal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ApplyTemporalComposite() => Run(BenchmarkFeatureId.ApplyTemporalComposite);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectApplyTemporalComposite() => Run(BenchmarkFeatureId.ApplyTemporalComposite);

    /// <summary>
    /// EN: Executes the APPLY and window-temporal composite benchmark.
    /// PT: Executa o benchmark composto de APPLY e janela-temporal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ApplyWindowTemporalComposite() => Run(BenchmarkFeatureId.ApplyWindowTemporalComposite);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectApplyWindowTemporalComposite() => Run(BenchmarkFeatureId.ApplyWindowTemporalComposite);

    /// <summary>
    /// EN: Executes a primary-key update benchmark.
    /// PT: Executa um benchmark de atualizacao por chave primaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void UpdateByPk() => Run(BenchmarkFeatureId.UpdateByPk);

    /// <summary>
    /// EN: Executes an update/delete round-trip benchmark.
    /// PT: Executa um benchmark de ciclo de update/delete.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void UpdateDeleteRoundTrip() => Run(BenchmarkFeatureId.UpdateDeleteRoundTrip);

    /// <summary>
    /// EN: Executes the parameter update/delete round-trip benchmark.
    /// PT: Executa o benchmark de roundtrip de update/delete com parametros.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterUpdateDeleteRoundTrip() => Run(BenchmarkFeatureId.ParameterUpdateDeleteRoundTrip);

    /// <summary>
    /// EN: Executes a primary-key delete benchmark.
    /// PT: Executa um benchmark de exclusao por chave primaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void DeleteByPk() => Run(BenchmarkFeatureId.DeleteByPk);

    /// <summary>
    /// EN: Executes a transaction-commit benchmark.
    /// PT: Executa um benchmark de confirmacao de transacao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TransactionCommit() => Run(BenchmarkFeatureId.TransactionCommit);

    /// <summary>
    /// EN: Executes a transaction-rollback benchmark.
    /// PT: Executa um benchmark de rollback de transacao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TransactionRollback() => Run(BenchmarkFeatureId.TransactionRollback);

    /// <summary>
    /// EN: Executes an update/delete workflow inside a transaction benchmark.
    /// PT: Executa um benchmark de update/delete dentro de uma transacao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TransactionalUpdateDeleteCommit() => Run(BenchmarkFeatureId.TransactionalUpdateDeleteCommit);

    /// <summary>
    /// EN: Executes a typed parameter insert transaction commit benchmark.
    /// PT: Executa um benchmark de confirmacao de transacao com inserts tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void ParameterTransactionCommit() => Run(BenchmarkFeatureId.ParameterTransactionCommit);

    /// <summary>
    /// EN: Executes a typed parameter insert transaction rollback benchmark.
    /// PT: Executa um benchmark de rollback de transacao com inserts tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void ParameterTransactionRollback() => Run(BenchmarkFeatureId.ParameterTransactionRollback);

    /// <summary>
    /// EN: Executes a provider-specific upsert benchmark.
    /// PT: Executa um benchmark de upsert especifico do provedor.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Upsert() => Run(BenchmarkFeatureId.Upsert);

    /// <summary>
    /// EN: Executes the merge insert-then-update benchmark.
    /// PT: Executa o benchmark de merge de inserir e depois atualizar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MergeInsertThenUpdate() => Run(BenchmarkFeatureId.MergeInsertThenUpdate);

    /// <summary>
    /// EN: Executes the upsert insert-then-update benchmark.
    /// PT: Executa o benchmark de upsert de inserir e depois atualizar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void UpsertInsertThenUpdate() => Run(BenchmarkFeatureId.UpsertInsertThenUpdate);

    /// <summary>
    /// EN: Executes a parameter projection benchmark.
    /// PT: Executa um benchmark de projeção parametrizada.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterProjection() => Run(BenchmarkFeatureId.ParameterProjection);

    /// <summary>
    /// EN: Executes a parameterized single-row insert benchmark.
    /// PT: Executa um benchmark de insercao parametrizada de uma linha.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterInsertSingle() => Run(BenchmarkFeatureId.ParameterInsertSingle);

    /// <summary>
    /// EN: Executes a parameter insert round-trip benchmark.
    /// PT: Executa o benchmark de roundtrip de insert com parametros.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterInsertRoundTrip() => Run(BenchmarkFeatureId.ParameterInsertRoundTrip);

    /// <summary>
    /// EN: Executes a parameter insert round-trip benchmark with null values.
    /// PT: Executa o benchmark de roundtrip de insert com parametros e valores nulos.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterInsertNullRoundTrip() => Run(BenchmarkFeatureId.ParameterInsertNullRoundTrip);

    /// <summary>
    /// EN: Executes a parameterized name lookup benchmark.
    /// PT: Executa um benchmark de consulta parametrizada por nome.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterSelectByNameMatrix() => Run(BenchmarkFeatureId.ParameterSelectByNameMatrix);

    /// <summary>
    /// EN: Executes a parameterized id lookup benchmark.
    /// PT: Executa um benchmark de consulta parametrizada por id.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterSelectByIdMatrix() => Run(BenchmarkFeatureId.ParameterSelectByIdMatrix);

    /// <summary>
    /// EN: Executes a typed parameter round-trip benchmark.
    /// PT: Executa um benchmark de roundtrip de parametros tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterRoundTripMatrix() => Run(BenchmarkFeatureId.ParameterRoundTripMatrix);

    /// <summary>
    /// EN: Executes a typed parameter projection benchmark.
    /// PT: Executa um benchmark de projeção de parametros tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterTypeMatrix() => Run(BenchmarkFeatureId.ParameterTypeMatrix);

    /// <summary>
    /// EN: Executes a typed date and currency parameter benchmark.
    /// PT: Executa um benchmark de data e moeda com parametros tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void ParameterDateCurrencyMatrix() => Run(BenchmarkFeatureId.ParameterDateCurrencyMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectParameterByName() => Run(BenchmarkFeatureId.ParameterSelectByNameMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectParameterById() => Run(BenchmarkFeatureId.ParameterSelectByIdMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectParameterRoundTripMatrix() => Run(BenchmarkFeatureId.ParameterRoundTripMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectParameterTypeMatrix() => Run(BenchmarkFeatureId.ParameterTypeMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void SelectParameterDateCurrencyMatrix() => Run(BenchmarkFeatureId.ParameterDateCurrencyMatrix);

    /// <summary>
    /// EN: Executes the typed field storage matrix benchmark.
    /// PT: Executa o benchmark da matriz de armazenamento tipado.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldStorageMatrix() => Run(BenchmarkFeatureId.TypedFieldStorageMatrix);

    /// <summary>
    /// EN: Executes the typed field function matrix benchmark.
    /// PT: Executa o benchmark da matriz de funcoes tipadas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldFunctionMatrix() => Run(BenchmarkFeatureId.TypedFieldFunctionMatrix);

    /// <summary>
    /// EN: Executes the typed field calculation matrix benchmark.
    /// PT: Executa o benchmark da matriz de calculo tipado.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldCalculationMatrix() => Run(BenchmarkFeatureId.TypedFieldCalculationMatrix);

    /// <summary>
    /// EN: Executes the typed field and function blend benchmark.
    /// PT: Executa o benchmark de mistura de campos tipados e funcoes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldAndFunctionBlend() => Run(BenchmarkFeatureId.TypedFieldAndFunctionBlend);

    /// <summary>
    /// EN: Executes the typed field compound predicate matrix benchmark.
    /// PT: Executa o benchmark da matriz de predicados compostos com campos tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldCompoundPredicateMatrix() => Run(BenchmarkFeatureId.TypedFieldCompoundPredicateMatrix);

    /// <summary>
    /// EN: Executes the typed field cast calculation matrix benchmark.
    /// PT: Executa o benchmark da matriz de calculo com casts em campos tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldCastCalculationMatrix() => Run(BenchmarkFeatureId.TypedFieldCastCalculationMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void CastCalculationMatrix() => Run(BenchmarkFeatureId.TypedFieldCastCalculationMatrix);

    /// <summary>
    /// EN: Executes the typed field null comparison matrix benchmark.
    /// PT: Executa o benchmark da matriz de comparacao com null em campos tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldNullComparisonMatrix() => Run(BenchmarkFeatureId.TypedFieldNullComparisonMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void NullComparisonMatrix() => Run(BenchmarkFeatureId.TypedFieldNullComparisonMatrix);

    /// <summary>
    /// EN: Executes the typed field text length matrix benchmark.
    /// PT: Executa o benchmark da matriz de comprimento de texto em campos tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldTextLengthMatrix() => Run(BenchmarkFeatureId.TypedFieldTextLengthMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void TextLengthMatrix() => Run(BenchmarkFeatureId.TypedFieldTextLengthMatrix);

    /// <summary>
    /// EN: Executes the typed field text case matrix benchmark.
    /// PT: Executa o benchmark da matriz de caixa de texto em campos tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldTextCaseMatrix() => Run(BenchmarkFeatureId.TypedFieldTextCaseMatrix);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void TextCaseMatrix() => Run(BenchmarkFeatureId.TypedFieldTextCaseMatrix);

    /// <summary>
    /// EN: Executes the typed field predicate matrix benchmark.
    /// PT: Executa o benchmark da matriz de predicados em campos tipados.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void TypedFieldPredicateMatrix() => Run(BenchmarkFeatureId.TypedFieldPredicateMatrix);

    /// <summary>
    /// EN: Executes a stored procedure call benchmark.
    /// PT: Executa um benchmark de chamada de procedimento armazenado.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void StoredProcedureCall() => Run(BenchmarkFeatureId.StoredProcedureCall);

    /// <summary>
    /// EN: Executes a string-aggregation benchmark.
    /// PT: Executa um benchmark de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregate() => Run(BenchmarkFeatureId.StringAggregate);

    /// <summary>
    /// EN: Executes a date-scalar benchmark.
    /// PT: Executa um benchmark escalar de data.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void DateScalar() => Run(BenchmarkFeatureId.DateScalar);

    /// <summary>
    /// EN: Executes the scalar temporal matrix benchmark.
    /// PT: Executa a matriz temporal escalar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void ScalarTemporalMatrix() => Run(BenchmarkFeatureId.ScalarTemporalMatrix);

    /// <summary>
    /// EN: Executes the shared math functions benchmark.
    /// PT: Executa o benchmark compartilhado de funcoes matematicas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathFunctions() => Run(BenchmarkFeatureId.MathFunctions);

    /// <summary>
    /// EN: Executes the explicit-base math LOG benchmark.
    /// PT: Executa o benchmark matematico LOG com base explicita.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathLogBaseFunction() => Run(BenchmarkFeatureId.MathLogBaseFunction);

    /// <summary>
    /// EN: Executes the math LOG2 benchmark.
    /// PT: Executa o benchmark matematico LOG2.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathLog2Function() => Run(BenchmarkFeatureId.MathLog2Function);

    /// <summary>
    /// EN: Executes the math PI benchmark.
    /// PT: Executa o benchmark matematico PI.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathPiFunction() => Run(BenchmarkFeatureId.MathPiFunction);

    /// <summary>
    /// EN: Executes the math RAND benchmark.
    /// PT: Executa o benchmark matematico RAND.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathRandFunction() => Run(BenchmarkFeatureId.MathRandFunction);

    /// <summary>
    /// EN: Executes the math remainder benchmark.
    /// PT: Executa o benchmark matematico de resto.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathRemainderFunction() => Run(BenchmarkFeatureId.MathRemainderFunction);

    /// <summary>
    /// EN: Executes the math truncation benchmark.
    /// PT: Executa o benchmark matematico de truncamento.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathTruncFunction() => Run(BenchmarkFeatureId.MathTruncFunction);

    /// <summary>
    /// EN: Executes the math cotangent benchmark.
    /// PT: Executa o benchmark matematico de cotangente.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathCotFunction() => Run(BenchmarkFeatureId.MathCotFunction);

    /// <summary>
    /// EN: Executes the MySQL utility math benchmark.
    /// PT: Executa o benchmark de utilitarios matematicos da familia MySQL.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MySqlUtilityMathFunctions() => Run(BenchmarkFeatureId.MySqlUtilityMathFunctions);

    /// <summary>
    /// EN: Executes the shared greatest/least/mod benchmark.
    /// PT: Executa o benchmark compartilhado de greatest/least/mod.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void GreatestLeastModFunctions() => Run(BenchmarkFeatureId.GreatestLeastModFunctions);

    /// <summary>
    /// EN: Executes the DB2 alias math benchmark.
    /// PT: Executa o benchmark de aliases matematicos do DB2.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Db2AliasMathFunctions() => Run(BenchmarkFeatureId.Db2AliasMathFunctions);

    /// <summary>
    /// EN: Executes the Firebird alias math benchmark.
    /// PT: Executa o benchmark de aliases matematicos do Firebird.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void FirebirdAliasMathFunctions() => Run(BenchmarkFeatureId.FirebirdAliasMathFunctions);

    /// <summary>
    /// EN: Executes the shared transcendental math benchmark.
    /// PT: Executa o benchmark compartilhado de matematica transcendental.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void MathTranscendentalFunctions() => Run(BenchmarkFeatureId.MathTranscendentalFunctions);

    private static readonly object _setupLogSync = new();
    protected void LogSetupIssue(Exception ex)
    {
        var root = ex?.GetBaseException();

        var message =
            $"[SETUP-{root!.GetType().Name}] {root.ToString()}";

        Console.WriteLine(message);

        var logEntry =
            $"{DateTime.UtcNow:O} {message}{Environment.NewLine}{root?.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        lock (_setupLogSync)
        {
            var directory = BenchmarkLogPath.GetDirectory();
            Directory.CreateDirectory(directory);

            var file = BenchmarkLogPath.GetFilePath($"{GetType().FullName}-setup-errors.log");
            if (!File.Exists(file))
                File.Create(file).Dispose();
            File.AppendAllText(
                file,
                logEntry);
        }
    }

    private static readonly object _logSync = new();
    private static readonly HashSet<string> Errors = [];
    protected virtual void LogBenchmarkIssue(BenchmarkFeatureId feature, Exception ex)
    {
        var root = ex?.GetBaseException();
        var message = root is NotSupportedException
            ? $"[NA-{root!.GetType().Name}] {feature}: {root.Message}{Environment.NewLine}{Environment.NewLine}"
            : $"[NA-{root!.GetType().Name}] {feature}: {root.Message} -- {ex?.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        Console.WriteLine(message);

        lock (_logSync)
        {
            var errorKey = $"{GetType().FullName}|{feature}|{root.GetType().FullName}|{root.Message}";
            if (Errors.Contains(errorKey))
                return;
            Errors.Add(errorKey);

            var directory = BenchmarkLogPath.GetDirectory();
            Directory.CreateDirectory(directory);

            var file = BenchmarkLogPath.GetFilePath($"{GetType().FullName}-errors.log");
            File.AppendAllText(
                file,
                message + Environment.NewLine);
        }
    }

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void InsertBatch10() => Run(BenchmarkFeatureId.InsertBatch10);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void SavepointCreate() => Run(BenchmarkFeatureId.SavepointCreate);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void RollbackToSavepoint() => Run(BenchmarkFeatureId.RollbackToSavepoint);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void ReleaseSavepoint() => Run(BenchmarkFeatureId.ReleaseSavepoint);

    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void NestedSavepointFlow() => Run(BenchmarkFeatureId.NestedSavepointFlow);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchInsert10() => Run(BenchmarkFeatureId.BatchInsert10);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchInsert100() => Run(BenchmarkFeatureId.BatchInsert100);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchMixedReadWrite() => Run(BenchmarkFeatureId.BatchMixedReadWrite);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchScalar() => Run(BenchmarkFeatureId.BatchScalar);

    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchNonQuery() => Run(BenchmarkFeatureId.BatchNonQuery);

    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonScalarRead() => Run(BenchmarkFeatureId.JsonScalarRead);

    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonPathRead() => Run(BenchmarkFeatureId.JsonPathRead);

    /// <summary>
    /// EN: Executes the JSON missing-path benchmark.
    /// PT: Executa o benchmark de caminho JSON ausente.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonMissingPathRead() => Run(BenchmarkFeatureId.JsonMissingPathRead);

    /// <summary>
    /// EN: Executes the JSON missing-path benchmark and expects a null result.
    /// PT: Executa o benchmark de caminho JSON ausente e espera um resultado nulo.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonMissingPathReturnsNull() => Run(BenchmarkFeatureId.JsonMissingPathReturnsNull);

    /// <summary>
    /// EN: Executes the JSON root-fragment benchmark.
    /// PT: Executa o benchmark de fragmento raiz JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonQueryRootFragment() => Run(BenchmarkFeatureId.JsonQueryRootFragment);

    /// <summary>
    /// EN: Executes the JSON_MODIFY replacement benchmark.
    /// PT: Executa o benchmark de substituicao JSON_MODIFY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonModifyReplace() => Run(BenchmarkFeatureId.JsonModifyReplace);

    /// <summary>
    /// EN: Executes the JSON typed field matrix benchmark.
    /// PT: Executa o benchmark da matriz de campos tipados com JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonTypedFieldMatrix() => Run(BenchmarkFeatureId.JsonTypedFieldMatrix);

    /// <summary>
    /// EN: Executes the json_each benchmark over a JSON array.
    /// PT: Executa o benchmark json_each sobre um array JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonEachFromArray() => Run(BenchmarkFeatureId.JsonEachFromArray);

    /// <summary>
    /// EN: Executes the json_each benchmark over a JSON object.
    /// PT: Executa o benchmark json_each sobre um objeto JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonEachFromObject() => Run(BenchmarkFeatureId.JsonEachFromObject);

    /// <summary>
    /// EN: Executes the json_tree benchmark over JSON.
    /// PT: Executa o benchmark json_tree sobre JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonTreeStructure() => Run(BenchmarkFeatureId.JsonTreeStructure);

    /// <summary>
    /// EN: Executes the OPENJSON benchmark over a JSON array.
    /// PT: Executa o benchmark OPENJSON sobre um array JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void OpenJsonArray() => Run(BenchmarkFeatureId.OpenJsonArray);

    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalCurrentTimestamp() => Run(BenchmarkFeatureId.TemporalCurrentTimestamp);

    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalDateAdd() => Run(BenchmarkFeatureId.TemporalDateAdd);

    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalNowWhere() => Run(BenchmarkFeatureId.TemporalNowWhere);

    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalNowOrderBy() => Run(BenchmarkFeatureId.TemporalNowOrderBy);

    /// <summary>
    /// EN: Executes the temporal field matrix benchmark.
    /// PT: Executa o benchmark da matriz de campos temporais.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalFieldMatrix() => Run(BenchmarkFeatureId.TemporalFieldMatrix);

    /// <summary>
    /// EN: Executes the temporal comparison matrix benchmark.
    /// PT: Executa o benchmark da matriz de comparacao temporal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalComparisonMatrix() => Run(BenchmarkFeatureId.TemporalComparisonMatrix);

    /// <summary>
    /// EN: Executes the temporal arithmetic matrix benchmark.
    /// PT: Executa o benchmark da matriz de aritmetica temporal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalArithmeticMatrix() => Run(BenchmarkFeatureId.TemporalArithmeticMatrix);

    /// <summary>
    /// EN: Executes the DATETRUNC benchmark.
    /// PT: Executa o benchmark DATETRUNC.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalDateTrunc() => Run(BenchmarkFeatureId.TemporalDateTrunc);

    /// <summary>
    /// EN: Executes the time-zone offset benchmark.
    /// PT: Executa o benchmark de fuso horario.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalTimeZoneOffset() => Run(BenchmarkFeatureId.TemporalTimeZoneOffset);

    /// <summary>
    /// EN: Executes the FROMPARTS benchmark.
    /// PT: Executa o benchmark FROMPARTS.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalFromParts() => Run(BenchmarkFeatureId.TemporalFromParts);

    /// <summary>
    /// EN: Executes the EOMONTH benchmark.
    /// PT: Executa o benchmark EOMONTH.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalEndOfMonth() => Run(BenchmarkFeatureId.TemporalEndOfMonth);

    /// <summary>
    /// EN: Executes the DATEDIFF_BIG benchmark.
    /// PT: Executa o benchmark DATEDIFF_BIG.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalDateDiffBig() => Run(BenchmarkFeatureId.TemporalDateDiffBig);

    /// <summary>
    /// EN: Executes the SQL Server metadata functions benchmark.
    /// PT: Executa o benchmark de funcoes de metadata do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SqlServerMetadataFunctions() => Run(BenchmarkFeatureId.SqlServerMetadataFunctions);

    /// <summary>
    /// EN: Executes the SCOPE_IDENTITY benchmark.
    /// PT: Executa o benchmark SCOPE_IDENTITY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void ScopeIdentity() => Run(BenchmarkFeatureId.ScopeIdentity);

    /// <summary>
    /// EN: Executes the SQL Server system functions benchmark.
    /// PT: Executa o benchmark de funcoes de sistema do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SqlServerSystemFunctions() => Run(BenchmarkFeatureId.SqlServerSystemFunctions);

    /// <summary>
    /// EN: Executes the SQL Server special functions benchmark.
    /// PT: Executa o benchmark de funcoes especiais do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SqlServerSpecialFunctions() => Run(BenchmarkFeatureId.SqlServerSpecialFunctions);

    /// <summary>
    /// EN: Executes the SQL Server context functions benchmark.
    /// PT: Executa o benchmark de funcoes de contexto do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SqlServerContextFunctions() => Run(BenchmarkFeatureId.SqlServerContextFunctions);

    /// <summary>
    /// EN: Executes the SQL Server transaction state functions benchmark.
    /// PT: Executa o benchmark de funcoes de estado de transacao do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SqlServerTransactionStateFunctions() => Run(BenchmarkFeatureId.SqlServerTransactionStateFunctions);

    /// <summary>
    /// EN: Executes the SQL Server session functions benchmark.
    /// PT: Executa o benchmark de funcoes de sessao do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SqlServerSessionFunctions() => Run(BenchmarkFeatureId.SqlServerSessionFunctions);

    /// <summary>
    /// EN: Executes the SQL Server string functions benchmark.
    /// PT: Executa o benchmark de funcoes de string do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringBasicFunctions() => Run(BenchmarkFeatureId.StringBasicFunctions);

    /// <summary>
    /// EN: Executes the SQL Server string utility benchmark.
    /// PT: Executa o benchmark de utilitarios de string do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringUtilityFunctions() => Run(BenchmarkFeatureId.StringUtilityFunctions);

    /// <summary>
    /// EN: Executes the SQL Server string metadata benchmark.
    /// PT: Executa o benchmark de metadados de string do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringMetadataFunctions() => Run(BenchmarkFeatureId.StringMetadataFunctions);

    /// <summary>
    /// EN: Executes the SQL Server STRING_ESCAPE benchmark.
    /// PT: Executa o benchmark STRING_ESCAPE do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringEscape() => Run(BenchmarkFeatureId.StringEscape);

    /// <summary>
    /// EN: Executes the SQL Server TRANSLATE benchmark.
    /// PT: Executa o benchmark TRANSLATE do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Translate() => Run(BenchmarkFeatureId.Translate);

    /// <summary>
    /// EN: Executes the SQL Server FORMATMESSAGE benchmark.
    /// PT: Executa o benchmark FORMATMESSAGE do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void FormatMessage() => Run(BenchmarkFeatureId.FormatMessage);

    /// <summary>
    /// EN: Executes the SQL Server ISJSON benchmark.
    /// PT: Executa o benchmark ISJSON do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void IsJson() => Run(BenchmarkFeatureId.IsJson);

    /// <summary>
    /// EN: Executes the SQL Server FORMAT benchmark.
    /// PT: Executa o benchmark FORMAT do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Format() => Run(BenchmarkFeatureId.Format);

    /// <summary>
    /// EN: Executes the SQL Server PARSE-family benchmark.
    /// PT: Executa o benchmark da familia PARSE do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void ParseFamily() => Run(BenchmarkFeatureId.ParseFamily);

    /// <summary>
    /// EN: Executes the SQL Server SOUNDEX benchmark.
    /// PT: Executa o benchmark SOUNDEX do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Soundex() => Run(BenchmarkFeatureId.Soundex);

    /// <summary>
    /// EN: Executes the SQL Server compression benchmark.
    /// PT: Executa o benchmark de compressao do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Compression() => Run(BenchmarkFeatureId.Compression);

    /// <summary>
    /// EN: Executes the APPROX_COUNT_DISTINCT benchmark.
    /// PT: Executa o benchmark APPROX_COUNT_DISTINCT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void ApproxCountDistinct() => Run(BenchmarkFeatureId.ApproxCountDistinct);

    /// <summary>
    /// EN: Executes the percentile aggregate benchmark.
    /// PT: Executa o benchmark de agregacao por percentil.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void PercentileAggregateFunctions() => Run(BenchmarkFeatureId.PercentileAggregateFunctions);

    /// <summary>
    /// EN: Executes the SQL Server aggregate functions benchmark.
    /// PT: Executa o benchmark de funcoes de agregacao do SQL Server.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void SqlServerAggregateFunctions() => Run(BenchmarkFeatureId.SqlServerAggregateFunctions);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateOrdered() => Run(BenchmarkFeatureId.StringAggregateOrdered);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateDistinct() => Run(BenchmarkFeatureId.StringAggregateDistinct);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateCustomSeparator() => Run(BenchmarkFeatureId.StringAggregateCustomSeparator);

    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateLargeGroup() => Run(BenchmarkFeatureId.StringAggregateLargeGroup);

    /// <summary>
    /// EN: Executes the string aggregate summary matrix benchmark.
    /// PT: Executa o benchmark da matriz resumo de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateSummaryMatrix() => Run(BenchmarkFeatureId.StringAggregateSummaryMatrix);

    /// <summary>
    /// EN: Executes the grouped string aggregate matrix benchmark.
    /// PT: Executa o benchmark da matriz agrupada de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateGroupCaseMatrix() => Run(BenchmarkFeatureId.StringAggregateGroupCaseMatrix);

    /// <summary>
    /// EN: Executes the string aggregation summary matrix alias benchmark.
    /// PT: Executa o benchmark alias da matriz resumo de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregationSummaryMatrix() => Run(BenchmarkFeatureId.StringAggregationSummaryMatrix);

    /// <summary>
    /// EN: Executes the grouped string aggregation alias benchmark.
    /// PT: Executa o benchmark alias da matriz agrupada de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregationGroupCaseMatrix() => Run(BenchmarkFeatureId.StringAggregationGroupCaseMatrix);

    /// <summary>
    /// EN: Executes the string aggregation variants benchmark.
    /// PT: Executa o benchmark das variantes de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregationVariants() => Run(BenchmarkFeatureId.StringAggregationVariants);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterInsert() => Run(BenchmarkFeatureId.RowCountAfterInsert);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterUpdate() => Run(BenchmarkFeatureId.RowCountAfterUpdate);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterSelect() => Run(BenchmarkFeatureId.RowCountAfterSelect);

    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void CteSimple() => Run(BenchmarkFeatureId.CteSimple);

    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void SelectCteSimple() => Run(BenchmarkFeatureId.CteSimple);

    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void WindowRowNumber() => Run(BenchmarkFeatureId.WindowRowNumber);

    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void WindowLag() => Run(BenchmarkFeatureId.WindowLag);

    /// <summary>
    /// EN: Executes a LEAD window benchmark query.
    /// PT: Executa uma consulta de benchmark de janela com LEAD.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowLead() => Run(BenchmarkFeatureId.WindowLead);

    /// <summary>
    /// EN: Executes a RANK and DENSE_RANK window benchmark query.
    /// PT: Executa uma consulta de benchmark de janela com RANK e DENSE_RANK.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowRankDenseRank() => Run(BenchmarkFeatureId.WindowRankDenseRank);

    /// <summary>
    /// EN: Executes a FIRST_VALUE and LAST_VALUE window benchmark query.
    /// PT: Executa uma consulta de benchmark de janela com FIRST_VALUE e LAST_VALUE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowFirstLastValue() => Run(BenchmarkFeatureId.WindowFirstLastValue);

    /// <summary>
    /// EN: Executes an NTILE window benchmark query.
    /// PT: Executa uma consulta de benchmark de janela com NTILE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowNtile() => Run(BenchmarkFeatureId.WindowNtile);

    /// <summary>
    /// EN: Executes a PERCENT_RANK and CUME_DIST window benchmark query.
    /// PT: Executa uma consulta de benchmark de janela com PERCENT_RANK e CUME_DIST.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowPercentRankCumeDist() => Run(BenchmarkFeatureId.WindowPercentRankCumeDist);

    /// <summary>
    /// EN: Executes an NTH_VALUE window benchmark query.
    /// PT: Executa uma consulta de benchmark de janela com NTH_VALUE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowNthValue() => Run(BenchmarkFeatureId.WindowNthValue);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowRankDenseRank() => Run(BenchmarkFeatureId.WindowRankDenseRank);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowFirstLastValue() => Run(BenchmarkFeatureId.WindowFirstLastValue);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowNtile() => Run(BenchmarkFeatureId.WindowNtile);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowPercentRankCumeDist() => Run(BenchmarkFeatureId.WindowPercentRankCumeDist);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowNthValue() => Run(BenchmarkFeatureId.WindowNthValue);

    /// <summary>
    /// EN: Executes an EXISTS predicate benchmark query.
    /// PT: Executa uma consulta de benchmark com predicado EXISTS.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectExistsPredicate() => Run(BenchmarkFeatureId.SelectExistsPredicate);

    /// <summary>
    /// EN: Executes a NOT EXISTS predicate benchmark query.
    /// PT: Executa uma consulta de benchmark com predicado NOT EXISTS.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectNotExistsPredicate() => Run(BenchmarkFeatureId.SelectNotExistsPredicate);

    /// <summary>
    /// EN: Executes a LEFT JOIN anti-join benchmark query.
    /// PT: Executa uma consulta de benchmark com anti-join via LEFT JOIN.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectLeftJoinAntiJoin() => Run(BenchmarkFeatureId.SelectLeftJoinAntiJoin);

    /// <summary>
    /// EN: Executes a correlated COUNT subquery benchmark.
    /// PT: Executa um benchmark com subconsulta correlacionada de COUNT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectCorrelatedCount() => Run(BenchmarkFeatureId.SelectCorrelatedCount);

    /// <summary>
    /// EN: Executes a scalar subquery and CASE matrix benchmark.
    /// PT: Executa um benchmark com subconsulta escalar e matriz CASE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectScalarCaseMatrix() => Run(BenchmarkFeatureId.SelectScalarCaseMatrix);

    /// <summary>
    /// EN: Executes a GROUP BY HAVING benchmark query.
    /// PT: Executa uma consulta de benchmark com GROUP BY HAVING.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void GroupByHaving() => Run(BenchmarkFeatureId.GroupByHaving);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGroupByHaving() => Run(BenchmarkFeatureId.GroupByHaving);

    /// <summary>
    /// EN: Executes a UNION ALL projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeccao UNION ALL.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void UnionAllProjection() => Run(BenchmarkFeatureId.UnionAllProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectUnionAllProjection() => Run(BenchmarkFeatureId.UnionAllProjection);

    /// <summary>
    /// EN: Executes a UNION projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeção UNION.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void UnionDistinctProjection() => Run(BenchmarkFeatureId.UnionDistinctProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectUnionDistinctProjection() => Run(BenchmarkFeatureId.UnionDistinctProjection);

    /// <summary>
    /// EN: Executes a DISTINCT projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeccao DISTINCT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void DistinctProjection() => Run(BenchmarkFeatureId.DistinctProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectDistinctProjection() => Run(BenchmarkFeatureId.DistinctProjection);

    /// <summary>
    /// EN: Executes a multi-join aggregate benchmark query.
    /// PT: Executa uma consulta de benchmark com agregacao e multiplos joins.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void MultiJoinAggregate() => Run(BenchmarkFeatureId.MultiJoinAggregate);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectMultiJoinAggregate() => Run(BenchmarkFeatureId.MultiJoinAggregate);

    /// <summary>
    /// EN: Executes a scalar subquery benchmark query.
    /// PT: Executa uma consulta de benchmark com subconsulta escalar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectScalarSubquery() => Run(BenchmarkFeatureId.SelectScalarSubquery);

    /// <summary>
    /// EN: Executes an IN subquery benchmark query.
    /// PT: Executa uma consulta de benchmark com subconsulta IN.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectInSubquery() => Run(BenchmarkFeatureId.SelectInSubquery);

    /// <summary>
    /// EN: Executes a NOT IN subquery benchmark query.
    /// PT: Executa uma consulta de benchmark com subconsulta NOT IN.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectNotInSubquery() => Run(BenchmarkFeatureId.SelectNotInSubquery);

    /// <summary>
    /// EN: Executes a combined BETWEEN, LIKE, and ORDER BY benchmark query.
    /// PT: Executa uma consulta de benchmark combinada com BETWEEN, LIKE e ORDER BY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectBetweenLikeOrderByMatrix() => Run(BenchmarkFeatureId.SelectBetweenLikeOrderByMatrix);

    /// <summary>
    /// EN: Executes a CROSS APPLY benchmark query.
    /// PT: Executa uma consulta de benchmark com CROSS APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void CrossApplyProjection() => Run(BenchmarkFeatureId.CrossApplyProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectCrossApplyProjection() => Run(BenchmarkFeatureId.CrossApplyProjection);

    /// <summary>
    /// EN: Executes an OUTER APPLY benchmark query.
    /// PT: Executa uma consulta de benchmark com OUTER APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void OuterApplyProjection() => Run(BenchmarkFeatureId.OuterApplyProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectOuterApplyProjection() => Run(BenchmarkFeatureId.OuterApplyProjection);

    /// <summary>
    /// EN: Executes a paged name projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeção paginada de nomes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PagedNameProjection() => Run(BenchmarkFeatureId.PagedNameProjection);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectPagedNameProjection() => Run(BenchmarkFeatureId.PagedNameProjection);

    /// <summary>
    /// EN: Executes a batch-reader benchmark that returns multiple result sets.
    /// PT: Executa um benchmark de leitura em lote que retorna varios conjuntos de resultado.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchReaderMultiResult() => Run(BenchmarkFeatureId.BatchReaderMultiResult);

    /// <summary>
    /// EN: Executes a batch benchmark that includes transaction control statements.
    /// PT: Executa um benchmark em lote que inclui comandos de controle de transacao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchTransactionControl() => Run(BenchmarkFeatureId.BatchTransactionControl);

    /// <summary>
    /// EN: Executes a simple SELECT parser benchmark.
    /// PT: Executa um benchmark do parser para um SELECT simples.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseSimpleSelect() => Run(BenchmarkFeatureId.ParseSimpleSelect);

    /// <summary>
    /// EN: Executes a complex join parser benchmark.
    /// PT: Executa um benchmark do parser para um join complexo.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseComplexJoin() => Run(BenchmarkFeatureId.ParseComplexJoin);

    /// <summary>
    /// EN: Executes an INSERT RETURNING parser benchmark.
    /// PT: Executa um benchmark do parser para INSERT RETURNING.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseInsertReturning() => Run(BenchmarkFeatureId.ParseInsertReturning);

    /// <summary>
    /// EN: Executes an ON CONFLICT DO UPDATE parser benchmark.
    /// PT: Executa um benchmark do parser para ON CONFLICT DO UPDATE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseOnConflictDoUpdate() => Run(BenchmarkFeatureId.ParseOnConflictDoUpdate);

    /// <summary>
    /// EN: Executes a JSON extract parser benchmark.
    /// PT: Executa um benchmark do parser para extracao JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void ParseJsonExtract() => Run(BenchmarkFeatureId.ParseJsonExtract);

    /// <summary>
    /// EN: Executes a string-aggregate WITHIN GROUP parser benchmark.
    /// PT: Executa um benchmark do parser para string aggregate WITHIN GROUP.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void ParseStringAggregateWithinGroup() => Run(BenchmarkFeatureId.ParseStringAggregateWithinGroup);

    /// <summary>
    /// EN: Executes an auto-dialect TOP, LIMIT, or FETCH parser benchmark.
    /// PT: Executa um benchmark do parser para TOP, LIMIT ou FETCH com autodialeto.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseAutoDialectTopLimitFetch() => Run(BenchmarkFeatureId.ParseAutoDialectTopLimitFetch);

    /// <summary>
    /// EN: Executes a multi-statement batch parser benchmark.
    /// PT: Executa um benchmark do parser para lote com multiplas instrucoes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void ParseMultiStatementBatch() => Run(BenchmarkFeatureId.ParseMultiStatementBatch);

    /// <summary>
    /// EN: Executes a JSON insert cast benchmark.
    /// PT: Executa um benchmark de cast de JSON em insert.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonInsertCast() => Run(BenchmarkFeatureId.JsonInsertCast);

    /// <summary>
    /// EN: Executes the JSON insert cast benchmark and expects a null result.
    /// PT: Executa o benchmark de insert e cast de JSON e espera um resultado nulo.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonInsertCastReturnsNull() => Run(BenchmarkFeatureId.JsonInsertCastReturnsNull);

    /// <summary>
    /// EN: Executes a row-count-in-batch benchmark.
    /// PT: Executa um benchmark de contagem de linhas em lote.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void RowCountInBatch() => Run(BenchmarkFeatureId.RowCountInBatch);

    /// <summary>
    /// EN: Executes the batch row-count benchmark.
    /// PT: Executa o benchmark de contagem de linhas em lote.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchRowCountInBatch() => Run(BenchmarkFeatureId.BatchRowCountInBatch);

    /// <summary>
    /// EN: Executes a pivot-count benchmark.
    /// PT: Executa um benchmark de contagem em pivot.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PivotCount() => Run(BenchmarkFeatureId.PivotCount);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectPivotCount() => Run(BenchmarkFeatureId.PivotCount);

    /// <summary>
    /// EN: Executes an insert-returning benchmark.
    /// PT: Executa um benchmark de insert com retorno.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ReturningInsert() => Run(BenchmarkFeatureId.ReturningInsert);

    /// <summary>
    /// EN: Executes a batch returning insert benchmark.
    /// PT: Executa um benchmark de batch insert com retorno.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void BatchReturningInsert() => Run(BenchmarkFeatureId.BatchReturningInsert);

    /// <summary>
    /// EN: Executes an update-returning benchmark.
    /// PT: Executa um benchmark de update com retorno.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ReturningUpdate() => Run(BenchmarkFeatureId.ReturningUpdate);

    /// <summary>
    /// EN: Executes a basic merge benchmark.
    /// PT: Executa um benchmark de merge basico.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void MergeBasic() => Run(BenchmarkFeatureId.MergeBasic);

    /// <summary>
    /// EN: Executes a partition-pruning select benchmark.
    /// PT: Executa um benchmark de select com partition pruning.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PartitionPruningSelect() => Run(BenchmarkFeatureId.PartitionPruningSelect);

    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectPartitionPruning() => Run(BenchmarkFeatureId.PartitionPruningSelect);

    /// <summary>
    /// EN: Executes an execution-plan benchmark.
    /// PT: Executa um benchmark de execution plan.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void ExecutionPlan() => Run(BenchmarkFeatureId.ExecutionPlan);

    /// <summary>
    /// EN: Executes an execution-plan benchmark for SELECT statements.
    /// PT: Executa um benchmark de execution plan para instrucoes SELECT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void ExecutionPlanSelect() => Run(BenchmarkFeatureId.ExecutionPlanSelect);

    /// <summary>
    /// EN: Executes an execution-plan benchmark for join queries.
    /// PT: Executa um benchmark de execution plan para consultas com join.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void ExecutionPlanJoin() => Run(BenchmarkFeatureId.ExecutionPlanJoin);

    /// <summary>
    /// EN: Executes an execution-plan benchmark for non-query DML statements.
    /// PT: Executa um benchmark de execution plan para instrucoes DML non-query.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void ExecutionPlanDml() => Run(BenchmarkFeatureId.ExecutionPlanDml);

    /// <summary>
    /// EN: Executes a debug-trace benchmark for SELECT statements.
    /// PT: Executa um benchmark de debug trace para instrucoes SELECT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void DebugTraceSelect() => Run(BenchmarkFeatureId.DebugTraceSelect);

    /// <summary>
    /// EN: Executes a debug-trace benchmark for batch statements.
    /// PT: Executa um benchmark de debug trace para instrucoes em lote.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void DebugTraceBatch() => Run(BenchmarkFeatureId.DebugTraceBatch);

    /// <summary>
    /// EN: Executes a debug-trace benchmark for JSON output.
    /// PT: Executa um benchmark de debug trace para saida JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void DebugTraceJson() => Run(BenchmarkFeatureId.DebugTraceJson);

    /// <summary>
    /// EN: Executes a benchmark that reads the last execution-plan history.
    /// PT: Executa um benchmark que le o historico do ultimo execution plan.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("diagnostics")]
    public void LastExecutionPlansHistory() => Run(BenchmarkFeatureId.LastExecutionPlansHistory);

    /// <summary>
    /// EN: Executes the temporary-table create and use benchmark.
    /// PT: Executa o benchmark de criar e usar tabela temporaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void TempTableCreateAndUse() => Run(BenchmarkFeatureId.TempTableCreateAndUse);

    /// <summary>
    /// EN: Executes the temporary-table rollback benchmark.
    /// PT: Executa o benchmark de rollback com tabela temporaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void TempTableRollback() => Run(BenchmarkFeatureId.TempTableRollback);

    /// <summary>
    /// EN: Executes the temporary-table cross-connection isolation benchmark.
    /// PT: Executa o benchmark de isolamento de tabela temporaria entre conexoes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void TempTableCrossConnectionIsolation() => Run(BenchmarkFeatureId.TempTableCrossConnectionIsolation);

    /// <summary>
    /// EN: Executes the volatile-data reset benchmark.
    /// PT: Executa o benchmark de reset de dados volateis.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void ResetVolatileData() => Run(BenchmarkFeatureId.ResetVolatileData);

    /// <summary>
    /// EN: Executes the full volatile-data reset benchmark.
    /// PT: Executa o benchmark de reset completo de dados volateis.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void ResetAllVolatileData() => Run(BenchmarkFeatureId.ResetAllVolatileData);

    /// <summary>
    /// EN: Executes the connection reopen benchmark after a close.
    /// PT: Executa o benchmark de reabrir a conexao depois de fechar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void ConnectionReopenAfterClose() => Run(BenchmarkFeatureId.ConnectionReopenAfterClose);

    /// <summary>
    /// EN: Executes a schema snapshot export benchmark.
    /// PT: Executa um benchmark de exportacao de snapshot de schema.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotExport() => Run(BenchmarkFeatureId.SchemaSnapshotExport);

    /// <summary>
    /// EN: Executes a schema snapshot to JSON benchmark.
    /// PT: Executa um benchmark de snapshot de schema para JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotToJson() => Run(BenchmarkFeatureId.SchemaSnapshotToJson);

    /// <summary>
    /// EN: Executes a schema snapshot load-from-JSON benchmark.
    /// PT: Executa um benchmark de carregamento de snapshot de schema a partir de JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotLoadJson() => Run(BenchmarkFeatureId.SchemaSnapshotLoadJson);

    /// <summary>
    /// EN: Executes a schema snapshot apply benchmark.
    /// PT: Executa um benchmark de aplicacao de snapshot de schema.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotApply() => Run(BenchmarkFeatureId.SchemaSnapshotApply);

    /// <summary>
    /// EN: Executes a schema snapshot round-trip benchmark.
    /// PT: Executa um benchmark de round-trip de snapshot de schema.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotRoundTrip() => Run(BenchmarkFeatureId.SchemaSnapshotRoundTrip);

    /// <summary>
    /// EN: Executes a schema snapshot comparison benchmark.
    /// PT: Executa um benchmark de comparacao de snapshot de schema.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("snapshot")]
    public void SchemaSnapshotCompare() => Run(BenchmarkFeatureId.SchemaSnapshotCompare);

    /// <summary>
    /// EN: Executes the fluent schema builder benchmark.
    /// PT: Executa o benchmark do builder fluente de schema.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentSchemaBuild() => Run(BenchmarkFeatureId.FluentSchemaBuild);

    /// <summary>
    /// EN: Executes the fluent seed benchmark for 100 rows.
    /// PT: Executa o benchmark de seed fluente para 100 linhas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentSeed100() => Run(BenchmarkFeatureId.FluentSeed100);

    /// <summary>
    /// EN: Executes the fluent seed benchmark for 1000 rows.
    /// PT: Executa o benchmark de seed fluente para 1000 linhas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentSeed1000() => Run(BenchmarkFeatureId.FluentSeed1000);

    /// <summary>
    /// EN: Executes the fluent scenario composition benchmark.
    /// PT: Executa o benchmark de composição de cenario fluente.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentScenarioCompose() => Run(BenchmarkFeatureId.FluentScenarioCompose);

}

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
    /// EN: Executes a primary-key update benchmark.
    /// PT: Executa um benchmark de atualizacao por chave primaria.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void UpdateByPk() => Run(BenchmarkFeatureId.UpdateByPk);

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
    /// EN: Executes a provider-specific upsert benchmark.
    /// PT: Executa um benchmark de upsert especifico do provedor.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void Upsert() => Run(BenchmarkFeatureId.Upsert);

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
    /// EN: Executes a stored procedure call benchmark.
    /// PT: Executa um benchmark de chamada de procedimento armazenado.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void StoredProcedureCall() => Run(BenchmarkFeatureId.StoredProcedureCall);

    ///// <summary>
    ///// EN: Executes a sequence next-value benchmark.
    ///// PT: Executa um benchmark de proximo valor de sequencia.
    ///// </summary>
    //[Benchmark]
    //[BenchmarkCategory("core")]
    //public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);

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

    private static readonly object _setupLogSync = new();
    protected void LogSetupIssue(Exception ex)
    {
        var root = ex.GetBaseException();

        var message =
            $"[SETUP-{root.GetType().Name}] {root.ToString()}";

        Console.WriteLine(message);

        var logEntry =
            $"{DateTime.UtcNow:O} {message}{Environment.NewLine}{root.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        lock (_setupLogSync)
        {
            var directory = GetLogDirectory();
            Directory.CreateDirectory(directory);

            var file = Path.Combine(directory, GetSafeLogFileName($"{GetType().FullName}-setup-errors.log"));
            if (!File.Exists(file))
                File.Create(file).Dispose();
            File.AppendAllText(
                file,
                logEntry);
        }
    }

    private static string GetLogDirectory() => Path.Combine(AppContext.BaseDirectory, "Logs");

    private static string GetSafeLogFileName(string fileName)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitized = new char[fileName.Length];

        for (var i = 0; i < fileName.Length; i++)
        {
            sanitized[i] = Array.IndexOf(invalidChars, fileName[i]) >= 0 ? '_' : fileName[i];
        }

        return new string(sanitized).Trim();
    }

    private static readonly object _logSync = new();
    private static readonly HashSet<string> Errors = [];
    protected virtual void LogBenchmarkIssue(BenchmarkFeatureId feature, Exception ex)
    {
        var root = ex.GetBaseException();
        var message = root is NotSupportedException
            ? $"[NA-{root.GetType().Name}] {feature}: {root.Message}{Environment.NewLine}{Environment.NewLine}"
            : $"[NA-{root.GetType().Name}] {feature}: {root.Message} -- {ex.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        Console.WriteLine(message);

        lock (_logSync)
        {
            var errorKey = $"{GetType().FullName}|{feature}|{root.GetType().FullName}|{root.Message}";
            if (Errors.Contains(errorKey))
                return;
            Errors.Add(errorKey);

            var directory = GetLogDirectory();
            Directory.CreateDirectory(directory);

            var file = Path.Combine(directory, GetSafeLogFileName($"{GetType().FullName}-errors.log"));
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

    /// <summary>
    /// EN: Executes a UNION ALL projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeccao UNION ALL.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void UnionAllProjection() => Run(BenchmarkFeatureId.UnionAllProjection);

    /// <summary>
    /// EN: Executes a UNION projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeção UNION.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void UnionDistinctProjection() => Run(BenchmarkFeatureId.UnionDistinctProjection);

    /// <summary>
    /// EN: Executes a DISTINCT projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeccao DISTINCT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void DistinctProjection() => Run(BenchmarkFeatureId.DistinctProjection);

    /// <summary>
    /// EN: Executes a multi-join aggregate benchmark query.
    /// PT: Executa uma consulta de benchmark com agregacao e multiplos joins.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void MultiJoinAggregate() => Run(BenchmarkFeatureId.MultiJoinAggregate);

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

    /// <summary>
    /// EN: Executes an OUTER APPLY benchmark query.
    /// PT: Executa uma consulta de benchmark com OUTER APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void OuterApplyProjection() => Run(BenchmarkFeatureId.OuterApplyProjection);

    /// <summary>
    /// EN: Executes a paged name projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeção paginada de nomes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PagedNameProjection() => Run(BenchmarkFeatureId.PagedNameProjection);

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
    /// EN: Executes a row-count-in-batch benchmark.
    /// PT: Executa um benchmark de contagem de linhas em lote.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void RowCountInBatch() => Run(BenchmarkFeatureId.RowCountInBatch);

    /// <summary>
    /// EN: Executes a pivot-count benchmark.
    /// PT: Executa um benchmark de contagem em pivot.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PivotCount() => Run(BenchmarkFeatureId.PivotCount);

    /// <summary>
    /// EN: Executes an insert-returning benchmark.
    /// PT: Executa um benchmark de insert com retorno.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ReturningInsert() => Run(BenchmarkFeatureId.ReturningInsert);

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
    /// PT: Executa o benchmark de composicao de cenario fluente.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setup")]
    public void FluentScenarioCompose() => Run(BenchmarkFeatureId.FluentScenarioCompose);

}

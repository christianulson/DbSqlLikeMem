namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Executes a 10-row batch insert benchmark.
    /// PT-br: Executa um benchmark de insercao em lote de 10 linhas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void InsertBatch10() => Run(BenchmarkFeatureId.InsertBatch10);

    /// <summary>
    /// EN: Executes a savepoint creation benchmark.
    /// PT-br: Executa um benchmark de criacao de savepoint.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void SavepointCreate() => Run(BenchmarkFeatureId.SavepointCreate);

    /// <summary>
    /// EN: Executes a rollback-to-savepoint benchmark.
    /// PT-br: Executa um benchmark de rollback ate o savepoint.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void RollbackToSavepoint() => Run(BenchmarkFeatureId.RollbackToSavepoint);

    /// <summary>
    /// EN: Executes a savepoint release benchmark.
    /// PT-br: Executa um benchmark de liberacao de savepoint.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void ReleaseSavepoint() => Run(BenchmarkFeatureId.ReleaseSavepoint);

    /// <summary>
    /// EN: Executes a nested savepoint flow benchmark.
    /// PT-br: Executa um benchmark de fluxo aninhado de savepoints.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transactions")]
    public void NestedSavepointFlow() => Run(BenchmarkFeatureId.NestedSavepointFlow);

    /// <summary>
    /// EN: Executes a batch insert benchmark.
    /// PT-br: Executa um benchmark de insercao em lote.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchInsert10() => Run(BenchmarkFeatureId.BatchInsert10);

    /// <summary>
    /// EN: Executes a 100-row batch insert benchmark.
    /// PT-br: Executa um benchmark de insercao em lote de 100 linhas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchInsert100() => Run(BenchmarkFeatureId.BatchInsert100);

    /// <summary>
    /// EN: Executes a mixed read-write batch benchmark.
    /// PT-br: Executa um benchmark em lote com leitura e escrita misturadas.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchMixedReadWrite() => Run(BenchmarkFeatureId.BatchMixedReadWrite);

    /// <summary>
    /// EN: Executes a scalar batch benchmark.
    /// PT-br: Executa um benchmark em lote escalar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchScalar() => Run(BenchmarkFeatureId.BatchScalar);

    /// <summary>
    /// EN: Executes a non-query batch benchmark.
    /// PT-br: Executa um benchmark em lote sem retorno de query.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchNonQuery() => Run(BenchmarkFeatureId.BatchNonQuery);

    /// <summary>
    /// EN: Executes a batch-reader benchmark that returns multiple result sets.
    /// PT-br: Executa um benchmark de leitura em lote que retorna varios conjuntos de resultado.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchReaderMultiResult() => Run(BenchmarkFeatureId.BatchReaderMultiResult);

    /// <summary>
    /// EN: Executes a batch benchmark that includes transaction control statements.
    /// PT-br: Executa um benchmark em lote que inclui comandos de controle de transacao.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchTransactionControl() => Run(BenchmarkFeatureId.BatchTransactionControl);

    /// <summary>
    /// EN: Executes a simple SELECT parser benchmark.
    /// PT-br: Executa um benchmark do parser para um SELECT simples.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseSimpleSelect() => Run(BenchmarkFeatureId.ParseSimpleSelect);

    /// <summary>
    /// EN: Executes a complex join parser benchmark.
    /// PT-br: Executa um benchmark do parser para um join complexo.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseComplexJoin() => Run(BenchmarkFeatureId.ParseComplexJoin);

    /// <summary>
    /// EN: Executes an INSERT RETURNING parser benchmark.
    /// PT-br: Executa um benchmark do parser para INSERT RETURNING.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseInsertReturning() => Run(BenchmarkFeatureId.ParseInsertReturning);

    /// <summary>
    /// EN: Executes an ON CONFLICT DO UPDATE parser benchmark.
    /// PT-br: Executa um benchmark do parser para ON CONFLICT DO UPDATE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseOnConflictDoUpdate() => Run(BenchmarkFeatureId.ParseOnConflictDoUpdate);

    /// <summary>
    /// EN: Executes a JSON extract parser benchmark.
    /// PT-br: Executa um benchmark do parser para extracao JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void ParseJsonExtract() => Run(BenchmarkFeatureId.ParseJsonExtract);

    /// <summary>
    /// EN: Executes a string-aggregate WITHIN GROUP parser benchmark.
    /// PT-br: Executa um benchmark do parser para string aggregate WITHIN GROUP.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void ParseStringAggregateWithinGroup() => Run(BenchmarkFeatureId.ParseStringAggregateWithinGroup);

    /// <summary>
    /// EN: Executes an auto-dialect TOP, LIMIT, or FETCH parser benchmark.
    /// PT-br: Executa um benchmark do parser para TOP, LIMIT ou FETCH com autodialeto.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ParseAutoDialectTopLimitFetch() => Run(BenchmarkFeatureId.ParseAutoDialectTopLimitFetch);

    /// <summary>
    /// EN: Executes a multi-statement batch parser benchmark.
    /// PT-br: Executa um benchmark do parser para lote com multiplas instrucoes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void ParseMultiStatementBatch() => Run(BenchmarkFeatureId.ParseMultiStatementBatch);

    /// <summary>
    /// EN: Executes a JSON insert cast benchmark.
    /// PT-br: Executa um benchmark de cast de JSON em insert.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonInsertCast() => Run(BenchmarkFeatureId.JsonInsertCast);

    /// <summary>
    /// EN: Executes the JSON insert cast benchmark and expects a null result.
    /// PT-br: Executa o benchmark de insert e cast de JSON e espera um resultado nulo.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonInsertCastReturnsNull() => Run(BenchmarkFeatureId.JsonInsertCastReturnsNull);

    /// <summary>
    /// EN: Executes a row-count-in-batch benchmark.
    /// PT-br: Executa um benchmark de contagem de linhas em lote.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void RowCountInBatch() => Run(BenchmarkFeatureId.RowCountInBatch);

    /// <summary>
    /// EN: Executes the batch row-count benchmark.
    /// PT-br: Executa o benchmark de contagem de linhas em lote.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("batch")]
    public void BatchRowCountInBatch() => Run(BenchmarkFeatureId.BatchRowCountInBatch);

    /// <summary>
    /// EN: Executes a pivot-count benchmark.
    /// PT-br: Executa um benchmark de contagem em pivot.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PivotCount() => Run(BenchmarkFeatureId.PivotCount);

    /// <summary>
    /// EN: Executes the select variant of the pivot-count benchmark.
    /// PT-br: Executa a variante select do benchmark de contagem em pivot.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectPivotCount() => Run(BenchmarkFeatureId.PivotCount);

    /// <summary>
    /// EN: Executes an insert-returning benchmark.
    /// PT-br: Executa um benchmark de insert com retorno.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ReturningInsert() => Run(BenchmarkFeatureId.ReturningInsert);

    /// <summary>
    /// EN: Executes a batch returning insert benchmark.
    /// PT-br: Executa um benchmark de batch insert com retorno.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void BatchReturningInsert() => Run(BenchmarkFeatureId.BatchReturningInsert);

    /// <summary>
    /// EN: Executes an update-returning benchmark.
    /// PT-br: Executa um benchmark de update com retorno.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void ReturningUpdate() => Run(BenchmarkFeatureId.ReturningUpdate);

    /// <summary>
    /// EN: Executes a basic merge benchmark.
    /// PT-br: Executa um benchmark de merge basico.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void MergeBasic() => Run(BenchmarkFeatureId.MergeBasic);
}

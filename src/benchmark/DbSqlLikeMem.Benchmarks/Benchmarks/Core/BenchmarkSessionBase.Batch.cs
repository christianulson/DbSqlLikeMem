namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes a batch benchmark that returns multiple result sets and keeps the result alive.
    /// PT-br: Executa um benchmark em lote que retorna varios conjuntos de resultado e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.BatchReaderMultiResult)]
    protected virtual void RunBatchReaderMultiResult()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchReaderMultiResult(1, 2);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a batch benchmark that includes transaction control statements and keeps the result alive.
    /// PT-br: Executa um benchmark em lote que inclui comandos de controle de transacao e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.BatchTransactionControl)]
    protected virtual void RunBatchTransactionControl()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchTransactionControl(1, 2);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a batch row-count benchmark and keeps the count alive.
    /// PT-br: Executa um benchmark de contagem de linhas em lote e mantem a contagem viva.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.RowCountInBatch)]
    protected virtual void RunRowCountInBatch()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunRowCountInBatch(1, 2);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the batch row-count benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de contagem de linhas em lote e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.BatchRowCountInBatch)]
    protected virtual void RunBatchRowCountInBatch()
        => RunRowCountInBatch();

    /// <summary>
    /// EN: Executes the batch insert benchmark for ten rows and keeps the returned count alive.
    /// PT-br: Executa o benchmark de insercao em lote de dez linhas e mantem viva a contagem retornada.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.BatchInsert10)]
    protected virtual void RunBatchInsert10()
    {
        var state = GetPreparedBatchUsersState("BatchInsert10");
        var count = state.RunBatchInsert(10);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the batch insert benchmark for one hundred rows and keeps the returned count alive.
    /// PT-br: Executa o benchmark de insercao em lote de cem linhas e mantem viva a contagem retornada.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.BatchInsert100)]
    protected virtual void RunBatchInsert100()
    {
        var state = GetPreparedBatchUsersState("BatchInsert100");
        var count = state.RunBatchInsert(100);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the mixed read-write batch benchmark and keeps the returned value alive.
    /// PT-br: Executa o benchmark de lote misto com leitura e escrita e mantem vivo o valor retornado.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.BatchMixedReadWrite)]
    protected virtual void RunBatchMixedReadWrite()
    {
        var state = GetPreparedBatchUsersState("BatchMixedReadWrite");
        var value = state.RunBatchMixedReadWrite(1, 2);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the scalar batch benchmark and keeps the returned value alive.
    /// PT-br: Executa o benchmark escalar em lote e mantem vivo o valor retornado.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.BatchScalar)]
    protected virtual void RunBatchScalar()
    {
        var state = GetPreparedBatchUsersState("BatchScalar");
        var value = state.RunBatchScalar(1, 2);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the non-query batch benchmark and keeps the returned count alive.
    /// PT-br: Executa o benchmark sem query em lote e mantem viva a contagem retornada.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.BatchNonQuery)]
    protected virtual void RunBatchNonQuery()
    {
        var state = GetPreparedBatchUsersState("BatchNonQuery");
        var count = state.RunBatchNonQuery(1, 2, 2, 1);
        GC.KeepAlive(count);
    }
}

using DbSqlLikeMem.TestTools.Performance;

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Resets volatile connection state and validates the result through the shared service.
    /// PT-br: Reinicia o estado volatil da conexao e valida o resultado pelo service compartilhado.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ResetVolatileData)]
    protected virtual void RunResetVolatileData()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var repo = new RepoService(() => connection, Dialect);
        var context = new FidelityTestContext();
        var service = new ConnectionLifecycleResetVolatileDataServiceTest(repo, context);
        var result = service.RunTestAsync().GetAwaiter().GetResult();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Resets all volatile connection state and validates the result through the shared service.
    /// PT-br: Reinicia todo o estado volatil da conexao e valida o resultado pelo service compartilhado.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ResetAllVolatileData)]
    protected virtual void RunResetAllVolatileData()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var repo = new RepoService(() => connection, Dialect);
        var context = new FidelityTestContext();
        var service = new ConnectionLifecycleResetAllVolatileDataServiceTest(repo, context);
        var result = service.RunTestAsync().GetAwaiter().GetResult();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Reopens a connection after close and validates the lifecycle behavior through the shared service.
    /// PT-br: Reabre uma conexao apos o fechamento e valida o comportamento de ciclo de vida pelo service compartilhado.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ConnectionReopenAfterClose)]
    protected virtual void RunConnectionReopenAfterClose()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var repo = new RepoService(() => connection, Dialect);
        var context = new FidelityTestContext();
        var service = new ConnectionLifecycleReopenAfterServiceTest(repo, context);
        var result = service.RunTestAsync().GetAwaiter().GetResult();
        GC.KeepAlive(result);
    }
}

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the baseline string-function benchmark.
    /// PT-br: Executa o benchmark base de funcoes de string.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.StringBasicFunctions)]
    protected virtual void RunStringBasicFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunStringBasicFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the PARSE-family benchmark.
    /// PT-br: Executa o benchmark da familia PARSE.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseFamily)]
    protected virtual void RunParseFamily()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunParseFamilyAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SOUNDEX benchmark.
    /// PT-br: Executa o benchmark SOUNDEX.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.Soundex)]
    protected virtual void RunSoundex()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSoundexAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the compression benchmark.
    /// PT-br: Executa o benchmark de compressao.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.Compression)]
    protected virtual void RunCompression()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunCompressionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the approximate count distinct benchmark.
    /// PT-br: Executa o benchmark de contagem distinta aproximada.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ApproxCountDistinct)]
    protected virtual void RunApproxCountDistinct()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunApproxCountDistinctAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the baseline string-function benchmark.
    /// PT-br: Executa o benchmark base de funcoes de string.
    /// </summary>
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
    protected virtual void RunApproxCountDistinct()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunApproxCountDistinctAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
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
}

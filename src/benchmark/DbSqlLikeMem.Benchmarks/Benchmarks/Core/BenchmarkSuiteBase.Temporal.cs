namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Executes the current timestamp benchmark.
    /// PT: Executa o benchmark do timestamp atual.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalCurrentTimestamp() => Run(BenchmarkFeatureId.TemporalCurrentTimestamp);

    /// <summary>
    /// EN: Executes the date add benchmark.
    /// PT: Executa o benchmark de adicao de data.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalDateAdd() => Run(BenchmarkFeatureId.TemporalDateAdd);

    /// <summary>
    /// EN: Executes the current time filter benchmark.
    /// PT: Executa o benchmark com filtro por horario atual.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("temporal")]
    public void TemporalNowWhere() => Run(BenchmarkFeatureId.TemporalNowWhere);

    /// <summary>
    /// EN: Executes the current time ordering benchmark.
    /// PT: Executa o benchmark de ordenacao por horario atual.
    /// </summary>
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
}

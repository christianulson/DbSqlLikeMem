namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Identifies the benchmark runtime or execution mode used by a session.
/// PT: Identifica o runtime ou modo de execucao de benchmark usado por uma sessao.
/// </summary>
public enum BenchmarkEngine
{
    /// <summary>
    /// EN: Uses the in-memory DbSqlLikeMem runtime.
    /// PT: Usa o runtime DbSqlLikeMem em memoria.
    /// </summary>
    DbSqlLikeMem,
    /// <summary>
    /// EN: Uses a Testcontainers-based external runtime.
    /// PT: Usa um runtime externo baseado em Testcontainers.
    /// </summary>
    Testcontainers,
    /// <summary>
    /// EN: Uses a native ADO.NET runtime.
    /// PT: Usa um runtime ADO.NET nativo.
    /// </summary>
    NativeAdoNet,
    /// <summary>
    /// EN: Marks a runtime as unavailable for the current benchmark session.
    /// PT: Marca um runtime como indisponivel para a sessao de benchmark atual.
    /// </summary>
    NotAvailable
}

using System.Diagnostics;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Accumulates usage and timing metrics for in-memory DB operations.
/// PT: Acumula métricas de uso e tempo para operações no banco em memória.
/// </summary>
public sealed class DbMetrics
{
    /// <summary>
    /// EN: Number of SELECT queries executed.
    /// PT: Quantidade de consultas SELECT executadas.
    /// </summary>
    public int Selects { get; internal set; }
    /// <summary>
    /// EN: Number of INSERT operations executed.
    /// PT: Quantidade de operações INSERT executadas.
    /// </summary>
    public int Inserts { get; internal set; }
    /// <summary>
    /// EN: Number of UPDATE operations executed.
    /// PT: Quantidade de operações UPDATE executadas.
    /// </summary>
    public int Updates { get; internal set; }
    /// <summary>
    /// EN: Number of DELETE operations executed.
    /// PT: Quantidade de operações DELETE executadas.
    /// </summary>
    public int Deletes { get; internal set; }
    /// <summary>
    /// EN: Number of index lookups used to pre-filter rows during SELECT.
    /// PT: Quantidade de consultas em índice usadas para pré-filtrar linhas em SELECT.
    /// </summary>
    public int IndexLookups { get; internal set; }
    /// <summary>
    /// EN: Total elapsed time since metrics started.
    /// PT: Tempo total decorrido desde o início da coleta.
    /// </summary>
    public TimeSpan Elapsed => _sw.Elapsed;

    private readonly Stopwatch _sw = Stopwatch.StartNew();
}

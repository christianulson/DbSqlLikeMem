using System.Diagnostics;

namespace DbSqlLikeMem;

/// <summary>
/// Acumula métricas de uso e tempo para operações no banco em memória.
/// </summary>
public sealed class DbMetrics
{
    /// <summary>
    /// Quantidade de consultas SELECT executadas.
    /// </summary>
    public int Selects { get; internal set; }
    /// <summary>
    /// Quantidade de operações INSERT executadas.
    /// </summary>
    public int Inserts { get; internal set; }
    /// <summary>
    /// Quantidade de operações UPDATE executadas.
    /// </summary>
    public int Updates { get; internal set; }
    /// <summary>
    /// Quantidade de operações DELETE executadas.
    /// </summary>
    public int Deletes { get; internal set; }
    /// <summary>
    /// Tempo total decorrido desde o início da coleta.
    /// </summary>
    public TimeSpan Elapsed => _sw.Elapsed;

    private readonly Stopwatch _sw = Stopwatch.StartNew();
}

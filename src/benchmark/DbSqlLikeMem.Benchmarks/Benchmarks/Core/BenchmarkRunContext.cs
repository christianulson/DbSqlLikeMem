namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Holds the current benchmark run correlation values used by logs and exports.
/// PT-br: Guarda os valores de correlacao da execucao atual do benchmark usados por logs e exportacoes.
/// </summary>
internal static class BenchmarkRunContext
{
    /// <summary>
    /// EN: Gets the current run identifier used to separate logs and exported artifacts.
    /// PT-br: Obtém o identificador da execucao atual usado para separar logs e artefatos exportados.
    /// </summary>
    public static string RunId { get; private set; } = "uninitialized";

    /// <summary>
    /// EN: Initializes the run identifier for the current benchmark execution.
    /// PT-br: Inicializa o identificador da execucao para a execucao atual do benchmark.
    /// </summary>
    /// <param name="profile">EN: The selected benchmark profile. PT-br: O perfil de benchmark selecionado.</param>
    public static void Initialize(BenchmarkRunProfile profile)
        => RunId = $"{profile.ToString().ToLowerInvariant()}-{DateTimeOffset.UtcNow:yyyyMMddHHmmssfff}";
}

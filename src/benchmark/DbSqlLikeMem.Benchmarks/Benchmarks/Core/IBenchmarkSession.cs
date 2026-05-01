namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Defines the contract implemented by benchmark sessions that initialize resources and execute provider features.
/// PT-br: Define o contrato implementado pelas sessoes de benchmark que inicializam recursos e executam recursos do provedor.
/// </summary>
public interface IBenchmarkSession : IDisposable
{
    /// <summary>
    /// EN: Gets the provider-specific SQL dialect used to generate benchmark commands.
    /// PT-br: Obtem o dialeto SQL especifico do provedor usado para gerar os comandos de benchmark.
    /// </summary>
    ProviderSqlDialect Dialect { get; }

    /// <summary>
    /// EN: Gets the engine or runtime used by the benchmark session.
    /// PT-br: Obtém o mecanismo ou runtime usado pela sessão de benchmark.
    /// </summary>
    BenchmarkEngine Engine { get; }

    /// <summary>
    /// EN: Prepares the session state required before running benchmarks.
    /// PT-br: Prepara o estado da sessão necessário antes da execução dos benchmarks.
    /// </summary>
    void Initialize();

    /// <summary>
    /// EN: Executes the benchmark logic associated with the requested feature.
    /// PT-br: Executa a lógica de benchmark associada ao recurso solicitado.
    /// </summary>
    /// <param name="feature">EN: The benchmark feature to execute. PT-br: O recurso de benchmark a ser executado.</param>
    void Execute(BenchmarkFeatureId feature);
}

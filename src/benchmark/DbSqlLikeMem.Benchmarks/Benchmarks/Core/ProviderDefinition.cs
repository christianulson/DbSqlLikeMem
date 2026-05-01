namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Describes a benchmark provider, including its display metadata and supported benchmark capabilities.
/// PT-br: Descreve um provedor de benchmark, incluindo seus metadados de exibicao e capacidades suportadas.
/// </summary>
/// <param name="Id">EN: The provider identifier. PT-br: O identificador do provedor.</param>
/// <param name="DisplayName">EN: The human-readable provider name. PT-br: O nome legivel do provedor.</param>
/// <param name="LatestSimulatedVersion">EN: The latest simulated version exposed by the benchmark suite. PT-br: A ultima versao simulada exposta pela suite de benchmark.</param>
/// <param name="ExternalEngine">EN: The external engine used for comparison runs. PT-br: O mecanismo externo usado nas execucoes de comparacao.</param>
/// <param name="ExternalImage">EN: The container image used for external comparison runs. PT-br: A imagem de container usada nas execucoes externas de comparacao.</param>
/// <param name="SupportsUpsert">EN: Indicates whether the provider supports upsert benchmarks. PT-br: Indica se o provedor suporta benchmarks de upsert.</param>
/// <param name="SupportsSequence">EN: Indicates whether the provider supports sequence benchmarks. PT-br: Indica se o provedor suporta benchmarks de sequence.</param>
/// <param name="SupportsStringAggregate">EN: Indicates whether the provider supports string-aggregate benchmarks. PT-br: Indica se o provedor suporta benchmarks de agregacao de strings.</param>
/// <param name="SupportsComparableBenchmarks">EN: Indicates whether the provider participates in comparable benchmark runs. PT-br: Indica se o provedor participa de execucoes comparaveis de benchmark.</param>
/// <param name="IndexRefs">EN: The documentation or wiki index references for the provider. PT-br: As referencias de indice de documentacao ou wiki do provedor.</param>
/// <param name="Notes">EN: Additional notes about the provider. PT-br: Observacoes adicionais sobre o provedor.</param>
public sealed record ProviderDefinition(
    ProviderId Id,
    string DisplayName,
    string LatestSimulatedVersion,
    BenchmarkEngine ExternalEngine,
    string? ExternalImage,
    bool SupportsUpsert,
    bool SupportsSequence,
    bool SupportsStringAggregate,
    bool SupportsComparableBenchmarks,
    string[] IndexRefs,
    string? Notes = null);

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Describes a benchmark feature, including its display metadata, comparability flag, and documentation references.
/// PT-br: Descreve um recurso de benchmark, incluindo seus metadados de exibição, indicador de comparabilidade e referências de documentação.
/// </summary>
/// <param name="Id">EN: The feature identifier. PT-br: O identificador do recurso.</param>
/// <param name="DisplayName">EN: The human-readable feature name. PT-br: O nome legível do recurso.</param>
/// <param name="Category">EN: The logical category used to group the feature. PT-br: A categoria lógica usada para agrupar o recurso.</param>
/// <param name="Comparable">EN: Indicates whether the feature participates in comparable benchmark runs. PT-br: Indica se o recurso participa de execuções comparáveis de benchmark.</param>
/// <param name="IndexRefs">EN: The documentation or wiki index references associated with the feature. PT-br: As referências de índice de documentação ou wiki associadas ao recurso.</param>
/// <param name="Notes">EN: Additional notes about the feature. PT-br: Observações adicionais sobre o recurso.</param>
public sealed record FeatureDefinition(
    BenchmarkFeatureId Id,
    string DisplayName,
    string Category,
    bool Comparable,
    string[] IndexRefs,
    string? Notes = null);

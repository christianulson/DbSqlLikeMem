namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Describes a benchmark feature, including its stable identifier, display metadata, status, comparability flag, and documentation references.
/// PT-br: Descreve um recurso de benchmark, incluindo seu identificador estavel, metadados de exibição, status, indicador de comparabilidade e referências de documentação.
/// </summary>
/// <param name="Id">EN: The feature identifier. PT-br: O identificador do recurso.</param>
/// <param name="DisplayName">EN: The human-readable feature name. PT-br: O nome legível do recurso.</param>
/// <param name="Category">EN: The logical category used to group the feature. PT-br: A categoria lógica usada para agrupar o recurso.</param>
/// <param name="Comparable">EN: Indicates whether the feature participates in comparable benchmark runs. PT-br: Indica se o recurso participa de execuções comparáveis de benchmark.</param>
/// <param name="IndexRefs">EN: The documentation or wiki index references associated with the feature. PT-br: As referências de índice de documentação ou wiki associadas ao recurso.</param>
/// <param name="Notes">EN: Additional notes about the feature. PT-br: Observações adicionais sobre o recurso.</param>
/// <param name="StableId">EN: The stable identifier used by catalog exports when it differs from the enum name. PT-br: O identificador estavel usado pelas exportacoes do catalogo quando difere do nome do enum.</param>
/// <param name="Status">EN: The lifecycle state of the feature in the catalog. PT-br: O estado de ciclo de vida do recurso no catalogo.</param>
public sealed record FeatureDefinition(
    BenchmarkFeatureId Id,
    string DisplayName,
    string Category,
    bool Comparable,
    string[] IndexRefs,
    string? Notes = null,
    string? StableId = null,
    FeatureStatus Status = FeatureStatus.Active);

/// <summary>
/// EN: Identifies the lifecycle state of a benchmark feature in the catalog.
/// PT-br: Identifica o estado de ciclo de vida de um recurso de benchmark no catalogo.
/// </summary>
public enum FeatureStatus
{
    /// <summary>
    /// EN: Keeps the feature active in the catalog and in comparable runs.
    /// PT-br: Mantem o recurso ativo no catalogo e em execucoes comparaveis.
    /// </summary>
    Active,

    /// <summary>
    /// EN: Keeps the feature available for history while signaling that newer features should replace it.
    /// PT-br: Mantem o recurso disponivel para historico enquanto sinaliza que recursos mais novos devem substitui-lo.
    /// </summary>
    Deprecated,

    /// <summary>
    /// EN: Marks the feature as removed from active use.
    /// PT-br: Marca o recurso como removido do uso ativo.
    /// </summary>
    Removed
}

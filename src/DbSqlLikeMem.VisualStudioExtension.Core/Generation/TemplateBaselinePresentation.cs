using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Formats versioned template baseline metadata for user-facing dialogs without duplicating catalog rules in the UI.
/// PT: Formata metadados de baseline versionada de templates para dialogos voltados ao usuario sem duplicar regras do catalogo na UI.
/// </summary>
public static class TemplateBaselinePresentation
{
    /// <summary>
    /// EN: Builds a summary for template configuration flows from the informed baseline profile.
    /// PT: Monta um resumo para fluxos de configuracao de templates a partir do perfil de baseline informado.
    /// </summary>
    /// <param name="profile">EN: Baseline profile selected by the user. PT: Perfil de baseline selecionado pelo usuario.</param>
    /// <param name="reviewMetadata">EN: Optional review metadata loaded from `review-metadata.json`. PT: Metadados opcionais de revisao carregados de `review-metadata.json`.</param>
    /// <param name="todayUtc">EN: Optional UTC date used to evaluate review windows deterministically. PT: Data UTC opcional usada para avaliar janelas de revisao de forma deterministica.</param>
    public static string BuildProfileSummary(
        TemplateBaselineProfile profile,
        TemplateReviewMetadata? reviewMetadata = null,
        DateTime? todayUtc = null)
    {
        ArgumentNullException.ThrowIfNull(profile);

        var effectiveFocus = reviewMetadata is not null
            && reviewMetadata.ProfileFocusById.TryGetValue(profile.Id, out var focusFromMetadata)
            && !string.IsNullOrWhiteSpace(focusFromMetadata)
                ? focusFromMetadata
                : profile.RecommendedTestFocus;
        var effectiveCadence = string.IsNullOrWhiteSpace(reviewMetadata?.ReviewCadence)
            ? profile.ReviewCadence
            : reviewMetadata!.ReviewCadence;
        var effectiveNextReviewOn = string.IsNullOrWhiteSpace(reviewMetadata?.NextPlannedReviewOn)
            ? profile.NextPlannedReviewOn
            : reviewMetadata!.NextPlannedReviewOn;
        var lastReviewedOn = string.IsNullOrWhiteSpace(reviewMetadata?.LastReviewedOn)
            ? "n/a"
            : reviewMetadata!.LastReviewedOn;
        var evidenceSuffix = reviewMetadata is not null && reviewMetadata.EvidenceFiles.Count > 0
            ? $" Evidence files: {reviewMetadata.EvidenceFiles.Count}."
            : string.Empty;
        var governanceWarnings = TemplateBaselineGovernance.ValidateProfileAlignment(profile, reviewMetadata, todayUtc);
        var governanceSuffix = governanceWarnings.Count == 0
            ? string.Empty
            : $" Governance drift: {string.Join(" | ", governanceWarnings)}";
        var outputSuffix = $" Outputs: {profile.ModelOutputDirectory} | {profile.RepositoryOutputDirectory}.";

        return $"{profile.DisplayName} ({profile.Version}) - {profile.Description} Focus: {effectiveFocus} Review: {effectiveCadence} (last {lastReviewedOn}, next {effectiveNextReviewOn}).{outputSuffix}{evidenceSuffix}{governanceSuffix}";
    }

    /// <summary>
    /// EN: Builds a summary for mapping configuration flows using the selected profile and object type recommendation.
    /// PT: Monta um resumo para fluxos de configuracao de mapeamento usando a recomendacao do perfil e do tipo de objeto selecionados.
    /// </summary>
    /// <param name="profile">EN: Baseline profile selected by the user. PT: Perfil de baseline selecionado pelo usuario.</param>
    /// <param name="objectType">EN: Object type currently being configured. PT: Tipo de objeto configurado no momento.</param>
    /// <param name="reviewMetadata">EN: Optional review metadata loaded from `review-metadata.json`. PT: Metadados opcionais de revisao carregados de `review-metadata.json`.</param>
    /// <param name="todayUtc">EN: Optional UTC date used to evaluate review windows deterministically. PT: Data UTC opcional usada para avaliar janelas de revisao de forma deterministica.</param>
    public static string BuildMappingSummary(
        TemplateBaselineProfile profile,
        DatabaseObjectType objectType,
        TemplateReviewMetadata? reviewMetadata = null,
        DateTime? todayUtc = null)
    {
        ArgumentNullException.ThrowIfNull(profile);

        var mapping = profile.RecommendedMappings.TryGetValue(objectType, out var recommended)
            ? recommended
            : new ObjectTypeMapping(objectType, "Generated", "{NamePascal}{Type}Factory.cs");

        return $"{BuildProfileSummary(profile, reviewMetadata, todayUtc)} Recommended {objectType}: {mapping.OutputDirectory} | {mapping.FileNamePattern}.";
    }
}

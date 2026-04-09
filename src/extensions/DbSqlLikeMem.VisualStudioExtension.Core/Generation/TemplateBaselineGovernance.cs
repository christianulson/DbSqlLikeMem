using System.Globalization;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Validates whether the shared baseline catalog stays aligned with the versioned review metadata.
/// PT: Valida se o catalogo compartilhado de baseline permanece alinhado aos metadados versionados de revisao.
/// </summary>
public static class TemplateBaselineGovernance
{
    /// <summary>
    /// EN: Returns governance warnings for the informed profile when the review metadata disagrees with catalog expectations.
    /// PT: Retorna avisos de governanca para o perfil informado quando os metadados de revisao divergem das expectativas do catalogo.
    /// </summary>
    /// <param name="profile">EN: Baseline profile that should be validated. PT: Perfil de baseline que deve ser validado.</param>
    /// <param name="reviewMetadata">EN: Review metadata loaded from `review-metadata.json`. PT: Metadados de revisao carregados de `review-metadata.json`.</param>
    /// <param name="todayUtc">EN: Optional UTC date used to evaluate review windows deterministically. PT: Data UTC opcional usada para avaliar janelas de revisao de forma deterministica.</param>
    public static IReadOnlyCollection<string> ValidateProfileAlignment(
        TemplateBaselineProfile profile,
        TemplateReviewMetadata? reviewMetadata,
        DateTime? todayUtc = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(profile, nameof(profile));

        if (reviewMetadata is null)
        {
            return [];
        }

        var warnings = new List<string>();
        if (!string.Equals(reviewMetadata.CurrentBaseline, profile.Version, StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add($"Review metadata current baseline '{reviewMetadata.CurrentBaseline}' differs from catalog version '{profile.Version}'.");
        }

        if (!string.Equals(reviewMetadata.ReviewCadence, profile.ReviewCadence, StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add($"Review cadence '{reviewMetadata.ReviewCadence}' differs from catalog cadence '{profile.ReviewCadence}'.");
        }

        if (!string.Equals(reviewMetadata.NextPlannedReviewOn, profile.NextPlannedReviewOn, StringComparison.Ordinal))
        {
            warnings.Add($"Next planned review '{reviewMetadata.NextPlannedReviewOn}' differs from catalog date '{profile.NextPlannedReviewOn}'.");
        }

        if (reviewMetadata.ProfileFocusById.TryGetValue(profile.Id, out var focusFromMetadata)
            && !string.Equals(focusFromMetadata, profile.RecommendedTestFocus, StringComparison.Ordinal))
        {
            warnings.Add("Recommended focus differs between review metadata and catalog.");
        }

        var effectiveToday = (todayUtc ?? DateTime.UtcNow).Date;
        if (TryParseIsoDate(reviewMetadata.NextPlannedReviewOn, out var nextPlannedReviewOn)
            && nextPlannedReviewOn < effectiveToday)
        {
            warnings.Add($"Template baseline review is overdue since '{reviewMetadata.NextPlannedReviewOn}'.");
        }

        return warnings;
    }

    private static bool TryParseIsoDate(string value, out DateTime parsedDate)
        => DateTime.TryParseExact(
            value,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out parsedDate);
}

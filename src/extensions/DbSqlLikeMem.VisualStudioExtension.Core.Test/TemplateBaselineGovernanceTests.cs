namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies governance drift detection between the baseline catalog and the versioned review metadata.
/// PT: Verifica a deteccao de drift de governanca entre o catalogo de baseline e os metadados versionados de revisao.
/// </summary>
public sealed class TemplateBaselineGovernanceTests
{
    /// <summary>
    /// EN: Ensures the current repository metadata stays aligned with the API and Worker baseline profiles.
    /// PT: Garante que os metadados atuais do repositorio permaneçam alinhados aos perfis de baseline API e Worker.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselineGovernance")]
    public void ValidateProfileAlignment_WhenRepositoryMetadataMatchesCatalog_ReturnsNoWarnings()
    {
        var metadata = TemplateReviewMetadataReader.TryLoadFromRepositoryRoot(GetRepositoryRoot())!;

        foreach (var profile in TemplateBaselineCatalog.GetProfiles())
        {
            var warnings = TemplateBaselineGovernance.ValidateProfileAlignment(profile, metadata);
            Assert.Empty(warnings);
        }
    }

    /// <summary>
    /// EN: Ensures governance drift is reported when review metadata disagrees with the selected baseline profile.
    /// PT: Garante que drift de governanca seja reportado quando os metadados de revisao divergirem do perfil de baseline selecionado.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselineGovernance")]
    public void ValidateProfileAlignment_WhenMetadataDiffers_ReturnsWarnings()
    {
        var profile = TemplateBaselineCatalog.GetProfile("api")!;
        var driftedMetadata = new TemplateReviewMetadata(
            currentBaseline: "vNext",
            promotionStagingPath: "templates/dbsqllikemem/vNext",
            reviewCadence: "monthly",
            lastReviewedOn: "2026-03-08",
            nextPlannedReviewOn: "2026-05-01",
            profileFocusById: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["api"] = "Different focus"
            },
            evidenceFiles: ["CHANGELOG.md"]);

        var warnings = TemplateBaselineGovernance.ValidateProfileAlignment(profile, driftedMetadata);

        Assert.NotEmpty(warnings);
        Assert.Contains(warnings, warning => warning.Contains("current baseline", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(warnings, warning => warning.Contains("review cadence", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(warnings, warning => warning.Contains("recommended focus", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// EN: Ensures overdue review windows are reported even when the catalog and metadata otherwise match.
    /// PT: Garante que janelas de revisao vencidas sejam reportadas mesmo quando catalogo e metadata coincidem no restante.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselineGovernance")]
    public void ValidateProfileAlignment_WhenReviewWindowIsOverdue_ReturnsWarning()
    {
        var profile = TemplateBaselineCatalog.GetProfile("worker")!;
        var metadata = new TemplateReviewMetadata(
            currentBaseline: "vCurrent",
            promotionStagingPath: "templates/dbsqllikemem/vNext",
            reviewCadence: "quarterly",
            lastReviewedOn: "2025-12-31",
            nextPlannedReviewOn: "2026-01-15",
            profileFocusById: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["worker"] = profile.RecommendedTestFocus
            },
            evidenceFiles: ["CHANGELOG.md"]);

        var warnings = TemplateBaselineGovernance.ValidateProfileAlignment(
            profile,
            metadata,
            todayUtc: new DateTime(2026, 3, 8, 0, 0, 0, DateTimeKind.Utc));

        Assert.Contains(warnings, warning => warning.Contains("overdue", StringComparison.OrdinalIgnoreCase));
    }

    private static string GetRepositoryRoot()
        => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
}

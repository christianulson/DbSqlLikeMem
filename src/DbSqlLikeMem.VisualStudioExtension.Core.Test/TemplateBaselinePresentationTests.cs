namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies how template baseline metadata is exposed to UI flows that consume the shared catalog.
/// PT: Verifica como os metadados de baseline de templates sao expostos para fluxos de UI que consomem o catalogo compartilhado.
/// </summary>
public sealed class TemplateBaselinePresentationTests
{
    /// <summary>
    /// EN: Ensures the profile summary keeps the description, test focus, and next review window visible to the UI.
    /// PT: Garante que o resumo do perfil mantenha a descricao, o foco de testes e a proxima janela de revisao visiveis para a UI.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselinePresentation")]
    public void BuildProfileSummary_ShouldExposeDescriptionFocusAndReviewWindow()
    {
        var profile = TemplateBaselineCatalog.GetProfile("api")!;
        var reviewMetadata = TemplateReviewMetadataReader.TryLoadFromRepositoryRoot(GetRepositoryRoot());

        var summary = TemplateBaselinePresentation.BuildProfileSummary(profile, reviewMetadata);

        Assert.Contains("Read-oriented baseline", summary);
        Assert.Contains("Light integration tests", summary);
        Assert.Contains("2026-03-08", summary);
        Assert.Contains("2026-06-30", summary);
        Assert.Contains("src/Models", summary);
        Assert.Contains("src/Repositories", summary);
    }

    /// <summary>
    /// EN: Ensures the mapping summary exposes the recommended folder and file pattern for the selected object type.
    /// PT: Garante que o resumo de mapeamento exponha a pasta e o padrao de arquivo recomendados para o tipo de objeto selecionado.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselinePresentation")]
    public void BuildMappingSummary_ShouldExposeRecommendedMappingForObjectType()
    {
        var profile = TemplateBaselineCatalog.GetProfile("worker")!;
        var reviewMetadata = TemplateReviewMetadataReader.TryLoadFromRepositoryRoot(GetRepositoryRoot());

        var summary = TemplateBaselinePresentation.BuildMappingSummary(profile, DatabaseObjectType.Sequence, reviewMetadata);

        Assert.Contains("Worker/Batch", summary);
        Assert.Contains("tests/Consistency/Sequences", summary);
        Assert.Contains("{NamePascal}SequenceConsistencyTests.cs", summary);
    }

    /// <summary>
    /// EN: Ensures overdue review windows become visible in the profile summary presented to the UI.
    /// PT: Garante que janelas de revisao vencidas fiquem visiveis no resumo do perfil apresentado para a UI.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselinePresentation")]
    public void BuildProfileSummary_WhenReviewWindowIsOverdue_ShouldExposeGovernanceWarning()
    {
        var profile = TemplateBaselineCatalog.GetProfile("api")!;
        var reviewMetadata = new TemplateReviewMetadata(
            currentBaseline: "vCurrent",
            promotionStagingPath: "templates/dbsqllikemem/vNext",
            reviewCadence: "quarterly",
            lastReviewedOn: "2025-12-31",
            nextPlannedReviewOn: "2026-01-15",
            profileFocusById: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["api"] = profile.RecommendedTestFocus
            },
            evidenceFiles: ["CHANGELOG.md"]);

        var summary = TemplateBaselinePresentation.BuildProfileSummary(
            profile,
            reviewMetadata,
            todayUtc: new DateTime(2026, 3, 8, 0, 0, 0, DateTimeKind.Utc));

        Assert.Contains("Governance drift", summary);
        Assert.Contains("overdue", summary, StringComparison.OrdinalIgnoreCase);
    }

    private static string GetRepositoryRoot()
        => TemplateBaselineCatalog.FindRepositoryRoot(AppContext.BaseDirectory)
            ?? throw new InvalidOperationException("Repository root containing templates/dbsqllikemem could not be resolved from the test base directory.");
}

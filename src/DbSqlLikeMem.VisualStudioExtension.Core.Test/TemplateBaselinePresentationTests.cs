using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

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

    private static string GetRepositoryRoot()
        => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
}

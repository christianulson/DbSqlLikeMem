using DbSqlLikeMem.VisualStudioExtension.Core.Generation;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies the versioned template review metadata reader used by VSIX governance summaries.
/// PT: Verifica o leitor de metadados versionados de revisao de templates usado pelos resumos de governanca da VSIX.
/// </summary>
public sealed class TemplateReviewMetadataReaderTests
{
    /// <summary>
    /// EN: Ensures the reader loads the current repository review metadata and exposes its key governance fields.
    /// PT: Garante que o leitor carregue os metadados de revisao do repositorio atual e exponha seus principais campos de governanca.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateReviewMetadata")]
    public void TryLoadFromRepositoryRoot_ShouldExposeGovernanceFields()
    {
        var repositoryRoot = GetRepositoryRoot();

        var metadata = TemplateReviewMetadataReader.TryLoadFromRepositoryRoot(repositoryRoot);

        Assert.NotNull(metadata);
        Assert.Equal("vCurrent", metadata!.CurrentBaseline);
        Assert.Equal("quarterly", metadata.ReviewCadence);
        Assert.Equal("2026-03-08", metadata.LastReviewedOn);
        Assert.Equal("2026-06-30", metadata.NextPlannedReviewOn);
        Assert.Equal("Light integration tests for tables, views, and repositories.", metadata.ProfileFocusById["api"]);
        Assert.Contains("templates/dbsqllikemem/review-checklist.md", metadata.EvidenceFiles);
    }

    private static string GetRepositoryRoot()
        => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
}

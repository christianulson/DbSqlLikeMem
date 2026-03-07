using DbSqlLikeMem.VisualStudioExtension.Core.Generation;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies the versioned template baseline catalog used by generation tooling.
/// PT: Verifica o catalogo versionado de baseline de templates usado pelas ferramentas de geracao.
/// </summary>
public sealed class TemplateBaselineCatalogTests
{
    /// <summary>
    /// EN: Ensures the catalog exposes the API and Worker profiles from the current baseline.
    /// PT: Garante que o catalogo exponha os perfis API e Worker da baseline atual.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselineCatalog")]
    public void GetProfiles_ShouldExposeCurrentApiAndWorkerProfiles()
    {
        var profiles = TemplateBaselineCatalog.GetProfiles().OrderBy(x => x.Id).ToArray();

        Assert.Collection(
            profiles,
            api =>
            {
                Assert.Equal("api", api.Id);
                Assert.Equal("vCurrent", api.Version);
                Assert.Equal("src/Models", api.ModelOutputDirectory);
                Assert.Equal("src/Repositories", api.RepositoryOutputDirectory);
            },
            worker =>
            {
                Assert.Equal("worker", worker.Id);
                Assert.Equal("vCurrent", worker.Version);
                Assert.Equal("src/Batch/Models", worker.ModelOutputDirectory);
                Assert.Equal("src/Batch/Repositories", worker.RepositoryOutputDirectory);
            });
    }

    /// <summary>
    /// EN: Ensures template configuration resolves the current baseline file paths under the repository root.
    /// PT: Garante que a configuracao de templates resolva os caminhos da baseline atual sob a raiz do repositorio.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselineCatalog")]
    public void CreateTemplateConfiguration_ShouldResolveCurrentBaselinePaths()
    {
        var repositoryRoot = GetRepositoryRoot();

        var configuration = TemplateBaselineCatalog.CreateTemplateConfiguration(repositoryRoot, "api");

        Assert.Equal(
            Path.Combine(repositoryRoot, "templates", "dbsqllikemem", "vCurrent", "api", "model.template.txt"),
            configuration.ModelTemplatePath);
        Assert.Equal(
            Path.Combine(repositoryRoot, "templates", "dbsqllikemem", "vCurrent", "api", "repository.template.txt"),
            configuration.RepositoryTemplatePath);
        Assert.Equal("src/Models", configuration.ModelOutputDirectory);
        Assert.Equal("src/Repositories", configuration.RepositoryOutputDirectory);
    }

    /// <summary>
    /// EN: Ensures the repository ships the baseline files required by the catalog contract.
    /// PT: Garante que o repositorio entregue os arquivos de baseline exigidos pelo contrato do catalogo.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselineCatalog")]
    public void CurrentBaselineFiles_ShouldExistInRepository()
    {
        var repositoryRoot = GetRepositoryRoot();

        foreach (var profile in TemplateBaselineCatalog.GetProfiles())
        {
            Assert.True(File.Exists(Path.Combine(repositoryRoot, profile.ModelTemplateRelativePath.Replace('/', Path.DirectorySeparatorChar))));
            Assert.True(File.Exists(Path.Combine(repositoryRoot, profile.RepositoryTemplateRelativePath.Replace('/', Path.DirectorySeparatorChar))));
        }

        Assert.True(File.Exists(Path.Combine(repositoryRoot, "templates", "dbsqllikemem", "README.md")));
        Assert.True(File.Exists(Path.Combine(repositoryRoot, "templates", "dbsqllikemem", "vNext", "README.md")));
    }

    /// <summary>
    /// EN: Ensures the catalog can locate the nearest repository root that contains the versioned template baseline.
    /// PT: Garante que o catalogo consiga localizar a raiz mais proxima do repositorio que contem a baseline versionada de templates.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateBaselineCatalog")]
    public void FindRepositoryRoot_ShouldClimbUntilTemplateBaselineIsFound()
    {
        var repositoryRoot = GetRepositoryRoot();
        var nestedPath = Path.Combine(repositoryRoot, "src", "DbSqlLikeMem.VisualStudioExtension");

        var resolvedRoot = TemplateBaselineCatalog.FindRepositoryRoot(nestedPath);

        Assert.Equal(repositoryRoot, resolvedRoot);
    }

    private static string GetRepositoryRoot()
        => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
}

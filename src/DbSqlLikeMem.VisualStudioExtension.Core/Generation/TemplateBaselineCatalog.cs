using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Exposes the versioned template baseline profiles shipped with the repository.
/// PT: Expoe os perfis versionados de baseline de templates entregues com o repositorio.
/// </summary>
public static class TemplateBaselineCatalog
{
    private static readonly IReadOnlyCollection<TemplateBaselineProfile> Profiles =
    [
        new TemplateBaselineProfile(
            "api",
            "API",
            "vCurrent",
            "Read-oriented baseline for solutions centered on models and repositories.",
            "Light integration tests for tables, views, and repositories.",
            "quarterly",
            "2026-06-30",
            "templates/dbsqllikemem/vCurrent/api/model.template.txt",
            "templates/dbsqllikemem/vCurrent/api/repository.template.txt",
            "src/Models",
            "src/Repositories"),
        new TemplateBaselineProfile(
            "worker",
            "Worker/Batch",
            "vCurrent",
            "Execution-oriented baseline for worker and batch solutions.",
            "Consistency-oriented tests for batch flows and DML validation.",
            "quarterly",
            "2026-06-30",
            "templates/dbsqllikemem/vCurrent/worker/model.template.txt",
            "templates/dbsqllikemem/vCurrent/worker/repository.template.txt",
            "src/Batch/Models",
            "src/Batch/Repositories"),
    ];

    /// <summary>
    /// EN: Gets the versioned baseline profiles known by the generation tooling.
    /// PT: Obtem os perfis versionados de baseline conhecidos pelas ferramentas de geracao.
    /// </summary>
    public static IReadOnlyCollection<TemplateBaselineProfile> GetProfiles() => Profiles;

    /// <summary>
    /// EN: Returns the profile that matches the informed identifier.
    /// PT: Retorna o perfil que corresponde ao identificador informado.
    /// </summary>
    /// <param name="profileId">EN: Profile identifier like `api` or `worker`. PT: Identificador do perfil como `api` ou `worker`.</param>
    public static TemplateBaselineProfile? GetProfile(string profileId)
        => Profiles.FirstOrDefault(profile => string.Equals(profile.Id, profileId, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// EN: Creates a template configuration pointing to the current baseline files under a repository root.
    /// PT: Cria uma configuracao de templates apontando para os arquivos da baseline atual sob uma raiz de repositorio.
    /// </summary>
    /// <param name="repositoryRoot">EN: Absolute repository root path. PT: Caminho absoluto da raiz do repositorio.</param>
    /// <param name="profileId">EN: Baseline profile identifier. PT: Identificador do perfil de baseline.</param>
    public static TemplateConfiguration CreateTemplateConfiguration(string repositoryRoot, string profileId)
    {
        if (string.IsNullOrWhiteSpace(repositoryRoot))
        {
            throw new ArgumentException("Repository root is required.", nameof(repositoryRoot));
        }

        var profile = GetProfile(profileId)
            ?? throw new ArgumentOutOfRangeException(nameof(profileId), profileId, "Unsupported template baseline profile.");

        return new TemplateConfiguration(
            ResolveRepositoryRelativePath(repositoryRoot, profile.ModelTemplateRelativePath),
            ResolveRepositoryRelativePath(repositoryRoot, profile.RepositoryTemplateRelativePath),
            profile.ModelOutputDirectory,
            profile.RepositoryOutputDirectory);
    }

    /// <summary>
    /// EN: Locates the nearest parent directory that contains the versioned template baseline catalog.
    /// PT: Localiza o diretorio pai mais proximo que contem o catalogo versionado de baseline de templates.
    /// </summary>
    /// <param name="startPath">EN: File system path used as the search starting point. PT: Caminho de sistema de arquivos usado como ponto inicial da busca.</param>
    public static string? FindRepositoryRoot(string startPath)
    {
        if (string.IsNullOrWhiteSpace(startPath))
        {
            return null;
        }

        var fullPath = Path.GetFullPath(startPath);
        var current = Directory.Exists(fullPath)
            ? new DirectoryInfo(fullPath)
            : new FileInfo(fullPath).Directory;

        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "templates", "dbsqllikemem", "README.md")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return null;
    }

    private static string ResolveRepositoryRelativePath(string repositoryRoot, string relativePath)
        => Path.GetFullPath(Path.Combine(repositoryRoot, relativePath.Replace('/', Path.DirectorySeparatorChar)));
}

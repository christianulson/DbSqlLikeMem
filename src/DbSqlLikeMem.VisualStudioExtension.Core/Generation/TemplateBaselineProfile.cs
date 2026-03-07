namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Describes a versioned template baseline profile exposed by the generation tooling.
/// PT: Descreve um perfil versionado de baseline de templates exposto pelas ferramentas de geracao.
/// </summary>
public sealed record TemplateBaselineProfile
{
    /// <summary>
    /// EN: Creates a template baseline profile entry.
    /// PT: Cria uma entrada de perfil de baseline de templates.
    /// </summary>
    /// <param name="id">EN: Stable profile identifier. PT: Identificador estavel do perfil.</param>
    /// <param name="displayName">EN: Human-readable profile name. PT: Nome legivel do perfil.</param>
    /// <param name="version">EN: Baseline version label. PT: Rotulo de versao da baseline.</param>
    /// <param name="description">EN: Short profile purpose description. PT: Descricao curta do objetivo do perfil.</param>
    /// <param name="modelTemplateRelativePath">EN: Repository-relative model template path. PT: Caminho relativo do template de modelo no repositorio.</param>
    /// <param name="repositoryTemplateRelativePath">EN: Repository-relative repository template path. PT: Caminho relativo do template de repositorio no repositorio.</param>
    /// <param name="modelOutputDirectory">EN: Default output directory for model generation. PT: Diretorio padrao de saida para geracao de modelos.</param>
    /// <param name="repositoryOutputDirectory">EN: Default output directory for repository generation. PT: Diretorio padrao de saida para geracao de repositorios.</param>
    public TemplateBaselineProfile(
        string id,
        string displayName,
        string version,
        string description,
        string modelTemplateRelativePath,
        string repositoryTemplateRelativePath,
        string modelOutputDirectory,
        string repositoryOutputDirectory)
    {
        Id = id;
        DisplayName = displayName;
        Version = version;
        Description = description;
        ModelTemplateRelativePath = modelTemplateRelativePath;
        RepositoryTemplateRelativePath = repositoryTemplateRelativePath;
        ModelOutputDirectory = modelOutputDirectory;
        RepositoryOutputDirectory = repositoryOutputDirectory;
    }

    /// <summary>
    /// EN: Gets the stable profile identifier.
    /// PT: Obtem o identificador estavel do perfil.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// EN: Gets the profile name shown to users and docs.
    /// PT: Obtem o nome do perfil mostrado a usuarios e documentacao.
    /// </summary>
    public string DisplayName { get; }

    /// <summary>
    /// EN: Gets the baseline version label.
    /// PT: Obtem o rotulo de versao da baseline.
    /// </summary>
    public string Version { get; }

    /// <summary>
    /// EN: Gets the short description for the profile intent.
    /// PT: Obtem a descricao curta da intencao do perfil.
    /// </summary>
    public string Description { get; }

    /// <summary>
    /// EN: Gets the repository-relative path of the model template.
    /// PT: Obtem o caminho relativo no repositorio do template de modelo.
    /// </summary>
    public string ModelTemplateRelativePath { get; }

    /// <summary>
    /// EN: Gets the repository-relative path of the repository template.
    /// PT: Obtem o caminho relativo no repositorio do template de repositorio.
    /// </summary>
    public string RepositoryTemplateRelativePath { get; }

    /// <summary>
    /// EN: Gets the default output directory for model files.
    /// PT: Obtem o diretorio padrao de saida para arquivos de modelo.
    /// </summary>
    public string ModelOutputDirectory { get; }

    /// <summary>
    /// EN: Gets the default output directory for repository files.
    /// PT: Obtem o diretorio padrao de saida para arquivos de repositorio.
    /// </summary>
    public string RepositoryOutputDirectory { get; }
}

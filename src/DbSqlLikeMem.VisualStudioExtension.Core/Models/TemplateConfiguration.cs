namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents template and output path settings used by the extension code generator.
/// Representa as configurações de templates e diretórios de saída usadas pelo gerador da extensão.
/// </summary>
public sealed record TemplateConfiguration
{
    /// <summary>
    /// Gets the model template file path.
    /// Obtém o caminho do arquivo de template de modelo.
    /// </summary>
    public string ModelTemplatePath { get; init; }

    /// <summary>
    /// Gets the repository template file path.
    /// Obtém o caminho do arquivo de template de repositório.
    /// </summary>
    public string RepositoryTemplatePath { get; init; }

    /// <summary>
    /// Gets the model output directory.
    /// Obtém o diretório de saída dos modelos.
    /// </summary>
    public string ModelOutputDirectory { get; init; }

    /// <summary>
    /// Gets the repository output directory.
    /// Obtém o diretório de saída dos repositórios.
    /// </summary>
    public string RepositoryOutputDirectory { get; init; }

    public TemplateConfiguration(
        string modelTemplatePath,
        string repositoryTemplatePath,
        string modelOutputDirectory,
        string repositoryOutputDirectory)
    {
        ModelTemplatePath = modelTemplatePath;
        RepositoryTemplatePath = repositoryTemplatePath;
        ModelOutputDirectory = modelOutputDirectory;
        RepositoryOutputDirectory = repositoryOutputDirectory;
    }

    /// <summary>
    /// Gets the default template configuration.
    /// Obtém a configuração padrão de templates.
    /// </summary>
    public static TemplateConfiguration Default { get; } = new(
        string.Empty,
        string.Empty,
        "Generated/Models",
        "Generated/Repositories");
}

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
    /// Gets the model file name pattern.
    /// Obtém o padrão de nome de arquivo do modelo.
    /// </summary>
    public string ModelFileNamePattern { get; init; }

    /// <summary>
    /// Gets the repository output directory.
    /// Obtém o diretório de saída dos repositórios.
    /// </summary>
    public string RepositoryOutputDirectory { get; init; }

    /// <summary>
    /// Gets the repository file name pattern.
    /// Obtém o padrão de nome de arquivo do repositório.
    /// </summary>
    public string RepositoryFileNamePattern { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="TemplateConfiguration"/> record.
    /// Inicializa uma nova instância do registro <see cref="TemplateConfiguration"/>.
    /// </summary>
    /// <param name="modelTemplatePath">Model template file path. Caminho do template de modelo.</param>
    /// <param name="repositoryTemplatePath">Repository template file path. Caminho do template de repositório.</param>
    /// <param name="modelOutputDirectory">Model output directory. Diretório de saída dos modelos.</param>
    /// <param name="repositoryOutputDirectory">Repository output directory. Diretório de saída dos repositórios.</param>
    /// <param name="modelFileNamePattern">Model file name pattern. Padrão de nome de arquivo do modelo.</param>
    /// <param name="repositoryFileNamePattern">Repository file name pattern. Padrão de nome de arquivo do repositório.</param>
    public TemplateConfiguration(
        string modelTemplatePath,
        string repositoryTemplatePath,
        string modelOutputDirectory,
        string repositoryOutputDirectory,
        string modelFileNamePattern = "{NamePascal}Model.cs",
        string repositoryFileNamePattern = "{NamePascal}Repository.cs")
    {
        ModelTemplatePath = modelTemplatePath;
        RepositoryTemplatePath = repositoryTemplatePath;
        ModelOutputDirectory = modelOutputDirectory;
        ModelFileNamePattern = modelFileNamePattern;
        RepositoryOutputDirectory = repositoryOutputDirectory;
        RepositoryFileNamePattern = repositoryFileNamePattern;
    }

    /// <summary>
    /// Gets the default template configuration.
    /// Obtém a configuração padrão de templates.
    /// </summary>
    public static TemplateConfiguration Default { get; } = new(
        string.Empty,
        string.Empty,
        "Generated/Models",
        "Generated/Repositories",
        "{NamePascal}Model.cs",
        "{NamePascal}Repository.cs");
}

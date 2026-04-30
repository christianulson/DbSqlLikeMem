namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents code generation mapping for a specific object type.
/// Representa o mapeamento de geração de código para um tipo de objeto específico.
/// </summary>
public sealed record ObjectTypeMapping
{
    /// <summary>
    /// Initializes a new object type mapping.
    /// Inicializa um novo mapeamento de tipo de objeto.
    /// </summary>
    /// <param name="objectType">Database object type. Tipo do objeto de banco.</param>
    /// <param name="outputDirectory">Output directory for generated files. Diretório de saída para arquivos gerados.</param>
    /// <param name="fileNamePattern">File name pattern used during generation. Padrão de nome de arquivo usado durante a geração.</param>
    /// <param name="namespace">Optional namespace injected into generated templates/content. Namespace opcional injetado nos templates/conteúdo gerados.</param>
    public ObjectTypeMapping(
        DatabaseObjectType objectType,
        string outputDirectory,
        string fileNamePattern = "{NamePascal}{Type}Factory.cs",
        string? @namespace = null)
    {
        ObjectType = objectType;
        OutputDirectory = outputDirectory;
        FileNamePattern = fileNamePattern;
        Namespace = @namespace;
    }

    /// <summary>
    /// Gets the database object type.
    /// Obtém o tipo de objeto de banco.
    /// </summary>
    public DatabaseObjectType ObjectType { get; }

    /// <summary>
    /// Gets the output directory for generated files.
    /// Obtém o diretório de saída dos arquivos gerados.
    /// </summary>
    public string OutputDirectory { get; }

    /// <summary>
    /// Gets the file name pattern.
    /// Obtém o padrão de nome de arquivo.
    /// </summary>
    public string FileNamePattern { get; }

    /// <summary>
    /// Gets the optional namespace associated with this mapping.
    /// Obtém o namespace opcional associado a este mapeamento.
    /// </summary>
    public string? Namespace { get; }
}

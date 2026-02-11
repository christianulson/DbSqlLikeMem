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
    public ObjectTypeMapping(DatabaseObjectType objectType, string outputDirectory, string fileNamePattern = "{NamePascal}{Type}Factory.cs")
    {
        ObjectType = objectType;
        OutputDirectory = outputDirectory;
        FileNamePattern = fileNamePattern;
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
}

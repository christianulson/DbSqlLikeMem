using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Validation;

/// <summary>
/// Represents a local snapshot of a generated object.
/// Representa um snapshot local de um objeto gerado.
/// </summary>
public sealed record LocalObjectSnapshot
{
    /// <summary>
    /// Initializes a new local object snapshot.
    /// Inicializa um novo snapshot local de objeto.
    /// </summary>
    public LocalObjectSnapshot(DatabaseObjectReference reference, string filePath, IReadOnlyDictionary<string, string>? properties = null)
    {
        Reference = reference;
        FilePath = filePath;
        Properties = properties;
    }

    /// <summary>
    /// Gets the object reference.
    /// Obtém a referência do objeto.
    /// </summary>
    public DatabaseObjectReference Reference { get; }

    /// <summary>
    /// Gets the source file path.
    /// Obtém o caminho do arquivo de origem.
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Gets optional extracted properties.
    /// Obtém propriedades extraídas opcionais.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Properties { get; }
}

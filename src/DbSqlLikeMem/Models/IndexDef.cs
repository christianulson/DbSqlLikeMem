namespace DbSqlLikeMem;

/// <summary>
/// EN: Represents an index definition, including key columns and uniqueness.
/// PT: Representa a definição de um índice, incluindo colunas-chave e unicidade.
/// </summary>
public class IndexDef
{
    /// <summary>
    /// EN: Gets the index name.
    /// PT: Obtém o nome do índice.
    /// </summary>
    public string Name { get; }
    /// <summary>
    /// EN: Gets the columns that compose the index key.
    /// PT: Obtém as colunas que compõem a chave do índice.
    /// </summary>
    public IReadOnlyList<string> KeyCols { get; }
    /// <summary>
    /// EN: Indicates whether the index is unique.
    /// PT: Indica se o índice é único.
    /// </summary>
    public bool Unique { get; }

    /// <summary>
    /// EN: Gets columns included in the index that are not part of the key.
    /// PT: Obtém colunas incluídas no índice, mas não fazem parte da chave.
    /// </summary>
    public IReadOnlyList<string> Include { get; }

    /// <summary>
    /// EN: Initializes the index definition.
    /// PT: Inicializa a definição do índice.
    /// </summary>
    /// <param name="name">EN: Index name. PT: Nome do índice.</param>
    /// <param name="keyCols">EN: Index key columns. PT: Colunas chave do índice.</param>
    /// <param name="include">EN: Additional included columns. PT: Colunas incluídas adicionais.</param>
    /// <param name="unique">EN: Whether the index is unique. PT: Indica se o índice é único.</param>
    public IndexDef(
        string name,
        IEnumerable<string> keyCols,
        string[]? include = null,
        bool unique = false)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        KeyCols = [.. keyCols ?? throw new ArgumentNullException(nameof(keyCols))];
        if (KeyCols.Count == 0) throw new ArgumentException("At least one key column is required", nameof(keyCols));
        Unique = unique;
        Include = include ?? [];
    }
}

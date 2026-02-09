namespace DbSqlLikeMem;

/// <summary>
/// Representa a definição de um índice, incluindo colunas-chave e unicidade.
/// </summary>
public class IndexDef
{
    /// <summary>
    /// Obtém o nome do índice.
    /// </summary>
    public string Name { get; }
    /// <summary>
    /// Obtém as colunas que compõem a chave do índice.
    /// </summary>
    public IReadOnlyList<string> KeyCols { get; }
    /// <summary>
    /// Indica se o índice é único.
    /// </summary>
    public bool Unique { get; }

    /// <summary>
    /// Obtém colunas incluídas no índice, mas não fazem parte da chave.
    /// </summary>
    public IReadOnlyList<string> Include { get; }

    /// <summary>
    /// Inicializa a definição do índice.
    /// </summary>
    /// <param name="name">Nome do índice.</param>
    /// <param name="keyCols">Colunas chave do índice.</param>
    /// <param name="include">Colunas incluídas adicionais.</param>
    /// <param name="unique">Indica se o índice é único.</param>
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

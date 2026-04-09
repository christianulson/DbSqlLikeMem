namespace DbSqlLikeMem;

/// <summary>
/// EN: Implements an index dictionary with case-insensitive comparison.
/// PT: Implementa um dicionário de índices com comparação case-insensitive.
/// </summary>
public class IndexDictionary : Dictionary<string, IndexDef>
{
    /// <summary>
    /// EN: Initializes the index dictionary ignoring case.
    /// PT: Inicializa o dicionário de índices ignorando maiúsculas/minúsculas.
    /// </summary>
    public IndexDictionary() : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    /// <summary>
    /// EN: Returns only _indexes marked as unique.
    /// PT: Retorna apenas os índices marcados como únicos.
    /// </summary>
    /// <returns>EN: Unique index set. PT: Conjunto de índices únicos.</returns>
    public IEnumerable<IndexDef> GetUnique()
        => Values.Where(i => i.Unique);
}

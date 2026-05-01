namespace DbSqlLikeMem;

/// <summary>
/// EN: Implements an index dictionary with case-insensitive comparison.
/// PT-br: Implementa um dicionário de índices com comparação case-insensitive.
/// </summary>
public class IndexDictionary : Dictionary<string, IndexDef>
{
    /// <summary>
    /// EN: Initializes the index dictionary ignoring case.
    /// PT-br: Inicializa o dicionário de índices ignorando maiúsculas/minúsculas.
    /// </summary>
    public IndexDictionary() : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    /// <summary>
    /// EN: Returns only _indexes marked as unique.
    /// PT-br: Retorna apenas os índices marcados como únicos.
    /// </summary>
    /// <returns>EN: Unique index set. PT-br: Conjunto de índices únicos.</returns>
    public IEnumerable<IndexDef> GetUnique()
    {
        foreach (var index in Values)
        {
            if (index.Unique)
                yield return index;
        }
    }
}

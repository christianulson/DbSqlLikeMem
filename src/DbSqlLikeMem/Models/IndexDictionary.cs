namespace DbSqlLikeMem;
/// <summary>
/// Implementa um dicionário de índices com comparação case-insensitive.
/// </summary>
public class IndexDictionary : Dictionary<string, IndexDef>
{
    /// <summary>
    /// Inicializa o dicionário de índices ignorando maiúsculas/minúsculas.
    /// </summary>
    public IndexDictionary() : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    /// <summary>
    /// Retorna apenas os índices marcados como únicos.
    /// </summary>
    /// <returns>Conjunto de índices únicos.</returns>
    public IEnumerable<IndexDef> GetUnique()
        => Values.Where(i => i.Unique);
}

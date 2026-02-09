namespace DbSqlLikeMem;

/// <summary>
/// Implementa um dicionário de schemas com comparação case-insensitive.
/// </summary>
public class SchemaDictionary 
    : Dictionary<string, ISchemaMock>
{
    /// <summary>
    /// Cria um dicionário de schemas vazio.
    /// </summary>
    public SchemaDictionary()
        : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    /// <summary>
    /// Cria um dicionário de schemas a partir de outro conjunto.
    /// </summary>
    /// <param name="schemas">Schemas iniciais.</param>
    public SchemaDictionary(
        IDictionary<string, ISchemaMock>? schemas)
    : base(StringComparer.OrdinalIgnoreCase)
    {
        ArgumentNullException.ThrowIfNull(schemas);
        foreach (var (k, v) in schemas)
            Add(k, v);
    }
}

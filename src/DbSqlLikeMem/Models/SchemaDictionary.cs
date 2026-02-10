namespace DbSqlLikeMem;

/// <summary>
/// EN: Implements a schema dictionary with case-insensitive comparison.
/// PT: Implementa um dicionário de schemas com comparação case-insensitive.
/// </summary>
public class SchemaDictionary 
    : Dictionary<string, ISchemaMock>
{
    /// <summary>
    /// EN: Creates an empty schema dictionary.
    /// PT: Cria um dicionário de schemas vazio.
    /// </summary>
    public SchemaDictionary()
        : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    /// <summary>
    /// EN: Creates a schema dictionary from another set.
    /// PT: Cria um dicionário de schemas a partir de outro conjunto.
    /// </summary>
    /// <param name="schemas">EN: Initial schemas. PT: Schemas iniciais.</param>
    public SchemaDictionary(
        IDictionary<string, ISchemaMock>? schemas)
    : base(StringComparer.OrdinalIgnoreCase)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(schemas, nameof(schemas));
        foreach (var it in schemas!)
            Add(it.Key, it.Value);
    }
}

namespace DbSqlLikeMem;

public class SchemaDictionary 
    : Dictionary<string, ISchemaMock>
{
    public SchemaDictionary()
        : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    public SchemaDictionary(
        IDictionary<string, ISchemaMock>? schemas)
    : base(StringComparer.OrdinalIgnoreCase)
    {
        ArgumentNullException.ThrowIfNull(schemas);
        foreach (var (k, v) in schemas)
            Add(k, v);
    }
}

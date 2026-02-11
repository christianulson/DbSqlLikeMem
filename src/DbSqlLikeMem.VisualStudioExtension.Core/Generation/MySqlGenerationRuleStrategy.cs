namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

internal sealed class MySqlGenerationRuleStrategy : IGenerationRuleStrategy
{
    private static readonly DefaultGenerationRuleStrategy Fallback = new();

    public string MapDbType(GenerationTypeContext context)
    {
        var normalizedType = context.DataType.ToLowerInvariant();

        return normalizedType switch
        {
            "tinyint" => (context.NumPrecision == 1 || context.CharMaxLen == 1) ? "Boolean" : "Byte",
            "bit" => (context.NumPrecision == 1) ? "Boolean" : "UInt64",
            _ => Fallback.MapDbType(context)
        };
    }
}

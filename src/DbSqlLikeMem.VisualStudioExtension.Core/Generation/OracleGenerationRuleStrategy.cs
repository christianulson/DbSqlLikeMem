namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

internal sealed class OracleGenerationRuleStrategy : IGenerationRuleStrategy
{
    private static readonly DefaultGenerationRuleStrategy Fallback = new();

    public string MapDbType(GenerationTypeContext context)
    {
        var normalizedType = context.DataType.ToLowerInvariant();
        var arr = normalizedType.Split('(');
        normalizedType = arr[0];

        return normalizedType switch
        {
            "tinyint" => (context.NumPrecision == 1 || context.CharMaxLen == 1)
                ? "Boolean"
                : "Byte",
            "bit" => (context.NumPrecision == 1)
                ? "Boolean"
                : "UInt64",
            "timestamp" => "Time",
            "raw" => "Binary",
            "number" => context.NumPrecision == 1
                    ? "Byte"
                    : context.NumPrecision <= 4
                        ? "Int16"
                        : context.NumPrecision <= 8
                            ? "Int32"
                            : "Int64",
            _ => Fallback.MapDbType(context)
        };
    }
}

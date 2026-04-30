namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

internal sealed class FirebirdGenerationRuleStrategy : IGenerationRuleStrategy
{
    private static readonly DefaultGenerationRuleStrategy Fallback = new();

    public string MapDbType(GenerationTypeContext context)
    {
        var normalizedType = context.DataType.Trim().ToLowerInvariant();

        if (int.TryParse(normalizedType, out var typeCode))
        {
            return MapFromTypeCode(typeCode, context);
        }

        var openParenIndex = normalizedType.IndexOf('(');
        if (openParenIndex >= 0)
        {
            normalizedType = normalizedType.Substring(0, openParenIndex);
        }

        return normalizedType switch
        {
            "smallint" => context.NumPrecision is > 0 ? "Decimal" : "Int16",
            "integer" => context.NumPrecision is > 0 ? "Decimal" : "Int32",
            "float" => "Single",
            "date" => "Date",
            "time" => "Time",
            "char" or "varchar" or "cstring" => "String",
            "bigint" => context.NumPrecision is > 0 ? "Decimal" : "Int64",
            "boolean" => "Boolean",
            "decfloat" or "decimal" or "numeric" => "Decimal",
            "double precision" => "Double",
            "time with time zone" => "Time",
            "timestamp with time zone" or "timestamp" => "DateTime",
            "blob" => "Binary",
            _ => Fallback.MapDbType(context)
        };
    }

    private static string MapFromTypeCode(int typeCode, GenerationTypeContext context)
        => typeCode switch
        {
            7 => context.NumPrecision is > 0 ? "Decimal" : "Int16",
            8 => context.NumPrecision is > 0 ? "Decimal" : "Int32",
            10 => "Single",
            12 => "Date",
            13 => "Time",
            14 => "String",
            16 => context.NumPrecision is > 0 ? "Decimal" : "Int64",
            23 => "Boolean",
            24 or 25 or 26 => "Decimal",
            27 => "Double",
            28 => "Time",
            29 or 35 => "DateTime",
            37 => "String",
            261 => "Binary",
            _ => Fallback.MapDbType(context)
        };
}

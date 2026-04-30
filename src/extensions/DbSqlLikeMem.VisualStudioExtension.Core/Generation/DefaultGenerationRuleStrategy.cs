namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

internal sealed class DefaultGenerationRuleStrategy : IGenerationRuleStrategy
{
    public string MapDbType(GenerationTypeContext context)
    {
        var normalizedType = context.DataType.ToLowerInvariant();

        var looksGuid =
            (normalizedType is "binary" or "varbinary") && context.CharMaxLen == 16
            || (normalizedType is "char" && context.CharMaxLen == 36
                && (context.ColumnName.EndsWith("guid", StringComparison.OrdinalIgnoreCase)
                    || context.ColumnName.EndsWith("uuid", StringComparison.OrdinalIgnoreCase)));

        if (looksGuid)
        {
            return "Guid";
        }

        return normalizedType switch
        {
            "tinyint" => "Byte",
            "smallint" => "Int16",
            "mediumint" => "Int32",
            "int"
            or "integer"
                => "Int32",
            "bigint" => "Int64",

            "bit" => "Boolean",

            "decimal"
            or "numeric"
                => "Decimal",

            "double"
                => "Double",

            "float"
            or "real"
                => "Single",

            "date" => "Date",
            "datetime"
            or "timestamp"
                => "DateTime",
            "time" => "Time",

            "year" => "Int32",

            "char"
            or "nchar"
            or "varchar"
            or "varchar2"
            or "nvarchar"
            or "text"
            or "tinytext"
            or "mediumtext"
            or "longtext"
            or "json"
            or "enum"
            or "set"
            or "clob"
                => "String",

            "binary"
            or "varbinary"
            or "blob"
            or "tinyblob"
            or "mediumblob"
            or "longblob"
            or "bytea"
                => "Binary",

            "uniqueidentifier"
            or "uuid"
                => "Guid",

            "bool"
            or "boolean"
                => "Boolean",

            _ => "Object"
        };
    }
}

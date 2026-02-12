namespace DbSqlLikeMem;

internal static class SqlUnsupported
{
    private static string FormatDialectLabel(ISqlDialect dialect)
        => string.Equals(dialect.Name, "postgresql", StringComparison.OrdinalIgnoreCase)
            ? $"{dialect.Name}/npgsql"
            : dialect.Name;

    public static NotSupportedException ForDialect(ISqlDialect dialect, string feature)
        => new($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): {feature}.");

    public static NotSupportedException ForParser(string feature)
        => new($"SQL não suportado no parser: {feature}.");

    public static NotSupportedException ForCommandType(ISqlDialect dialect, string operation, Type queryType)
        => new($"SQL não suportado em {operation} para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): {queryType.Name}.");
}

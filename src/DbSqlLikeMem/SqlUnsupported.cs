namespace DbSqlLikeMem;

internal static class SqlUnsupported
{
    public static NotSupportedException ForDialect(ISqlDialect dialect, string feature)
        => new($"SQL não suportado para dialeto '{dialect.Name}' (v{dialect.Version}): {feature}.");

    public static NotSupportedException ForParser(string feature)
        => new($"SQL não suportado no parser: {feature}.");

    public static NotSupportedException ForCommandType(ISqlDialect dialect, string operation, Type queryType)
        => new($"SQL não suportado em {operation} para dialeto '{dialect.Name}' (v{dialect.Version}): {queryType.Name}.");
}

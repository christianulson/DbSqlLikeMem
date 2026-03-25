namespace DbSqlLikeMem;

internal static class PostgresFamilyDialectHelper
{
    internal static bool IsPostgresFamilyDialect(ISqlDialect dialect)
        => dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase);
}

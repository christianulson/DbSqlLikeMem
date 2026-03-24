namespace DbSqlLikeMem;

internal static class MySqlFamilyDialectHelper
{
    internal static bool IsMySqlFamilyDialect(ISqlDialect dialect)
        => dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase)
        || dialect.Name.Equals("mariadb", StringComparison.OrdinalIgnoreCase);
}

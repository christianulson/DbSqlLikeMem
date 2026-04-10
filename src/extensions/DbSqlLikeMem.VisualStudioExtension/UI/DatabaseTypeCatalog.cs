namespace DbSqlLikeMem.VisualStudioExtension.UI;

internal static class DatabaseTypeCatalog
{
    internal const string DefaultDatabaseType = "SqlServer";

    internal static IReadOnlyList<string> SupportedDatabaseTypes { get; } = [
        "SqlServer",
        "SqlAzure",
        "AzureSql",
        "PostgreSql",
        "MySql",
        "MariaDb",
        "Oracle",
        "Sqlite",
        "Db2",
        "Firebird"
    ];
}

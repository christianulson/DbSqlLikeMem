namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// EN: Normalizes database type labels and lookup keys across the extensions.
/// PT-br: Normaliza rótulos e chaves de tipo de banco entre as extensions.
/// </summary>
public static class DatabaseTypeNormalizer
{
    private static readonly IReadOnlyDictionary<string, string> CanonicalNames =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sqlserver"] = "SqlServer",
            ["mssql"] = "SqlServer",
            ["sqlazure"] = "SqlAzure",
            ["azuresql"] = "AzureSql",
            ["postgresql"] = "PostgreSql",
            ["postgres"] = "PostgreSql",
            ["pg"] = "PostgreSql",
            ["pgsql"] = "PostgreSql",
            ["mysql"] = "MySql",
            ["mariadb"] = "MariaDb",
            ["sqlite"] = "Sqlite",
            ["sqlite3"] = "Sqlite",
            ["db2"] = "Db2",
            ["db2luw"] = "Db2",
            ["oracle"] = "Oracle",
            ["firebird"] = "Firebird",
            ["firebirdsql"] = "Firebird"
        };

    private static readonly IReadOnlyList<string> SupportedDisplayNames =
    [
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

    /// <summary>
    /// EN: Returns the normalized lookup key for a database type.
    /// PT-br: Retorna a chave normalizada de consulta para um tipo de banco.
    /// </summary>
    public static string NormalizeKey(string? databaseType)
    {
        var trimmed = (databaseType ?? string.Empty).Trim();
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        return trimmed
            .Replace(" ", string.Empty)
            .Replace("_", string.Empty)
            .Replace("-", string.Empty)
            .Replace("/", string.Empty)
            .ToLowerInvariant();
    }

    /// <summary>
    /// EN: Returns the canonical database type label used by the core.
    /// PT-br: Retorna o rótulo canônico de tipo de banco usado pelo core.
    /// </summary>
    public static string NormalizeDisplayName(string? databaseType)
    {
        var trimmed = (databaseType ?? string.Empty).Trim();
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        var key = NormalizeKey(trimmed);
        return CanonicalNames.TryGetValue(key, out var canonicalName)
            ? canonicalName
            : trimmed;
    }

    /// <summary>
    /// EN: Returns the supported database type labels used by the UI catalogs.
    /// PT-br: Retorna os rótulos de tipo de banco suportados usados pelos catalogos da interface.
    /// </summary>
    public static IReadOnlyList<string> GetSupportedDisplayNames() => SupportedDisplayNames;
}

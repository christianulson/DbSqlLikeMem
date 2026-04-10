namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents a saved database connection definition.
/// Representa uma definição de conexão de banco de dados salva.
/// </summary>
public sealed record ConnectionDefinition
{
    /// <summary>
    /// Initializes a new connection definition.
    /// Inicializa uma nova definição de conexão.
    /// </summary>
    public ConnectionDefinition(string id, string databaseType, string databaseName, string connectionString, string? displayName = null)
    {
        Id = id;
        DatabaseType = NormalizeDatabaseType(databaseType);
        DatabaseName = databaseName;
        ConnectionString = connectionString;
        DisplayName = displayName;
    }

    /// <summary>
    /// Gets the unique connection identifier.
    /// Obtém o identificador único da conexão.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Gets the database engine type.
    /// Obtém o tipo do mecanismo de banco.
    /// </summary>
    public string DatabaseType { get; }

    /// <summary>
    /// Gets the logical database name.
    /// Obtém o nome lógico do banco de dados.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// Gets the provider connection string.
    /// Obtém a connection string do provedor.
    /// </summary>
    public string ConnectionString { get; }

    /// <summary>
    /// Gets the optional display name.
    /// Obtém o nome de exibição opcional.
    /// </summary>
    public string? DisplayName { get; }

    /// <summary>
    /// Gets a friendly name for UI display.
    /// Obtém um nome amigável para exibição na interface.
    /// </summary>
    public string FriendlyName => DisplayName ?? DatabaseName;

    private static string NormalizeDatabaseType(string databaseType)
    {
        var trimmed = (databaseType ?? string.Empty).Trim();
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        var normalized = trimmed.Replace(" ", string.Empty).Replace("_", string.Empty).Replace("-", string.Empty);

        if (trimmed.Equals("SqlServer", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
        {
            return "SqlServer";
        }

        if (trimmed.Equals("SqlAzure", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("sqlazure", StringComparison.OrdinalIgnoreCase))
        {
            return "SqlAzure";
        }

        if (trimmed.Equals("AzureSql", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("azuresql", StringComparison.OrdinalIgnoreCase))
        {
            return "AzureSql";
        }

        if (trimmed.Equals("PostgreSql", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            return "PostgreSql";
        }

        if (trimmed.Equals("MySql", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            return "MySql";
        }

        if (trimmed.Equals("MariaDb", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("mariadb", StringComparison.OrdinalIgnoreCase))
        {
            return "MariaDb";
        }

        if (trimmed.Equals("Sqlite", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("sqlite", StringComparison.OrdinalIgnoreCase))
        {
            return "Sqlite";
        }

        if (trimmed.Equals("Db2", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            return "Db2";
        }

        if (trimmed.Equals("Oracle", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            return "Oracle";
        }

        if (trimmed.Equals("Firebird", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("firebird", StringComparison.OrdinalIgnoreCase))
        {
            return "Firebird";
        }

        return trimmed;
    }
}

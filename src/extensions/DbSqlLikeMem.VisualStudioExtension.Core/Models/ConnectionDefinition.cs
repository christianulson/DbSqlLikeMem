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
        DatabaseType = databaseType;
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
}

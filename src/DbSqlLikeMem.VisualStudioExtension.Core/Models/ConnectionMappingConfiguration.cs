namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents output mapping rules for a connection.
/// Representa regras de mapeamento de saída para uma conexão.
/// </summary>
public sealed record ConnectionMappingConfiguration
{
    /// <summary>
    /// Initializes a new mapping configuration.
    /// Inicializa uma nova configuração de mapeamento.
    /// </summary>
    public ConnectionMappingConfiguration(string connectionId, IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> mappings)
    {
        ConnectionId = connectionId;
        Mappings = mappings;
    }

    /// <summary>
    /// Gets the associated connection identifier.
    /// Obtém o identificador da conexão associada.
    /// </summary>
    public string ConnectionId { get; }

    /// <summary>
    /// Gets mappings by database object type.
    /// Obtém os mapeamentos por tipo de objeto de banco.
    /// </summary>
    public IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> Mappings { get; }

    /// <summary>
    /// Indicates whether the specified object type has a mapping.
    /// Indica se o tipo de objeto informado possui mapeamento.
    /// </summary>
    public bool HasMappingFor(DatabaseObjectType type) => Mappings.ContainsKey(type);
}

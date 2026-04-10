namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents output mapping rules for a connection.
/// Representa regras de mapeamento de saída para uma conexão.
/// </summary>
public sealed record ConnectionMappingConfiguration
{
    private const string DefaultOutputDirectory = "Generated";
    private const string DefaultFileNamePattern = "{NamePascal}{Type}Factory.cs";

    /// <summary>
    /// Initializes a new mapping configuration.
    /// Inicializa uma nova configuração de mapeamento.
    /// </summary>
    public ConnectionMappingConfiguration(string connectionId, IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> mappings)
    {
        ConnectionId = connectionId;
        Mappings = NormalizeMappings(mappings);
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

    private static IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> NormalizeMappings(
        IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> mappings)
    {
        var normalized = new Dictionary<DatabaseObjectType, ObjectTypeMapping>();
        var baseline = GetBaselineMapping(mappings);

        foreach (DatabaseObjectType objectType in Enum.GetValues(typeof(DatabaseObjectType)))
        {
            if (mappings.TryGetValue(objectType, out var mapping))
            {
                normalized[objectType] = mapping;
                continue;
            }

            normalized[objectType] = new ObjectTypeMapping(
                objectType,
                baseline.OutputDirectory,
                baseline.FileNamePattern,
                baseline.Namespace);
        }

        return normalized;
    }

    private static ObjectTypeMapping GetBaselineMapping(IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> mappings)
    {
        foreach (var preferredType in new[]
        {
            DatabaseObjectType.Table,
            DatabaseObjectType.View,
            DatabaseObjectType.Procedure,
            DatabaseObjectType.Function,
            DatabaseObjectType.Sequence
        })
        {
            if (mappings.TryGetValue(preferredType, out var mapping))
            {
                return mapping;
            }
        }

        return new ObjectTypeMapping(DatabaseObjectType.Table, DefaultOutputDirectory, DefaultFileNamePattern);
    }
}

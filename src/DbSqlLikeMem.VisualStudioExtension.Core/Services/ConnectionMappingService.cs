using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// EN: Resolves and updates per-connection object-type mappings used by code generation.
/// PT: Resolve e atualiza mapeamentos por conexao e tipo de objeto usados pela geracao de codigo.
/// </summary>
public sealed class ConnectionMappingService
{
    private const string DefaultFileNamePattern = "{NamePascal}{Type}Factory.cs";
    private const string DefaultOutputDirectory = "Generated";

    /// <summary>
    /// EN: Creates the default mapping configuration for a connection.
    /// PT: Cria a configuracao padrao de mapeamento para uma conexao.
    /// </summary>
    /// <param name="connectionId">EN: Connection identifier associated with the mapping. PT: Identificador da conexao associado ao mapeamento.</param>
    /// <param name="outputDirectory">EN: Default output directory applied to each object type. PT: Diretorio de saida padrao aplicado a cada tipo de objeto.</param>
    /// <param name="fileNamePattern">EN: Default file name pattern applied to each object type. PT: Padrao de nome de arquivo padrao aplicado a cada tipo de objeto.</param>
    /// <param name="namespace">EN: Optional namespace applied to each object type. PT: Namespace opcional aplicado a cada tipo de objeto.</param>
    public ConnectionMappingConfiguration CreateDefaultConfiguration(
        string connectionId,
        string outputDirectory = DefaultOutputDirectory,
        string fileNamePattern = DefaultFileNamePattern,
        string? @namespace = null)
        => new(connectionId, CreateDefaultMappings(outputDirectory, fileNamePattern, @namespace));

    /// <summary>
    /// EN: Gets the mapping for the informed object type or a default mapping when it is missing.
    /// PT: Obtem o mapeamento do tipo de objeto informado ou um mapeamento padrao quando ele estiver ausente.
    /// </summary>
    /// <param name="configuration">EN: Existing mapping configuration for a connection. PT: Configuracao de mapeamento existente para uma conexao.</param>
    /// <param name="objectType">EN: Object type whose mapping should be resolved. PT: Tipo de objeto cujo mapeamento deve ser resolvido.</param>
    public ObjectTypeMapping GetMappingOrDefault(
        ConnectionMappingConfiguration? configuration,
        DatabaseObjectType objectType)
    {
        if (configuration?.Mappings.TryGetValue(objectType, out var mapping) == true)
        {
            return mapping;
        }

        return new ObjectTypeMapping(objectType, DefaultOutputDirectory, DefaultFileNamePattern);
    }

    /// <summary>
    /// EN: Resolves the recommended mapping defaults from a versioned baseline profile while preserving the informed namespace.
    /// PT: Resolve os defaults recomendados de mapeamento a partir de um perfil versionado de baseline preservando o namespace informado.
    /// </summary>
    /// <param name="profileId">EN: Baseline profile identifier like `api` or `worker`. PT: Identificador do perfil de baseline como `api` ou `worker`.</param>
    /// <param name="objectType">EN: Object type whose recommended mapping should be resolved. PT: Tipo de objeto cujo mapeamento recomendado deve ser resolvido.</param>
    /// <param name="namespace">EN: Optional namespace that should remain associated with the returned mapping. PT: Namespace opcional que deve permanecer associado ao mapeamento retornado.</param>
    public ObjectTypeMapping CreateRecommendedMapping(
        string profileId,
        DatabaseObjectType objectType,
        string? @namespace = null)
    {
        var baselineMapping = TemplateBaselineCatalog.CreateRecommendedMapping(profileId, objectType);
        var effectiveNamespace = string.IsNullOrWhiteSpace(@namespace) ? null : @namespace!.Trim();

        return new ObjectTypeMapping(
            objectType,
            baselineMapping.OutputDirectory,
            baselineMapping.FileNamePattern,
            effectiveNamespace);
    }

    /// <summary>
    /// EN: Updates only the informed object-type mapping while preserving the remaining mappings of the connection.
    /// PT: Atualiza apenas o mapeamento do tipo de objeto informado preservando os mapeamentos restantes da conexao.
    /// </summary>
    /// <param name="connectionId">EN: Connection identifier associated with the mapping. PT: Identificador da conexao associado ao mapeamento.</param>
    /// <param name="configuration">EN: Existing mapping configuration for the connection. PT: Configuracao de mapeamento existente para a conexao.</param>
    /// <param name="objectType">EN: Object type that should receive the new mapping values. PT: Tipo de objeto que deve receber os novos valores de mapeamento.</param>
    /// <param name="fileNamePattern">EN: File name pattern to apply to the informed object type. PT: Padrao de nome de arquivo a aplicar ao tipo de objeto informado.</param>
    /// <param name="outputDirectory">EN: Output directory to apply to the informed object type. PT: Diretorio de saida a aplicar ao tipo de objeto informado.</param>
    /// <param name="namespace">EN: Optional namespace to apply to the informed object type. PT: Namespace opcional a aplicar ao tipo de objeto informado.</param>
    public ConnectionMappingConfiguration UpsertMapping(
        string connectionId,
        ConnectionMappingConfiguration? configuration,
        DatabaseObjectType objectType,
        string fileNamePattern,
        string outputDirectory,
        string? @namespace = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(connectionId, nameof(connectionId));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(fileNamePattern, nameof(connectionId));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(outputDirectory, nameof(connectionId));

        var effective = configuration ?? CreateDefaultConfiguration(connectionId);
        var updatedMappings = effective.Mappings.ToDictionary(entry => entry.Key, entry => entry.Value);
        updatedMappings[objectType] = new ObjectTypeMapping(objectType, outputDirectory, fileNamePattern, @namespace);
        return new ConnectionMappingConfiguration(effective.ConnectionId, updatedMappings);
    }

    private static IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> CreateDefaultMappings(
        string outputDirectory,
        string fileNamePattern,
        string? @namespace)
    {
        return ((DatabaseObjectType[])Enum.GetValues(typeof(DatabaseObjectType)))
            .ToDictionary(
                objectType => objectType,
                objectType => new ObjectTypeMapping(objectType, outputDirectory, fileNamePattern, @namespace));
    }
}

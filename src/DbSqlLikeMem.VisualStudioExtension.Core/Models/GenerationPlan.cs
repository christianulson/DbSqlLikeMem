namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents the generation planning result.
/// Representa o resultado do planejamento de geração.
/// </summary>
public sealed record GenerationPlan
{
    /// <summary>
    /// Initializes a new generation plan.
    /// Inicializa um novo plano de geração.
    /// </summary>
    public GenerationPlan(bool requiresConfiguration, IReadOnlyCollection<DatabaseObjectType> missingMappings, IReadOnlyCollection<DatabaseObjectReference> objectsToGenerate)
    {
        RequiresConfiguration = requiresConfiguration;
        MissingMappings = missingMappings;
        ObjectsToGenerate = objectsToGenerate;
    }

    /// <summary>
    /// Gets whether additional mapping configuration is required.
    /// Obtém se é necessária configuração adicional de mapeamento.
    /// </summary>
    public bool RequiresConfiguration { get; }

    /// <summary>
    /// Gets object types missing mapping configuration.
    /// Obtém os tipos de objeto sem configuração de mapeamento.
    /// </summary>
    public IReadOnlyCollection<DatabaseObjectType> MissingMappings { get; }

    /// <summary>
    /// Gets objects selected for generation.
    /// Obtém os objetos selecionados para geração.
    /// </summary>
    public IReadOnlyCollection<DatabaseObjectReference> ObjectsToGenerate { get; }
}

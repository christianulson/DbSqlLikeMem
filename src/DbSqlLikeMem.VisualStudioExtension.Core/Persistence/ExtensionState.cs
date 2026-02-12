using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Persistence;

/// <summary>
/// Represents persisted extension state.
/// Representa o estado persistido da extensão.
/// </summary>
public sealed record ExtensionState
{
    /// <summary>
    /// Initializes a new extension state.
    /// Inicializa um novo estado da extensão.
    /// </summary>
    public ExtensionState(IReadOnlyCollection<ConnectionDefinition> connections, IReadOnlyCollection<ConnectionMappingConfiguration> mappings, TemplateConfiguration? templateConfiguration = null)
    {
        Connections = connections;
        Mappings = mappings;
        TemplateConfiguration = templateConfiguration ?? TemplateConfiguration.Default;
    }

    /// <summary>
    /// Gets stored connections.
    /// Obtém as conexões armazenadas.
    /// </summary>
    public IReadOnlyCollection<ConnectionDefinition> Connections { get; }

    /// <summary>
    /// Gets stored mappings.
    /// Obtém os mapeamentos armazenados.
    /// </summary>
    public IReadOnlyCollection<ConnectionMappingConfiguration> Mappings { get; }

    /// <summary>
    /// Gets template settings for model/repository generation.
    /// </summary>
    public TemplateConfiguration TemplateConfiguration { get; }
}

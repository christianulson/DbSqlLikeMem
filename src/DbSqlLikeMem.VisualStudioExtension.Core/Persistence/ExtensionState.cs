using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Persistence;

public sealed record ExtensionState(
    IReadOnlyCollection<ConnectionDefinition> Connections,
    IReadOnlyCollection<ConnectionMappingConfiguration> Mappings);

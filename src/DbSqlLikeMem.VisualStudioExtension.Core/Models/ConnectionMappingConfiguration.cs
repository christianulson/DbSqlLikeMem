namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

public sealed record ConnectionMappingConfiguration(
    string ConnectionId,
    IReadOnlyDictionary<DatabaseObjectType, ObjectTypeMapping> Mappings)
{
    public bool HasMappingFor(DatabaseObjectType type) => Mappings.ContainsKey(type);
}

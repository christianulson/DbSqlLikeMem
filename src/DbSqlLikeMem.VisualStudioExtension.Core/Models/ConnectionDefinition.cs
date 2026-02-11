namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

public sealed record ConnectionDefinition(
    string Id,
    string DatabaseType,
    string DatabaseName,
    string ConnectionString,
    string? DisplayName = null)
{
    public string FriendlyName => DisplayName ?? DatabaseName;
}

namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

public sealed record DatabaseObjectReference(
    string Schema,
    string Name,
    DatabaseObjectType Type,
    IReadOnlyDictionary<string, string>? Properties = null)
{
    public string FullName => string.IsNullOrWhiteSpace(Schema) ? Name : $"{Schema}.{Name}";
}

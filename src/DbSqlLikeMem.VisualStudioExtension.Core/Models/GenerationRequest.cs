namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

public sealed record GenerationRequest(
    ConnectionDefinition Connection,
    IReadOnlyCollection<DatabaseObjectReference> SelectedObjects);

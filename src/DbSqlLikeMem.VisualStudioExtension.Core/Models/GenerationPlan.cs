namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

public sealed record GenerationPlan(
    bool RequiresConfiguration,
    IReadOnlyCollection<DatabaseObjectType> MissingMappings,
    IReadOnlyCollection<DatabaseObjectReference> ObjectsToGenerate);

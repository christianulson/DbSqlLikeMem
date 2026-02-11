using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Validation;

public sealed record LocalObjectSnapshot(
    DatabaseObjectReference Reference,
    string FilePath,
    IReadOnlyDictionary<string, string>? Properties = null);

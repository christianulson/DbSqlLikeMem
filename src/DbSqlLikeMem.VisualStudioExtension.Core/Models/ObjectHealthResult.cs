namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

public sealed record ObjectHealthResult(
    DatabaseObjectReference DatabaseObject,
    string LocalFilePath,
    ObjectHealthStatus Status,
    string? Message = null);

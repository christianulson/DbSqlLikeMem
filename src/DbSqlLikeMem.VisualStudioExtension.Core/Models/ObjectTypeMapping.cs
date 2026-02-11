namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

public sealed record ObjectTypeMapping(
    DatabaseObjectType ObjectType,
    string OutputDirectory,
    string FileNamePattern = "{Name}.generated.cs");

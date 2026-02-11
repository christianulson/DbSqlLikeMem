using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

public sealed class TreeNode
{
    public required string Label { get; init; }

    public string? ContextKey { get; init; }

    public DatabaseObjectType? ObjectType { get; init; }

    public List<TreeNode> Children { get; } = [];
}

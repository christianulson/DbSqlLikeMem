using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class TreeNode
{
    public TreeNode(string label)
    {
        Label = label;
    }

    /// <summary>
    /// Gets this API value.
    /// Obtém este valor da API.
    /// </summary>
    public string Label { get; }

    /// <summary>
    /// Gets this API value.
    /// Obtém este valor da API.
    /// </summary>
    public string? ContextKey { get; init; }

    /// <summary>
    /// Gets this API value.
    /// Obtém este valor da API.
    /// </summary>
    public DatabaseObjectType? ObjectType { get; init; }

    /// <summary>
    /// Gets this API value.
    /// Obtém este valor da API.
    /// </summary>
    public List<TreeNode> Children { get; } = [];
}

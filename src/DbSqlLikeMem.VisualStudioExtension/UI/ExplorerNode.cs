using System.Collections.ObjectModel;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

public enum ExplorerNodeKind
{
    DatabaseType,
    Connection,
    ObjectType,
    Object
}

public sealed class ExplorerNode
{
    public required string Label { get; init; }

    public required ExplorerNodeKind Kind { get; init; }

    public string? ConnectionId { get; init; }

    public DatabaseObjectType? ObjectType { get; init; }

    public DatabaseObjectReference? DatabaseObject { get; init; }

    public ObjectHealthStatus? HealthStatus { get; set; }

    public string StatusGlyph => HealthStatus switch
    {
        ObjectHealthStatus.Synchronized => "🟢",
        ObjectHealthStatus.DifferentFromDatabase => "🟡",
        ObjectHealthStatus.MissingInDatabase or ObjectHealthStatus.MissingLocalArtifacts => "🔴",
        _ => string.Empty
    };

    public ObservableCollection<ExplorerNode> Children { get; } = new();
}

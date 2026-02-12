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
    public ExplorerNode(string label, ExplorerNodeKind kind)
    {
        Label = label;
        Kind = kind;
    }

    public string Label { get; }

    public ExplorerNodeKind Kind { get; }

    public string? ConnectionId { get; set; }

    public DatabaseObjectType? ObjectType { get; set; }

    public DatabaseObjectReference? DatabaseObject { get; set; }

    public ObjectHealthStatus? HealthStatus { get; set; }

    public string StatusGlyph => HealthStatus switch
    {
        ObjectHealthStatus.Synchronized => "🟢",
        ObjectHealthStatus.DifferentFromDatabase => "🟡",
        ObjectHealthStatus.MissingInDatabase or ObjectHealthStatus.MissingLocalArtifacts => "🔴",
        _ => string.Empty
    };

    public ObservableCollection<ExplorerNode> Children { get; } = [];
}

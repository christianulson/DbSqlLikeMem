using System.Collections.ObjectModel;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

/// <summary>
/// Represents node categories used in the explorer tree.
/// Representa as categorias de nó usadas na árvore do explorador.
/// </summary>
public enum ExplorerNodeKind
{
    /// <summary>
    /// Represents a database provider group node.
    /// Representa um nó de agrupamento por tipo de banco.
    /// </summary>
    DatabaseType,

    /// <summary>
    /// Represents a saved connection node.
    /// Representa um nó de conexão salva.
    /// </summary>
    Connection,

    /// <summary>
    /// Represents an object-type grouping node.
    /// Representa um nó de agrupamento por tipo de objeto.
    /// </summary>
    ObjectType,

    /// <summary>
    /// Represents a database object leaf node.
    /// Representa um nó folha de objeto de banco.
    /// </summary>
    Object
}

/// <summary>
/// Represents an explorer tree node displayed in the UI.
/// Representa um nó da árvore do explorador exibida na interface.
/// </summary>
public sealed class ExplorerNode
{
    /// <summary>
    /// Initializes a new explorer node with a label and kind.
    /// Inicializa um novo nó do explorador com rótulo e tipo.
    /// </summary>
    public ExplorerNode(string label, ExplorerNodeKind kind)
    {
        Label = label;
        Kind = kind;
    }

    /// <summary>
    /// Gets the display label of the node.
    /// Obtém o rótulo de exibição do nó.
    /// </summary>
    public string Label { get; }

    /// <summary>
    /// Gets the category of this node.
    /// Obtém a categoria deste nó.
    /// </summary>
    public ExplorerNodeKind Kind { get; }

    /// <summary>
    /// Gets or sets the associated connection identifier.
    /// Obtém ou define o identificador de conexão associado.
    /// </summary>
    public string? ConnectionId { get; set; }

    /// <summary>
    /// Gets or sets the database object type represented by this node.
    /// Obtém ou define o tipo de objeto de banco representado por este nó.
    /// </summary>
    public DatabaseObjectType? ObjectType { get; set; }

    /// <summary>
    /// Gets or sets the database object metadata represented by this node.
    /// Obtém ou define os metadados do objeto de banco representado por este nó.
    /// </summary>
    public DatabaseObjectReference? DatabaseObject { get; set; }

    /// <summary>
    /// Gets or sets the health status of the represented object.
    /// Obtém ou define o status de saúde do objeto representado.
    /// </summary>
    public ObjectHealthStatus? HealthStatus { get; set; }

    /// <summary>
    /// Gets an emoji glyph that represents the current health status.
    /// Obtém um glifo em emoji que representa o status de saúde atual.
    /// </summary>
    public string StatusGlyph => HealthStatus switch
    {
        ObjectHealthStatus.Synchronized => "🟢",
        ObjectHealthStatus.DifferentFromDatabase => "🟡",
        ObjectHealthStatus.MissingInDatabase or ObjectHealthStatus.MissingLocalArtifacts => "🔴",
        _ => string.Empty
    };


    /// <summary>
    /// Gets or sets whether the node is expanded in the tree view.
    /// Obtém ou define se o nó está expandido na árvore.
    /// </summary>
    public bool IsExpanded { get; set; }

    /// <summary>
    /// Gets the child nodes of this explorer node.
    /// Obtém os nós filhos deste nó do explorador.
    /// </summary>
    public ObservableCollection<ExplorerNode> Children { get; } = [];
}

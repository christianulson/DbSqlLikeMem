using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// EN: Represents a node in the generated connection tree.
/// PT: Representa um nó na árvore de conexoes gerada.
/// </summary>
/// <remarks>
/// EN: Creates a tree node with the provided label.
/// PT: Cria um nó de árvore com o rótulo informado.
/// </remarks>
public sealed class TreeNode(string label)
{

    /// <summary>
    /// EN: Gets the node label shown in the tree.
    /// PT: Obtém o rótulo do nó exibido na árvore.
    /// </summary>
    public string Label { get; } = label;

    /// <summary>
    /// EN: Gets or sets the context key associated with the node.
    /// PT: Obtém ou define a chave de contexto associada ao nó.
    /// </summary>
    public string? ContextKey { get; init; }

    /// <summary>
    /// EN: Gets or sets the database object type represented by the node.
    /// PT: Obtém ou define o tipo de objeto de banco representado pelo nó.
    /// </summary>
    public DatabaseObjectType? ObjectType { get; init; }

    /// <summary>
    /// EN: Gets the parent node in the generated tree.
    /// PT: Obtém o nó pai na árvore gerada.
    /// </summary>
    public TreeNode? Parent { get; private set; }

    /// <summary>
    /// EN: Gets the child nodes of this node.
    /// PT: Obtém os nós filhos deste nó.
    /// </summary>
    public List<TreeNode> Children { get; } = [];

    /// <summary>
    /// EN: Adds a child node and links it back to this node as its parent.
    /// PT: Adiciona um nó filho e o vincula a este nó como pai.
    /// </summary>
    /// <param name="child">EN: The child node to add. PT: O nó filho a adicionar.</param>
    public void AddChild(TreeNode child)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(child, nameof(child));

        child.Parent = this;
        Children.Add(child);
    }
}

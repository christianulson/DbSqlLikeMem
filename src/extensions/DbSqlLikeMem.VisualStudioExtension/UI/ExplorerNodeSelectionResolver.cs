namespace DbSqlLikeMem.VisualStudioExtension.UI;

/// <summary>
/// EN: Resolves the effective explorer node used by context menu actions.
/// PT-br: Resolve o no efetivo do explorador usado pelas acoes do menu de contexto.
/// </summary>
internal static class ExplorerNodeSelectionResolver
{
    /// <summary>
    /// EN: Returns the object node that should drive the current explorer selection.
    /// PT-br: Retorna o no de objeto que deve direcionar a selecao atual do explorador.
    /// </summary>
    internal static ExplorerNode? GetEffectiveSelectedNode(ExplorerNode? node)
    {
        if (node?.Kind is ExplorerNodeKind.TableDetailGroup or ExplorerNodeKind.TableDetailItem)
        {
            return FindParentObjectNode(node);
        }

        return node;
    }

    /// <summary>
    /// EN: Walks the parent chain until the owning object node is found.
    /// PT-br: Percorre a cadeia de pais ate encontrar o no de objeto proprietario.
    /// </summary>
    internal static ExplorerNode? FindParentObjectNode(ExplorerNode? detailNode)
    {
        var current = detailNode?.Parent;
        while (current is not null)
        {
            if (current.Kind == ExplorerNodeKind.Object)
            {
                return current;
            }

            current = current.Parent;
        }

        return null;
    }
}

namespace DbSqlLikeMem.VisualStudioExtension.Tests;

/// <summary>
/// EN: Verifies explorer node promotion rules used by the Visual Studio tool window context menu.
/// PT: Verifica as regras de promocao de no do explorador usadas pelo menu de contexto da janela de ferramentas do Visual Studio.
/// </summary>
public sealed class ExplorerNodeSelectionResolverTests
{
    /// <summary>
    /// EN: Ensures object nodes stay selected as-is when they already represent the effective context.
    /// PT: Garante que nos de objeto permaneçam selecionados como estao quando ja representam o contexto efetivo.
    /// </summary>
    [Fact]
    [Trait("Category", "VisualStudioSelectionResolver")]
    public void GetEffectiveSelectedNode_WhenObjectNode_ReturnsSameNode()
    {
        var node = new ExplorerNode("Orders", ExplorerNodeKind.Object)
        {
            ObjectType = DatabaseObjectType.Table
        };

        var effective = ExplorerNodeSelectionResolver.GetEffectiveSelectedNode(node);

        Assert.Same(node, effective);
    }

    /// <summary>
    /// EN: Ensures table detail groups promote to the owning object node.
    /// PT: Garante que grupos de detalhe de tabela sejam promovidos para o no de objeto proprietario.
    /// </summary>
    [Fact]
    [Trait("Category", "VisualStudioSelectionResolver")]
    public void GetEffectiveSelectedNode_WhenDetailGroup_ReturnsOwningObject()
    {
        var (objectNode, detailGroup, _) = BuildDetailChain();

        var effective = ExplorerNodeSelectionResolver.GetEffectiveSelectedNode(detailGroup);

        Assert.Same(objectNode, effective);
    }

    /// <summary>
    /// EN: Ensures table detail items also promote to the owning object node.
    /// PT: Garante que itens de detalhe de tabela tambem sejam promovidos para o no de objeto proprietario.
    /// </summary>
    [Fact]
    [Trait("Category", "VisualStudioSelectionResolver")]
    public void GetEffectiveSelectedNode_WhenDetailItem_ReturnsOwningObject()
    {
        var (objectNode, _, detailItem) = BuildDetailChain();

        var effective = ExplorerNodeSelectionResolver.GetEffectiveSelectedNode(detailItem);

        Assert.Same(objectNode, effective);
    }

    /// <summary>
    /// EN: Ensures detail nodes without an owning object do not produce a false context.
    /// PT: Garante que nos de detalhe sem objeto proprietario nao produzam um contexto falso.
    /// </summary>
    [Fact]
    [Trait("Category", "VisualStudioSelectionResolver")]
    public void GetEffectiveSelectedNode_WhenNoObjectAncestor_ReturnsNull()
    {
        var orphanDetail = new ExplorerNode("Columns", ExplorerNodeKind.TableDetailGroup);

        var effective = ExplorerNodeSelectionResolver.GetEffectiveSelectedNode(orphanDetail);

        Assert.Null(effective);
    }

    private static (ExplorerNode ObjectNode, ExplorerNode DetailGroup, ExplorerNode DetailItem) BuildDetailChain()
    {
        var objectNode = new ExplorerNode("Orders", ExplorerNodeKind.Object)
        {
            ObjectType = DatabaseObjectType.Table
        };

        var detailGroup = new ExplorerNode("Columns", ExplorerNodeKind.TableDetailGroup)
        {
            TableDetailKind = "Columns"
        };

        var detailItem = new ExplorerNode("Id", ExplorerNodeKind.TableDetailItem)
        {
            TableDetailKind = "Column"
        };

        objectNode.AddChild(detailGroup);
        detailGroup.AddChild(detailItem);
        return (objectNode, detailGroup, detailItem);
    }
}

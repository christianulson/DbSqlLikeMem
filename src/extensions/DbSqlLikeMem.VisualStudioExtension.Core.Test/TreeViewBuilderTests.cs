namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies the explorer tree built by the Visual Studio extension core.
/// PT: Verifica a arvore do explorador montada pelo core da extensao do Visual Studio.
/// </summary>
public sealed class TreeViewBuilderTests
{
    /// <summary>
    /// EN: Verifies function objects are grouped under the Functions node and sorted by name.
    /// PT: Verifica se objetos function sao agrupados sob o no Functions e ordenados por nome.
    /// </summary>
    [Fact]
    [Trait("Category", "TreeViewBuilder")]
    public void Build_ShouldGroupFunctionsAndSortChildren()
    {
        var builder = new TreeViewBuilder();
        var connection = new ConnectionDefinition("conn-1", "SqlServer", "ERP", "Server=.;Initial Catalog=erp;");
        var objects =
            new[]
            {
                new DatabaseObjectReference("dbo", "Invoices", DatabaseObjectType.Sequence, "public"),
                new DatabaseObjectReference("dbo", "vw_Customers", DatabaseObjectType.View, "public"),
                new DatabaseObjectReference("dbo", "fn_Tax", DatabaseObjectType.Function, "public"),
                new DatabaseObjectReference("dbo", "Customers", DatabaseObjectType.Table, "public"),
                new DatabaseObjectReference("dbo", "fn_Adjust", DatabaseObjectType.Function, "public"),
                new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table, "public"),
                new DatabaseObjectReference("dbo", "vw_Orders", DatabaseObjectType.View, "public"),
                new DatabaseObjectReference("dbo", "seq_Audit", DatabaseObjectType.Sequence, "public")
            };

        var root = builder.Build(connection, objects);

        Assert.Equal("SqlServer", root.Label);
        Assert.Null(root.Parent);
        var databaseNode = Assert.Single(root.Children);
        Assert.Equal("ERP", databaseNode.Label);
        Assert.Same(root, databaseNode.Parent);
        Assert.Equal(["Tables", "Views", "Procedures", "Functions", "Sequences"], databaseNode.Children.Select(child => child.Label).ToArray());

        var functionNode = Assert.Single(databaseNode.Children, child => child.Label == "Functions");
        Assert.Equal("object-type", functionNode.ContextKey);
        Assert.Equal(DatabaseObjectType.Function, functionNode.ObjectType);
        Assert.Same(databaseNode, functionNode.Parent);
        Assert.Equal(["fn_Adjust", "fn_Tax"], functionNode.Children.Select(child => child.Label).ToArray());
        Assert.All(functionNode.Children, child => Assert.Same(functionNode, child.Parent));

        var tableNode = Assert.Single(databaseNode.Children, child => child.Label == "Tables");
        Assert.Same(databaseNode, tableNode.Parent);
        Assert.Equal(["Customers", "Orders"], tableNode.Children.Select(child => child.Label).ToArray());
        Assert.All(tableNode.Children, child => Assert.Same(tableNode, child.Parent));

        var viewNode = Assert.Single(databaseNode.Children, child => child.Label == "Views");
        Assert.Same(databaseNode, viewNode.Parent);
        Assert.Equal(["vw_Customers", "vw_Orders"], viewNode.Children.Select(child => child.Label).ToArray());

        var sequenceNode = Assert.Single(databaseNode.Children, child => child.Label == "Sequences");
        Assert.Same(databaseNode, sequenceNode.Parent);
        Assert.Equal(["Invoices", "seq_Audit"], sequenceNode.Children.Select(child => child.Label).ToArray());
    }
}

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies object name filtering in the extension core.
/// PT: Verifica a filtragem por nome de objeto no core da extensao.
/// </summary>
public class ObjectFilterServiceTests
{
    /// <summary>
    /// EN: Verifies exact matching returns only the selected object.
    /// PT: Verifica se a correspondencia exata retorna apenas o objeto selecionado.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectFilterService")]
    public void Filter_Equals_ReturnsExactMatches()
    {
        var service = new ObjectFilterService();
        var source = new[]
        {
            new DatabaseObjectReference("dbo", "User", DatabaseObjectType.Table, "public"),
            new DatabaseObjectReference("dbo", "Users", DatabaseObjectType.Table, "public")
        };

        var result = service.Filter(source, "User", FilterMode.Equals);

        Assert.Single(result);
        Assert.Equal("User", result.Single().Name);
    }

    /// <summary>
    /// EN: Verifies contains matching returns all objects whose names include the filter value.
    /// PT: Verifica se a correspondencia por contem retorna todos os objetos cujo nome inclui o valor filtrado.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectFilterService")]
    public void Filter_Like_ReturnsContainsMatches()
    {
        var service = new ObjectFilterService();
        var source = new[]
        {
            new DatabaseObjectReference("dbo", "Order", DatabaseObjectType.Table, "public"),
            new DatabaseObjectReference("dbo", "OrderItem", DatabaseObjectType.Table, "public"),
            new DatabaseObjectReference("dbo", "User", DatabaseObjectType.Table, "public")
        };

        var result = service.Filter(source, "order", FilterMode.Like);

        Assert.Equal(2, result.Count);
    }

    /// <summary>
    /// EN: Verifies blank filter values return the original object set.
    /// PT: Verifica se valores de filtro em branco retornam o conjunto original de objetos.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectFilterService")]
    public void Filter_EmptyValue_ReturnsAllObjects()
    {
        var service = new ObjectFilterService();
        var source = new[]
        {
            new DatabaseObjectReference("dbo", "Order", DatabaseObjectType.Table, "public"),
            new DatabaseObjectReference("dbo", "OrderItem", DatabaseObjectType.Table, "public"),
            new DatabaseObjectReference("dbo", "User", DatabaseObjectType.Table, "public")
        };

        var result = service.Filter(source, "   ", FilterMode.Like);

        Assert.Equal(source.Length, result.Count);
    }
}

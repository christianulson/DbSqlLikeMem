namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public class ObjectFilterServiceTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectFilterService")]
    public void Filter_Equals_ReturnsExactMatches()
    {
        var service = new ObjectFilterService();
        var source = new[]
        {
            new DatabaseObjectReference("dbo", "User", DatabaseObjectType.Table),
            new DatabaseObjectReference("dbo", "Users", DatabaseObjectType.Table)
        };

        var result = service.Filter(source, "User", FilterMode.Equals);

        Assert.Single(result);
        Assert.Equal("User", result.Single().Name);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectFilterService")]
    public void Filter_Like_ReturnsContainsMatches()
    {
        var service = new ObjectFilterService();
        var source = new[]
        {
            new DatabaseObjectReference("dbo", "Order", DatabaseObjectType.Table),
            new DatabaseObjectReference("dbo", "OrderItem", DatabaseObjectType.Table),
            new DatabaseObjectReference("dbo", "User", DatabaseObjectType.Table)
        };

        var result = service.Filter(source, "order", FilterMode.Like);

        Assert.Equal(2, result.Count);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectFilterService")]
    public void Filter_EmptyValue_ReturnsAllObjects()
    {
        var service = new ObjectFilterService();
        var source = new[]
        {
            new DatabaseObjectReference("dbo", "Order", DatabaseObjectType.Table),
            new DatabaseObjectReference("dbo", "OrderItem", DatabaseObjectType.Table),
            new DatabaseObjectReference("dbo", "User", DatabaseObjectType.Table)
        };

        var result = service.Filter(source, "   ", FilterMode.Like);

        Assert.Equal(source.Length, result.Count);
    }
}

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class GenerationRuleSetTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void MapDbType_UsesMySqlStrategyWhenDatabaseTypeIsMySql()
    {
        var type = GenerationRuleSet.MapDbType("bit", null, 8, "Mask", "MySql");
        Assert.Equal("UInt64", type);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void MapDbType_UsesDefaultStrategyWhenDatabaseTypeIsSqlServer()
    {
        var type = GenerationRuleSet.MapDbType("tinyint", null, 1, "IsEnabled", "SqlServer");
        Assert.Equal("Byte", type);
    }
}

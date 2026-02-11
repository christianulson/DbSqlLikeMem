namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

public sealed class GenerationRuleSetTests
{
    [Fact]
    public void MapDbType_UsesMySqlStrategyWhenDatabaseTypeIsMySql()
    {
        var type = GenerationRuleSet.MapDbType("bit", null, 8, "Mask", "MySql");
        Assert.Equal("UInt64", type);
    }

    [Fact]
    public void MapDbType_UsesDefaultStrategyWhenDatabaseTypeIsSqlServer()
    {
        var type = GenerationRuleSet.MapDbType("tinyint", null, 1, "IsEnabled", "SqlServer");
        Assert.Equal("Byte", type);
    }
}

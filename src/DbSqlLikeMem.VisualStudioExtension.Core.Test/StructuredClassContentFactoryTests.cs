namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class StructuredClassContentFactoryTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void Build_GeneratesColumnsPkIndexesAndForeignKeysLikeConsole()
    {
        var dbObject = new DatabaseObjectReference(
            "dbo",
            "orders",
            DatabaseObjectType.Table,
            new Dictionary<string, string>
            {
                ["Columns"] = "Id|int|0|0|1|||0|int|;CustomerId|int|1|0|0||||int|",
                ["PrimaryKey"] = "Id",
                ["Indexes"] = "IX_Orders_CustomerId|0|CustomerId",
                ["ForeignKeys"] = "CustomerId|Customers|Id"
            });

        var content = StructuredClassContentFactory.Build(dbObject, "Sample.Namespace");

        Assert.Contains("table.AddColumn(\"Id\", DbType.Int32, false, true", content);
        Assert.Contains("table.AddPrimaryKeyIndexes(\"Id\");", content);
        Assert.Contains("table.CreateIndex(\"IX_Orders_CustomerId\", [\"CustomerId\"], unique: false);", content);
        Assert.Contains("table.CreateForeignKey(\"CustomerId\", \"Customers\", \"Id\");", content);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void Build_WithCompositePrimaryKey_CreatesPrimaryIndexWithAllFields()
    {
        var dbObject = new DatabaseObjectReference(
            "dbo",
            "order_items",
            DatabaseObjectType.Table,
            new Dictionary<string, string>
            {
                ["Columns"] = "OrderId|int|0|0|0||||int|;ItemId|int|1|0|0||||int|;Qty|int|2|0|0||||int|",
                ["PrimaryKey"] = "OrderId,ItemId",
                ["Indexes"] = "",
                ["ForeignKeys"] = ""
            });

        var content = StructuredClassContentFactory.Build(dbObject);

        Assert.Contains("table.AddPrimaryKeyIndexes(\"OrderId\",\"ItemId\");", content);
        Assert.Contains("table.CreateIndex(\"PRIMARY\", [\"OrderId\", \"ItemId\"], unique: true);", content);
    }


    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void Build_WithSqlServerStrategy_DoesNotTreatTinyIntAsBoolean()
    {
        var dbObject = new DatabaseObjectReference(
            "dbo",
            "feature_flags",
            DatabaseObjectType.Table,
            new Dictionary<string, string>
            {
                ["Columns"] = "IsEnabled|tinyint|0|0|0|1||0|tinyint(1)||1;BitMask|bit|1|0|0|0||0|bit(8)||8",
                ["PrimaryKey"] = "",
                ["Indexes"] = "",
                ["ForeignKeys"] = ""
            });

        var content = StructuredClassContentFactory.Build(dbObject, databaseType: "SqlServer");

        Assert.Contains("table.AddColumn(\"IsEnabled\", DbType.Byte, false", content);
        Assert.Contains("table.AddColumn(\"BitMask\", DbType.Boolean, false", content);
    }
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void Build_UsesSameTypeRulesAsConsoleGenerator_ForTinyIntAndBit()
    {
        var dbObject = new DatabaseObjectReference(
            "dbo",
            "feature_flags",
            DatabaseObjectType.Table,
            new Dictionary<string, string>
            {
                ["Columns"] = "IsEnabled|tinyint|0|0|0|1||0|tinyint(1)||1;BitMask|bit|1|0|0|0||0|bit(8)||8",
                ["PrimaryKey"] = "",
                ["Indexes"] = "",
                ["ForeignKeys"] = ""
            });

        var content = StructuredClassContentFactory.Build(dbObject);

        Assert.Contains("table.AddColumn(\"IsEnabled\", DbType.Boolean, false", content);
        Assert.Contains("table.AddColumn(\"BitMask\", DbType.UInt64, false", content);
        Assert.Contains(", defaultValue: true", content);
    }
}

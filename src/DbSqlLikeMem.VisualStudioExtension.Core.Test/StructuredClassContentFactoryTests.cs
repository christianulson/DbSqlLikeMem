namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

public sealed class StructuredClassContentFactoryTests
{
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

        Assert.Contains("table.Columns[\"Id\"] = new(0, DbType.Int32, false, true);", content);
        Assert.Contains("table.PrimaryKeyIndexes.Add(table.Columns[\"Id\"]?.Index);", content);
        Assert.Contains("table.CreateIndex(new IndexDef(\"IX_Orders_CustomerId\", [\"CustomerId\"], unique: false));", content);
        Assert.Contains("table.ForeignKeys.Add((\"CustomerId\", \"Customers\", \"Id\"));", content);
    }

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

        Assert.Contains("table.PrimaryKeyIndexes.Add(table.Columns[\"OrderId\"]?.Index);", content);
        Assert.Contains("table.PrimaryKeyIndexes.Add(table.Columns[\"ItemId\"]?.Index);", content);
        Assert.Contains("table.CreateIndex(new IndexDef(\"PRIMARY\", [\"OrderId\", \"ItemId\"], unique: true));", content);
    }


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

        Assert.Contains("table.Columns[\"IsEnabled\"] = new(0, DbType.Byte, false);", content);
        Assert.Contains("table.Columns[\"BitMask\"] = new(1, DbType.Boolean, false);", content);
    }
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

        Assert.Contains("table.Columns[\"IsEnabled\"] = new(0, DbType.Boolean, false);", content);
        Assert.Contains("table.Columns[\"BitMask\"] = new(1, DbType.UInt64, false);", content);
        Assert.Contains("table.Columns[\"IsEnabled\"].DefaultValue = true;", content);
    }
}

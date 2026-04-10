namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies structured class generation for tables, sequences, and routines.
/// PT: Verifica a geracao estruturada de classes para tabelas, sequences e rotinas.
/// </summary>
public sealed class StructuredClassContentFactoryTests
{
    /// <summary>
    /// EN: Verifies table generation keeps columns, primary keys, indexes, and foreign keys aligned with the console generator.
    /// PT: Verifica se a geracao de tabela mantem colunas, chaves primarias, indices e chaves estrangeiras alinhados com o gerador de console.
    /// </summary>
    [Fact]
    [Trait("Category", "StructuredClassContentFactory")]
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
        Assert.Contains("table.CreateForeignKey(\"FK_orders_CustomerId_Customers_Id\", \"Customers\", [(\"CustomerId\", \"Id\")]);", content);
    }

    /// <summary>
    /// EN: Verifies composite primary keys are emitted as a single primary index that contains every key field.
    /// PT: Verifica se chaves primarias compostas sao emitidas como um unico indice primario contendo todos os campos da chave.
    /// </summary>
    [Fact]
    [Trait("Category", "StructuredClassContentFactory")]
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
    /// EN: Verifies the SQL Server strategy keeps tinyint mapped as a byte and bit mapped as a boolean.
    /// PT: Verifica se a estrategia de SQL Server mantem tinyint mapeado como byte e bit mapeado como boolean.
    /// </summary>
    [Fact]
    [Trait("Category", "StructuredClassContentFactory")]
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
    /// EN: Verifies the default type rules match the console generator for tinyint and bit columns.
    /// PT: Verifica se as regras de tipo padrao coincidem com o gerador de console para colunas tinyint e bit.
    /// </summary>
    [Fact]
    [Trait("Category", "StructuredClassContentFactory")]
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

    /// <summary>
    /// EN: Verifies sequence generation emits the sequence factory and schema registration call.
    /// PT: Verifica se a geracao de sequence emite a factory da sequence e a chamada de registro do schema.
    /// </summary>
    [Fact]
    [Trait("Category", "StructuredClassContentFactory")]
    public void Build_ForSequence_GeneratesSequenceFactoryUsingSchemaRegistration()
    {
        var dbObject = new DatabaseObjectReference(
            "dbo",
            "seq_orders",
            DatabaseObjectType.Sequence,
            new Dictionary<string, string>
            {
                ["StartValue"] = "100",
                ["IncrementBy"] = "5",
                ["CurrentValue"] = "115"
            });

        var content = StructuredClassContentFactory.Build(dbObject, "Sample.Namespace");

        Assert.Contains("public static SequenceDef CreateSequenceSeqOrders", content);
        Assert.Contains("db.AddSequence(\"seq_orders\", startValue: 100L, incrementBy: 5L, currentValue: 115L, schemaName: \"dbo\")", content);
        Assert.Contains("// DBSqlLikeMem:CurrentValue=115", content);
    }

    /// <summary>
    /// EN: Verifies procedure generation emits routine metadata and adds the procedure to the database snapshot.
    /// PT: Verifica se a geracao de procedure emite os metadados da rotina e adiciona a procedure ao snapshot do banco.
    /// </summary>
    [Fact]
    [Trait("Category", "StructuredClassContentFactory")]
    public void Build_ForProcedure_GeneratesProcedureFactoryUsingRoutineMetadata()
    {
        var dbObject = new DatabaseObjectReference(
            "dbo",
            "sp_update_customer",
            DatabaseObjectType.Procedure,
            new Dictionary<string, string>
            {
                ["RequiredIn"] = "CustomerId|Int32|1|",
                ["OptionalIn"] = "Region|String|0|EU",
                ["OutParams"] = "RowsAffected|Int32|1|",
                ["ReturnParam"] = "ReturnCode|Int32|0|"
            });

        var content = StructuredClassContentFactory.Build(dbObject, "Sample.Namespace");

        Assert.Contains("public static ProcedureDef CreateProcedureSpUpdateCustomer", content);
        Assert.Contains("new ProcedureDef(\"sp_update_customer\", [new ProcParam(\"CustomerId\", DbType.Int32, true)], [new ProcParam(\"Region\", DbType.String, false, \"EU\")], [new ProcParam(\"RowsAffected\", DbType.Int32, true)], new ProcParam(\"ReturnCode\", DbType.Int32, false))", content);
        Assert.Contains("db.AddProcedure(procedure, schemaName: \"dbo\")", content);
    }

    /// <summary>
    /// EN: Verifies function generation emits routine metadata and adds the function to the database snapshot.
    /// PT: Verifica se a geracao de function emite os metadados da rotina e adiciona a function ao snapshot do banco.
    /// </summary>
    [Fact]
    [Trait("Category", "StructuredClassContentFactory")]
    public void Build_ForFunction_GeneratesFunctionFactoryUsingRoutineMetadata()
    {
        var dbObject = new DatabaseObjectReference(
            "dbo",
            "fn_total",
            DatabaseObjectType.Function,
            new Dictionary<string, string>
            {
                ["Parameters"] = "CustomerId|int|1|0|0|0|",
                ["ReturnTypeSql"] = "int",
                ["BodySql"] = "CustomerId"
            });

        var content = StructuredClassContentFactory.Build(dbObject, "Sample.Namespace");

        Assert.Contains("public static DbFunctionDef CreateFunctionFnTotal", content);
        Assert.Contains("DbFunctionDef.CreateUserDefined(\"fn_total\", \"int\", [new DbFunctionParameterDef(\"CustomerId\", \"int\", true, false, false, false)], \"CustomerId\", db)", content);
        Assert.Contains("db.AddFunction(function, schemaName: \"dbo\")", content);
    }
}

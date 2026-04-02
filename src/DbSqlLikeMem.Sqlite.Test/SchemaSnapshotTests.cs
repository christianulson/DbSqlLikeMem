using FluentAssertions;
using System.Text.Json;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Covers the first schema snapshot export/load slice over the SQLite mock surface.
/// PT: Cobre a primeira fatia de exportacao/carga de schema snapshot sobre a superficie do mock SQLite.
/// </summary>
public sealed class SchemaSnapshotTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies schema snapshot export preserves table and column structural metadata.
    /// PT: Verifica se a exportacao de schema snapshot preserva metadados estruturais de tabelas e colunas.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void Export_ShouldCaptureBasicStructuralMetadata()
    {
        var db = new SqliteDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false, identity: true),
            new("Name", DbType.String, false, size: 50, defaultValue: "anonymous"),
            new("Amount", DbType.Decimal, false, decimalPlaces: 2, defaultValue: 10.5m),
            new("Status", DbType.String, true, size: 20, enumValues: ["new", "done"])
        ]);

        var table = db.GetTable("Users");
        table.NextIdentity = 42;

        using var connection = new SqliteConnectionMock(db);

        var snapshot = SchemaSnapshot.Export(connection);

        snapshot.DialectName.Should().BeEquivalentTo("sqlite");
        snapshot.Version.Should().Be(db.Version);

        var schema = snapshot.Schemas.Should().ContainSingle().Subject;
        schema.Name.Should().BeEquivalentTo("DefaultSchema");

        var exportedTable = schema.Tables.Should().ContainSingle().Subject;
        exportedTable.Name.Should().BeEquivalentTo("users");
        exportedTable.NextIdentity.Should().Be(42);
        exportedTable.Columns.Count.Should().Be(4);

        var id = exportedTable.Columns[0];
        id.Name.Should().BeEquivalentTo("Id");
        id.Identity.Should().BeTrue();
        id.DbType.Should().Be(DbType.Int32);

        var amount = exportedTable.Columns[2];
        amount.DbType.Should().Be(DbType.Decimal);
        amount.DecimalPlaces.Should().Be(2);
        amount.DefaultValue.Should().NotBeNull();

        var status = exportedTable.Columns[3];
        status.EnumValues.Should().Equal(["done", "new"]);
    }

    /// <summary>
    /// EN: Verifies schema snapshot JSON round-trip can rebuild the target database structure deterministically.
    /// PT: Verifica se o round-trip JSON do schema snapshot consegue reconstruir a estrutura do banco de destino de forma deterministica.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void LoadAndApplyTo_ShouldReplaceTargetStructuralState()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false, identity: true),
            new("Name", DbType.String, false, size: 50, defaultValue: "anonymous"),
            new("Amount", DbType.Decimal, false, decimalPlaces: 2, defaultValue: 10.5m)
        ]);
        sourceDb.GetTable("Users").NextIdentity = 17;

        using var sourceConnection = new SqliteConnectionMock(sourceDb);
        var json = SchemaSnapshot.Export(sourceConnection).ToJson();

        using var document = JsonDocument.Parse(json);
        document.RootElement.TryGetProperty("schemas", out var schemasProperty).Should().BeTrue();
        schemasProperty.GetArrayLength().Should().Be(1);

        var targetDb = new SqliteDbMock();
        targetDb.AddTable("Legacy", [new("ObsoleteId", DbType.Int32, false)]);

        SchemaSnapshot.Load(json, targetDb);

        targetDb.TryGetTable("Legacy", out _).Should().BeFalse();
        targetDb.TryGetTable("Users", out var usersTable).Should().BeTrue();
        usersTable.Should().NotBeNull();
        usersTable!.Columns.Count.Should().Be(3);
        usersTable.NextIdentity.Should().Be(17);
        usersTable.Columns["Id"].Identity.Should().BeTrue();
        usersTable.Columns["Name"].Size.Should().Be(50);
        usersTable.Columns["Name"].DefaultValue.Should().Be("anonymous");
        usersTable.Columns["Amount"].DecimalPlaces.Should().Be(2);
        usersTable.Columns["Amount"].DefaultValue.Should().Be(10.5m);
    }

    /// <summary>
    /// EN: Verifies schema snapshot export and replay preserve view definitions and sequence state.
    /// PT: Verifica se a exportacao e o replay de schema snapshot preservam definicoes de view e estado de sequence.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void LoadAndApplyTo_ShouldPreserveViewsAndSequences()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false, size: 50)
        ]);
        sourceDb.AddSequence("seq_users", startValue: 10, incrementBy: 5, currentValue: 20);

        var viewQuery = SqlQueryParser.Parse(
            "CREATE VIEW active_users AS SELECT Id, Name FROM Users WHERE Id > 0",
            sourceDb,
            sourceDb.Dialect).Should().BeOfType<SqlCreateViewQuery>().Subject;
        sourceDb.AddView(viewQuery);

        using var sourceConnection = new SqliteConnectionMock(sourceDb);
        var snapshot = SchemaSnapshot.Export(sourceConnection);
        var schema = snapshot.Schemas.Should().ContainSingle().Subject;
        var exportedView = schema.Views.Should().ContainSingle().Subject;
        var exportedSequence = schema.Sequences.Should().ContainSingle().Subject;

        exportedView.Name.Should().BeEquivalentTo("active_users");
        exportedView.SelectSql.Should().Contain("SELECT Id, Name FROM Users WHERE Id > 0");
        exportedSequence.Name.Should().BeEquivalentTo("seq_users");
        exportedSequence.StartValue.Should().Be(10);
        exportedSequence.IncrementBy.Should().Be(5);
        exportedSequence.CurrentValue.Should().Be(20);

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        targetDb.TryGetView("active_users", out var replayedView).Should().BeTrue();
        replayedView.Should().NotBeNull();
        replayedView!.RawSql.Should().Contain("SELECT Id, Name FROM Users WHERE Id > 0");

        targetDb.TryGetSequence("seq_users", out var replayedSequence).Should().BeTrue();
        replayedSequence.Should().NotBeNull();
        replayedSequence!.StartValue.Should().Be(10);
        replayedSequence.IncrementBy.Should().Be(5);
        replayedSequence.CurrentValue.Should().Be(20);
    }

    /// <summary>
    /// EN: Verifies schema snapshot export and replay preserve primary keys, indexes, and foreign keys.
    /// PT: Verifica se a exportacao e o replay de schema snapshot preservam chaves primarias, indices e chaves estrangeiras.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void LoadAndApplyTo_ShouldPreserveIndexesAndConstraints()
    {
        var sourceDb = new SqliteDbMock();
        var users = sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Email", DbType.String, false, size: 100)
        ]);
        users.AddPrimaryKeyIndexes("Id");
        users.CreateIndex("ix_users_email", ["Email"], unique: true);

        var orders = sourceDb.AddTable("Orders", [
            new("OrderId", DbType.Int32, false),
            new("UserId", DbType.Int32, false),
            new("Amount", DbType.Decimal, false, decimalPlaces: 2)
        ]);
        orders.AddPrimaryKeyIndexes("OrderId");
        orders.CreateIndex("ix_orders_user_amount", ["UserId"], ["Amount"], unique: false);
        orders.CreateForeignKey("fk_orders_users", "Users", [("UserId", "Id")]);

        var snapshot = SchemaSnapshot.Export(sourceDb);
        var schema = snapshot.Schemas.Should().ContainSingle().Subject;
        var exportedUsers = schema.Tables.Should().ContainSingle(table => table.Name.Equals("users", StringComparison.OrdinalIgnoreCase)).Subject;
        var exportedOrders = schema.Tables.Should().ContainSingle(table => table.Name.Equals("orders", StringComparison.OrdinalIgnoreCase)).Subject;

        exportedUsers.PrimaryKeyColumns.Should().Equal(["Id"]);
        var usersIndex = exportedUsers.Indexes.Should().ContainSingle().Subject;
        usersIndex.Name.Should().BeEquivalentTo("ix_users_email");
        usersIndex.Unique.Should().BeTrue();
        usersIndex.KeyColumns.Should().Equal(["Email"]);

        exportedOrders.PrimaryKeyColumns.Should().Equal(["OrderId"]);
        var ordersIndex = exportedOrders.Indexes.Should().ContainSingle().Subject;
        ordersIndex.KeyColumns.Should().Equal(["UserId"]);
        ordersIndex.IncludeColumns.Should().Equal(["Amount"]);

        var foreignKey = exportedOrders.ForeignKeys.Should().ContainSingle().Subject;
        foreignKey.Name.Should().BeEquivalentTo("fk_orders_users");
        foreignKey.RefTableName.Should().BeEquivalentTo("users");
        var reference = foreignKey.References.Should().ContainSingle().Subject;
        reference.ColumnName.Should().BeEquivalentTo("UserId");
        reference.RefColumnName.Should().BeEquivalentTo("Id");

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        var replayedUsers = targetDb.GetTable("Users");
        var replayedOrders = targetDb.GetTable("Orders");

        replayedUsers.PrimaryKeyIndexes.Should().ContainSingle();
        replayedUsers.PrimaryKeyIndexes.Should().Contain(replayedUsers.Columns["Id"].Index);
        replayedUsers.Indexes.ContainsKey("ix_users_email").Should().BeTrue();
        replayedUsers.Indexes["ix_users_email"].Unique.Should().BeTrue();

        replayedOrders.PrimaryKeyIndexes.Should().ContainSingle();
        replayedOrders.PrimaryKeyIndexes.Should().Contain(replayedOrders.Columns["OrderId"].Index);
        replayedOrders.Indexes.ContainsKey("ix_orders_user_amount").Should().BeTrue();
        replayedOrders.Indexes["ix_orders_user_amount"].KeyCols.Should().Equal(["UserId"]);
        replayedOrders.Indexes["ix_orders_user_amount"].Include.Should().Equal(["Amount"]);
        replayedOrders.ForeignKeys.ContainsKey("fk_orders_users").Should().BeTrue();

        var replayedForeignKey = replayedOrders.ForeignKeys["fk_orders_users"];
        replayedForeignKey.RefTable.TableName.Should().BeEquivalentTo("users");
        var replayedReference = replayedForeignKey.References.Should().ContainSingle().Subject;
        replayedReference.col.Name.Should().BeEquivalentTo("UserId");
        replayedReference.refCol.Name.Should().BeEquivalentTo("Id");
    }

    /// <summary>
    /// EN: Verifies schema snapshot export and replay preserve stored procedure signatures.
    /// PT: Verifica se a exportacao e o replay de schema snapshot preservam assinaturas de procedures.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void LoadAndApplyTo_ShouldPreserveStoredProcedureSignatures()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.AddProdecure(

            new ProcedureDef(
                "usp_sync_user",
                RequiredIn: [new ProcParam("@userId", DbType.Int32)],
                OptionalIn: [new ProcParam("@status", DbType.String, Required: false, Value: "new")],
                OutParams: [new ProcParam("@affected", DbType.Int32, Required: false)],
                ReturnParam: new ProcParam("@returnValue", DbType.Int32, Required: false, Value: 0)));

        var snapshot = SchemaSnapshot.Export(sourceDb);
        var schema = snapshot.Schemas.Should().ContainSingle().Subject;
        var procedure = schema.Procedures.Should().ContainSingle().Subject;

        procedure.Name.Should().BeEquivalentTo("usp_sync_user");
        procedure.RequiredIn.Should().ContainSingle().Subject.Name.Should().BeEquivalentTo("@userId");
        procedure.OptionalIn.Should().ContainSingle().Subject.Value?.GetString().Should().Be("new");
        procedure.OutParams.Should().ContainSingle().Subject.Name.Should().BeEquivalentTo("@affected");
        procedure.ReturnParam?.Name.Should().BeEquivalentTo("@returnValue");

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        targetDb.TryGetProcedure("usp_sync_user", out var replayedProcedure).Should().BeTrue();
        replayedProcedure.Should().NotBeNull();
        replayedProcedure!.RequiredIn.Should().ContainSingle().Subject.Name.Should().BeEquivalentTo("@userId");
        replayedProcedure.OptionalIn.Should().ContainSingle().Subject.Value.Should().Be("new");
        replayedProcedure.OutParams.Should().ContainSingle().Subject.Name.Should().BeEquivalentTo("@affected");
        replayedProcedure.ReturnParam?.Name.Should().BeEquivalentTo("@returnValue");
        replayedProcedure.ReturnParam?.Value.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies schema snapshot export and replay preserve objects across multiple schemas.
    /// PT: Verifica se a exportacao e o replay de schema snapshot preservam objetos em multiplos schemas.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void LoadAndApplyTo_ShouldPreserveMultipleSchemas()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.CreateSchema("app");
        sourceDb.CreateSchema("reporting");
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false, size: 50)
        ], schemaName: "app");
        sourceDb.AddSequence("seq_report", startValue: 5, incrementBy: 2, currentValue: 9, schemaName: "reporting");
        sourceDb.AddProdecure(
            new ProcedureDef(
                "usp_refresh_report",
                RequiredIn: [new ProcParam("@reportId", DbType.Int32)],
                OptionalIn: [],
                OutParams: []),
            schemaName: "reporting");

        var viewQuery = SqlQueryParser.Parse(
            "CREATE VIEW active_users AS SELECT Id, Name FROM Users WHERE Id > 0",
            sourceDb,
            sourceDb.Dialect).Should().BeOfType<SqlCreateViewQuery>().Subject;
        sourceDb.AddView(viewQuery, "app");

        var snapshot = SchemaSnapshot.Export(sourceDb);

        snapshot.Schemas.Count.Should().Be(3);
        snapshot.Schemas.Should().Contain(schema => schema.Name.Equals("app"));
        snapshot.Schemas.Should().Contain(schema => schema.Name.Equals("reporting"));

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        targetDb.TryGetTable("Users", out var appUsers, "app").Should().BeTrue();
        appUsers.Should().NotBeNull();
        targetDb.TryGetView("active_users", out var appView, "app").Should().BeTrue();
        appView.Should().NotBeNull();

        targetDb.TryGetSequence("seq_report", out var reportingSequence, "reporting").Should().BeTrue();
        reportingSequence.Should().NotBeNull();
        reportingSequence!.CurrentValue.Should().Be(9);

        targetDb.TryGetProcedure("usp_refresh_report", out var reportingProcedure, "reporting").Should().BeTrue();
        reportingProcedure.Should().NotBeNull();
        reportingProcedure!.RequiredIn.Should().ContainSingle().Subject.Name.Should().BeEquivalentTo("@reportId");
    }

    /// <summary>
    /// EN: Verifies schema snapshot export and replay preserve cross-schema foreign key targets.
    /// PT: Verifica se a exportacao e o replay de schema snapshot preservam alvos de chave estrangeira entre schemas.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void LoadAndApplyTo_ShouldPreserveCrossSchemaForeignKeys()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.CreateSchema("app");
        sourceDb.CreateSchema("reporting");

        var users = sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false)
        ], schemaName: "app");
        users.AddPrimaryKeyIndexes("Id");

        var audits = sourceDb.AddTable("Audits", [
            new("AuditId", DbType.Int32, false),
            new("UserId", DbType.Int32, false)
        ], schemaName: "reporting");
        audits.AddPrimaryKeyIndexes("AuditId");
        audits.CreateForeignKey("fk_audits_users", "app.Users", [("UserId", "Id")]);

        var snapshot = SchemaSnapshot.Export(sourceDb);
        var reportingSchema = snapshot.Schemas.Should().ContainSingle(schema => schema.Name.Equals("reporting")).Subject;
        var auditsTable = reportingSchema.Tables.Should().ContainSingle(table => table.Name.Equals("audits", StringComparison.OrdinalIgnoreCase)).Subject;
        var foreignKey = auditsTable.ForeignKeys.Should().ContainSingle().Subject;

        foreignKey.RefTableName.Should().BeEquivalentTo("users");
        foreignKey.RefSchemaName.Should().BeEquivalentTo("app");

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        var replayedAudits = targetDb.GetTable("Audits", "reporting");
        var replayedForeignKey = replayedAudits.ForeignKeys["fk_audits_users"];

        replayedForeignKey.RefTable.TableName.Should().BeEquivalentTo("users");
        replayedForeignKey.RefTable.Schema.SchemaName.Should().BeEquivalentTo("app");
        var replayedReference = replayedForeignKey.References.Should().ContainSingle().Subject;
        replayedReference.col.Name.Should().BeEquivalentTo("UserId");
        replayedReference.refCol.Name.Should().BeEquivalentTo("Id");
    }

    /// <summary>
    /// EN: Verifies connection-level schema snapshot helpers can export and import the current database structure.
    /// PT: Verifica se os helpers de schema snapshot na conexao conseguem exportar e importar a estrutura atual do banco.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void ConnectionHelpers_ShouldRoundTripSchemaSnapshot()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false, size: 50)
        ]);

        using var sourceConnection = new SqliteConnectionMock(sourceDb);
        var snapshot = sourceConnection.ExportSchemaSnapshot();
        var json = sourceConnection.ExportSchemaSnapshotJson();

        snapshot.Schemas.Should().ContainSingle();
        json.Should().Contain("\"schemas\"");

        var targetDb = new SqliteDbMock();
        targetDb.AddTable("Legacy", [new("Id", DbType.Int32, false)]);

        using var targetConnection = new SqliteConnectionMock(targetDb);
        targetConnection.ImportSchemaSnapshot(json);

        targetConnection.TryGetTable("Legacy", out _).Should().BeFalse();
        targetConnection.TryGetTable("Users", out var usersTable).Should().BeTrue();
        usersTable.Should().NotBeNull();
        usersTable!.Columns.Count.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies connection import realigns the current database when the previous schema no longer exists after replay.
    /// PT: Verifica se o import pela conexao realinha o database atual quando o schema anterior deixa de existir apos o replay.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void ConnectionHelpers_ShouldRealignCurrentDatabaseAfterSchemaReplacement()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.CreateSchema("app");
        sourceDb.CreateSchema("reporting");
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false)
        ], schemaName: "app");

        using var sourceConnection = new SqliteConnectionMock(sourceDb);
        var json = sourceConnection.ExportSchemaSnapshotJson();

        var targetDb = new SqliteDbMock();
        targetDb.CreateSchema("legacy");
        using var targetConnection = new SqliteConnectionMock(targetDb, "legacy");

        targetConnection.Database.Should().BeEquivalentTo("legacy");

        targetConnection.ImportSchemaSnapshot(json);

        targetConnection.Database.Should().BeEquivalentTo("app");
        targetConnection.TryGetTable("Users", out var usersTable, "app").Should().BeTrue();
        usersTable.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies schema snapshot compatibility checks use dialect equality and minimum target version.
    /// PT: Verifica se as checagens de compatibilidade do schema snapshot usam igualdade de dialeto e versao minima no destino.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void CompatibilityHelpers_ShouldValidateDialectAndVersion()
    {
        var snapshot = SchemaSnapshot.Export(new SqliteDbMock(version: 5));

        snapshot.IsCompatibleWith(new SqliteDbMock(version: 5)).Should().BeTrue();
        snapshot.IsCompatibleWith(new SqliteDbMock(version: 6)).Should().BeTrue();
        snapshot.IsCompatibleWith(new SqliteDbMock(version: 4)).Should().BeFalse();

        var dialectMismatchSnapshot = snapshot with { DialectName = "mysql" };
        dialectMismatchSnapshot.IsCompatibleWith(new SqliteDbMock(version: 6)).Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies strict compatibility validation rejects replay against an incompatible target connection before applying changes.
    /// PT: Verifica se a validacao estrita de compatibilidade rejeita o replay contra uma conexao de destino incompativel antes de aplicar mudancas.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void ConnectionHelpers_ShouldRejectStrictImportWhenSnapshotIsIncompatible()
    {
        var sourceDb = new SqliteDbMock(version: 5);
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false)
        ]);

        var targetDb = new SqliteDbMock(version: 4);
        targetDb.AddTable("Legacy", [new("Id", DbType.Int32, false)]);

        using var targetConnection = new SqliteConnectionMock(targetDb);
        var json = SchemaSnapshot.Export(sourceDb).ToJson();

        var ex = ((Action)(() => targetConnection.ImportSchemaSnapshot(json, ensureCompatibility: true))).Should().Throw<InvalidOperationException>().Which;
        ex.Message.Should().Contain("target version is 4");
        targetConnection.TryGetTable("Legacy", out var legacyTable).Should().BeTrue();
        legacyTable.Should().NotBeNull();
        targetConnection.TryGetTable("Users", out _).Should().BeFalse();

        var mysqlSnapshot = SchemaSnapshot.Export(sourceDb) with { DialectName = "mysql" };
        ex = ((Action)(() => targetConnection.ImportSchemaSnapshot(mysqlSnapshot.ToJson(), ensureCompatibility: true))).Should().Throw<InvalidOperationException>().Which;
        ex.Message.Should().Contain("not compatible");
    }

    /// <summary>
    /// EN: Verifies strict compatibility validation also works when replay targets the raw mock database API.
    /// PT: Verifica se a validacao estrita de compatibilidade tambem funciona quando o replay usa a API crua de DbMock.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void DbMockHelpers_ShouldRejectStrictImportWhenSnapshotIsIncompatible()
    {
        var sourceDb = new SqliteDbMock(version: 5);
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false)
        ]);

        var json = SchemaSnapshot.Export(sourceDb).ToJson();

        var targetDb = new SqliteDbMock(version: 4);
        targetDb.AddTable("Legacy", [new("Id", DbType.Int32, false)]);

        var ex = ((Action)(() => SchemaSnapshot.Load(json, targetDb, ensureCompatibility: true))).Should().Throw<InvalidOperationException>().Which;
        ex.Message.Should().Contain("target version is 4");
        targetDb.TryGetTable("Legacy", out var legacyTable).Should().BeTrue();
        legacyTable.Should().NotBeNull();
        targetDb.TryGetTable("Users", out _).Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies snapshot fingerprints stay stable for equivalent structures and change when the schema drifts.
    /// PT: Verifica se as assinaturas do snapshot permanecem estaveis para estruturas equivalentes e mudam quando o schema deriva.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void FingerprintHelpers_ShouldDetectEquivalentAndDriftedStructures()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false, size: 50)
        ]);

        var snapshot = SchemaSnapshot.Export(sourceDb);
        var sameStructureDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), sameStructureDb);

        snapshot.Matches(SchemaSnapshot.Export(sameStructureDb)).Should().BeTrue();
        snapshot.Matches(sameStructureDb).Should().BeTrue();

        using var sameStructureConnection = new SqliteConnectionMock(sameStructureDb);
        snapshot.Matches(sameStructureConnection).Should().BeTrue();
        snapshot.GetFingerprint().Should().Be(SchemaSnapshot.Export(sameStructureDb).GetFingerprint());

        sameStructureDb.GetTable("Users").CreateIndex("ix_users_name", ["Name"], unique: false);

        snapshot.Matches(sameStructureDb).Should().BeFalse();
        snapshot.GetFingerprint().Should().NotBe(SchemaSnapshot.Export(sameStructureDb).GetFingerprint());
    }

    /// <summary>
    /// EN: Verifies snapshot comparisons report structured drift details for diagnostics.
    /// PT: Verifica se as comparacoes de snapshot reportam detalhes estruturados de drift para diagnostico.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void ComparisonHelpers_ShouldReportStructuredDriftDetails()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false, size: 50)
        ]);

        var snapshot = SchemaSnapshot.Export(sourceDb);

        var targetDb = new SqliteDbMock();
        targetDb.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("DisplayName", DbType.String, false, size: 50)
        ]);
        targetDb.AddTable("Orders", [
            new("OrderId", DbType.Int32, false)
        ]);

        var comparison = snapshot.CompareTo(targetDb);

        comparison.IsMatch.Should().BeFalse();
        comparison.Differences.Should().NotBeEmpty();
        comparison.Differences.Should().Contain(difference => difference.Contains("Table 'Users'"));
        comparison.Differences.Should().Contain(difference => difference.Contains("Table in schema 'DefaultSchema' only in target: 'Orders'"));

        var text = comparison.ToText();
        text.Should().Contain("IsMatch: False");
        text.Should().Contain("Differences:");
    }

    /// <summary>
    /// EN: Verifies the support profile explicitly describes the current snapshot subset and known out-of-scope metadata.
    /// PT: Verifica se o perfil de suporte descreve explicitamente o subset atual do snapshot e os metadados conhecidos fora de escopo.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void SupportProfile_ShouldDescribeCurrentSubsetAndKnownGaps()
    {
        var profile = SchemaSnapshot.GetSupportProfile(new SqliteDbMock(version: 6));

        profile.DialectName.Should().BeEquivalentTo("sqlite");
        profile.Version.Should().Be(6);
        profile.SupportedObjects.Should().Contain("tables");
        profile.SupportedObjects.Should().Contain("views");
        profile.SupportedObjects.Should().Contain("procedure-signatures");
        profile.UnsupportedObjects.Should().Contain("trigger-bodies");
        profile.UnsupportedObjects.Should().Contain("computed-default-expressions");

        var text = profile.ToText();
        text.Should().Contain("SupportedObjects:");
        text.Should().Contain("UnsupportedObjects:");

        using var connection = new SqliteConnectionMock(new SqliteDbMock(version: 6));
        var connectionProfile = connection.GetSchemaSnapshotSupportProfile();

        connectionProfile.DialectName.Should().BeEquivalentTo("sqlite");
        connectionProfile.Version.Should().Be(6);
        connectionProfile.SupportedObjects.Should().Contain("tables");
        connectionProfile.UnsupportedObjects.Should().Contain("trigger-bodies");

        var connectionText = connection.GetSchemaSnapshotSupportProfileText();
        connectionText.Should().Contain("SupportedObjects:");
        connectionText.Should().Contain("UnsupportedObjects:");
    }

    /// <summary>
    /// EN: Verifies a full export replay export cycle remains drift-free for the supported schema snapshot subset.
    /// PT: Verifica se um ciclo completo de exportacao replay exportacao permanece sem drift para o subset suportado do schema snapshot.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void EndToEndReplay_ShouldRemainDriftFreeForSupportedSubset()
    {
        var sourceDb = new SqliteDbMock(version: 6);
        sourceDb.CreateSchema("app");
        sourceDb.CreateSchema("reporting");

        var users = sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false, identity: true),
            new("Name", DbType.String, false, size: 50, defaultValue: "anonymous")
        ], schemaName: "app");
        users.AddPrimaryKeyIndexes("Id");
        users.CreateIndex("ix_users_name", ["Name"], unique: false);
        users.NextIdentity = 11;

        var reports = sourceDb.AddTable("Reports", [
            new("ReportId", DbType.Int32, false),
            new("UserId", DbType.Int32, false)
        ], schemaName: "reporting");
        reports.AddPrimaryKeyIndexes("ReportId");
        reports.CreateForeignKey("fk_reports_users", "app.Users", [("UserId", "Id")]);

        sourceDb.AddSequence("seq_reports", startValue: 100, incrementBy: 10, currentValue: 120, schemaName: "reporting");
        sourceDb.AddProdecure(
            new ProcedureDef(
                "usp_refresh_report",
                RequiredIn: [new ProcParam("@reportId", DbType.Int32)],
                OptionalIn: [new ProcParam("@mode", DbType.String, Required: false, Value: "full")],
                OutParams: []),
            schemaName: "reporting");

        var viewQuery = SqlQueryParser.Parse(
            "CREATE VIEW active_users AS SELECT Id, Name FROM Users WHERE Id > 0",
            sourceDb,
            sourceDb.Dialect).Should().BeOfType<SqlCreateViewQuery>().Subject;
        sourceDb.AddView(viewQuery, "app");

        var snapshot = SchemaSnapshot.Export(sourceDb);
        var targetDb = new SqliteDbMock(version: 6);

        snapshot.ApplyTo(targetDb, ensureCompatibility: true);

        var replayedSnapshot = SchemaSnapshot.Export(targetDb);
        var comparison = snapshot.CompareTo(replayedSnapshot);

        comparison.IsMatch.Should().BeTrue();
        comparison.Differences.Should().BeEmpty();
        snapshot.GetFingerprint().Should().Be(replayedSnapshot.GetFingerprint());
    }

    /// <summary>
    /// EN: Verifies snapshot-first helpers can apply directly to a connection without reserializing through the caller.
    /// PT: Verifica se os helpers orientados a snapshot conseguem aplicar diretamente em uma conexao sem reserializar pelo chamador.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void SnapshotFirstHelpers_ShouldApplyDirectlyToConnection()
    {
        var snapshot = SchemaSnapshot.Export(new SqliteDbMock(version: 5) { })
            with
        {
            Schemas =
                [
                    new SchemaSnapshotSchema
                    {
                        Name = "app",
                        Tables =
                        [
                            new SchemaSnapshotTable
                            {
                                Name = "Users",
                                NextIdentity = 3,
                                Columns =
                                [
                                    new SchemaSnapshotColumn
                                    {
                                        Name = "Id",
                                        DbType = DbType.Int32,
                                        Nullable = false,
                                        Identity = true
                                    }
                                ],
                                PrimaryKeyColumns = ["Id"],
                                Indexes = [],
                                ForeignKeys = []
                            }
                        ],
                        Views = [],
                        Procedures = [],
                        Sequences = []
                    }
                ]
        };

        var targetDb = new SqliteDbMock(version: 6);
        targetDb.CreateSchema("legacy");
        using var targetConnection = new SqliteConnectionMock(targetDb, "legacy");

        snapshot.IsCompatibleWith(targetConnection).Should().BeTrue();

        snapshot.ApplyTo(targetConnection, ensureCompatibility: true);

        targetConnection.Database.Should().BeEquivalentTo("app");
        targetConnection.TryGetTable("Users", out var usersTable, "app").Should().BeTrue();
        usersTable.Should().NotBeNull();
        usersTable!.NextIdentity.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies file-based schema snapshot helpers can persist and reload versionable JSON fixtures.
    /// PT: Verifica se os helpers de arquivo de schema snapshot conseguem persistir e recarregar fixtures JSON versionaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void FileHelpers_ShouldRoundTripSchemaSnapshot()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false, identity: true),
            new("Name", DbType.String, false, size: 50, defaultValue: "anonymous")
        ]);
        sourceDb.GetTable("Users").NextIdentity = 9;

        var path = Path.Combine(Path.GetTempPath(), $"{nameof(SchemaSnapshotTests)}-{Guid.NewGuid():N}.json");

        try
        {
            SchemaSnapshot.Export(sourceDb).SaveToFile(path);

            File.Exists(path).Should().BeTrue();

            var fromFile = SchemaSnapshot.LoadFromFile(path);
            fromFile.Schemas.Should().ContainSingle();

            var targetDb = new SqliteDbMock();
            targetDb.AddTable("Legacy", [new("Id", DbType.Int32, false)]);

            SchemaSnapshot.LoadFromFile(path, targetDb);

            targetDb.TryGetTable("Legacy", out _).Should().BeFalse();
            var users = targetDb.GetTable("Users");
            users.NextIdentity.Should().Be(9);
            users.Columns["Name"].DefaultValue.Should().Be("anonymous");
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    /// <summary>
    /// EN: Verifies connection-level file helpers can export and import schema snapshots through JSON files.
    /// PT: Verifica se os helpers de arquivo na conexao conseguem exportar e importar schema snapshots por arquivos JSON.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void ConnectionFileHelpers_ShouldRoundTripSchemaSnapshot()
    {
        var sourceDb = new SqliteDbMock();
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false, size: 50)
        ]);

        var path = Path.Combine(Path.GetTempPath(), $"{nameof(SchemaSnapshotTests)}-connection-{Guid.NewGuid():N}.json");

        try
        {
            using (var sourceConnection = new SqliteConnectionMock(sourceDb))
                sourceConnection.ExportSchemaSnapshotToFile(path);

            var json = File.ReadAllText(path);
            json.Should().Contain("\"schemas\"");

            var targetDb = new SqliteDbMock();
            targetDb.AddTable("Legacy", [new("Id", DbType.Int32, false)]);

            using var targetConnection = new SqliteConnectionMock(targetDb);
            targetConnection.ImportSchemaSnapshotFromFile(path);

            targetConnection.TryGetTable("Legacy", out _).Should().BeFalse();
            targetConnection.TryGetTable("Users", out var usersTable).Should().BeTrue();
            usersTable.Should().NotBeNull();
            usersTable!.Columns.Count.Should().Be(2);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    /// <summary>
    /// EN: Verifies static snapshot loaders can apply directly to a connection from JSON text and file paths.
    /// PT: Verifica se os loaders estaticos de snapshot conseguem aplicar diretamente em uma conexao a partir de texto JSON e caminhos de arquivo.
    /// </summary>
    [Fact]
    [Trait("Category", "SchemaSnapshot")]
    public void StaticLoaders_ShouldApplyDirectlyToConnection()
    {
        var sourceDb = new SqliteDbMock(version: 5);
        sourceDb.AddTable("Users", [
            new("Id", DbType.Int32, false)
        ]);

        var json = SchemaSnapshot.Export(sourceDb).ToJson();
        var path = Path.Combine(Path.GetTempPath(), $"{nameof(SchemaSnapshotTests)}-static-load-{Guid.NewGuid():N}.json");

        try
        {
            File.WriteAllText(path, json);

            using var jsonConnection = new SqliteConnectionMock(new SqliteDbMock(version: 6));
            SchemaSnapshot.Load(json, jsonConnection, ensureCompatibility: true);
            jsonConnection.TryGetTable("Users", out var jsonUsers).Should().BeTrue();
            jsonUsers.Should().NotBeNull();

            using var fileConnection = new SqliteConnectionMock(new SqliteDbMock(version: 6));
            SchemaSnapshot.LoadFromFile(path, fileConnection, ensureCompatibility: true);
            fileConnection.TryGetTable("Users", out var fileUsers).Should().BeTrue();
            fileUsers.Should().NotBeNull();
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }
}

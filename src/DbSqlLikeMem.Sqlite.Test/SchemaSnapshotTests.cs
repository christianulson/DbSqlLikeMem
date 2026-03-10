using System.Text.Json;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Covers the first schema snapshot export/load slice over the SQLite mock surface.
/// PT: Cobre a primeira fatia de exportacao/carga de schema snapshot sobre a superficie do mock SQLite.
/// </summary>
public sealed class SchemaSnapshotTests
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

        Assert.Equal("sqlite", snapshot.DialectName, ignoreCase: true);
        Assert.Equal(db.Version, snapshot.Version);

        var schema = Assert.Single(snapshot.Schemas);
        Assert.Equal("DefaultSchema", schema.Name, ignoreCase: true);

        var exportedTable = Assert.Single(schema.Tables);
        Assert.Equal("users", exportedTable.Name, ignoreCase: true);
        Assert.Equal(42, exportedTable.NextIdentity);
        Assert.Equal(4, exportedTable.Columns.Count);

        var id = exportedTable.Columns[0];
        Assert.Equal("Id", id.Name, ignoreCase: true);
        Assert.True(id.Identity);
        Assert.Equal(DbType.Int32, id.DbType);

        var amount = exportedTable.Columns[2];
        Assert.Equal(DbType.Decimal, amount.DbType);
        Assert.Equal(2, amount.DecimalPlaces);
        Assert.True(amount.DefaultValue.HasValue);

        var status = exportedTable.Columns[3];
        Assert.Equal(["done", "new"], status.EnumValues);
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
        Assert.True(document.RootElement.TryGetProperty("schemas", out var schemasProperty));
        Assert.Equal(1, schemasProperty.GetArrayLength());

        var targetDb = new SqliteDbMock();
        targetDb.AddTable("Legacy", [new("ObsoleteId", DbType.Int32, false)]);

        SchemaSnapshot.Load(json, targetDb);

        Assert.False(targetDb.TryGetTable("Legacy", out _));
        Assert.True(targetDb.TryGetTable("Users", out var usersTable));
        Assert.NotNull(usersTable);
        Assert.Equal(3, usersTable!.Columns.Count);
        Assert.Equal(17, usersTable.NextIdentity);
        Assert.True(usersTable.Columns["Id"].Identity);
        Assert.Equal(50, usersTable.Columns["Name"].Size);
        Assert.Equal("anonymous", usersTable.Columns["Name"].DefaultValue);
        Assert.Equal(2, usersTable.Columns["Amount"].DecimalPlaces);
        Assert.Equal(10.5m, usersTable.Columns["Amount"].DefaultValue);
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

        var viewQuery = Assert.IsType<SqlCreateViewQuery>(SqlQueryParser.Parse(
            "CREATE VIEW active_users AS SELECT Id, Name FROM Users WHERE Id > 0",
            sourceDb.Dialect));
        sourceDb.AddView(viewQuery);

        using var sourceConnection = new SqliteConnectionMock(sourceDb);
        var snapshot = SchemaSnapshot.Export(sourceConnection);
        var schema = Assert.Single(snapshot.Schemas);
        var exportedView = Assert.Single(schema.Views);
        var exportedSequence = Assert.Single(schema.Sequences);

        Assert.Equal("active_users", exportedView.Name, ignoreCase: true);
        Assert.Contains("SELECT Id, Name FROM Users WHERE Id > 0", exportedView.SelectSql, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("seq_users", exportedSequence.Name, ignoreCase: true);
        Assert.Equal(10, exportedSequence.StartValue);
        Assert.Equal(5, exportedSequence.IncrementBy);
        Assert.Equal(20, exportedSequence.CurrentValue);

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        Assert.True(targetDb.TryGetView("active_users", out var replayedView));
        Assert.NotNull(replayedView);
        Assert.Contains("SELECT Id, Name FROM Users WHERE Id > 0", replayedView!.RawSql, StringComparison.OrdinalIgnoreCase);

        Assert.True(targetDb.TryGetSequence("seq_users", out var replayedSequence));
        Assert.NotNull(replayedSequence);
        Assert.Equal(10, replayedSequence!.StartValue);
        Assert.Equal(5, replayedSequence.IncrementBy);
        Assert.Equal(20, replayedSequence.CurrentValue);
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
        var schema = Assert.Single(snapshot.Schemas);
        var exportedUsers = Assert.Single(schema.Tables, table => table.Name.Equals("users", StringComparison.OrdinalIgnoreCase));
        var exportedOrders = Assert.Single(schema.Tables, table => table.Name.Equals("orders", StringComparison.OrdinalIgnoreCase));

        Assert.Equal(["Id"], exportedUsers.PrimaryKeyColumns);
        var usersIndex = Assert.Single(exportedUsers.Indexes);
        Assert.Equal("ix_users_email", usersIndex.Name, ignoreCase: true);
        Assert.True(usersIndex.Unique);
        Assert.Equal(["Email"], usersIndex.KeyColumns);

        Assert.Equal(["OrderId"], exportedOrders.PrimaryKeyColumns);
        var ordersIndex = Assert.Single(exportedOrders.Indexes);
        Assert.Equal(["UserId"], ordersIndex.KeyColumns);
        Assert.Equal(["Amount"], ordersIndex.IncludeColumns);

        var foreignKey = Assert.Single(exportedOrders.ForeignKeys);
        Assert.Equal("fk_orders_users", foreignKey.Name, ignoreCase: true);
        Assert.Equal("users", foreignKey.RefTableName, ignoreCase: true);
        var reference = Assert.Single(foreignKey.References);
        Assert.Equal("UserId", reference.ColumnName, ignoreCase: true);
        Assert.Equal("Id", reference.RefColumnName, ignoreCase: true);

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        var replayedUsers = targetDb.GetTable("Users");
        var replayedOrders = targetDb.GetTable("Orders");

        Assert.Single(replayedUsers.PrimaryKeyIndexes);
        Assert.Contains(replayedUsers.Columns["Id"].Index, replayedUsers.PrimaryKeyIndexes);
        Assert.True(replayedUsers.Indexes.ContainsKey("ix_users_email"));
        Assert.True(replayedUsers.Indexes["ix_users_email"].Unique);

        Assert.Single(replayedOrders.PrimaryKeyIndexes);
        Assert.Contains(replayedOrders.Columns["OrderId"].Index, replayedOrders.PrimaryKeyIndexes);
        Assert.True(replayedOrders.Indexes.ContainsKey("ix_orders_user_amount"));
        Assert.Equal(["UserId"], replayedOrders.Indexes["ix_orders_user_amount"].KeyCols);
        Assert.Equal(["Amount"], replayedOrders.Indexes["ix_orders_user_amount"].Include);
        Assert.True(replayedOrders.ForeignKeys.ContainsKey("fk_orders_users"));

        var replayedForeignKey = replayedOrders.ForeignKeys["fk_orders_users"];
        Assert.Equal("users", replayedForeignKey.RefTable.TableName, ignoreCase: true);
        var replayedReference = Assert.Single(replayedForeignKey.References);
        Assert.Equal("UserId", replayedReference.col.Name, ignoreCase: true);
        Assert.Equal("Id", replayedReference.refCol.Name, ignoreCase: true);
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
            "usp_sync_user",
            new ProcedureDef(
                RequiredIn: [new ProcParam("@userId", DbType.Int32)],
                OptionalIn: [new ProcParam("@status", DbType.String, Required: false, Value: "new")],
                OutParams: [new ProcParam("@affected", DbType.Int32, Required: false)],
                ReturnParam: new ProcParam("@returnValue", DbType.Int32, Required: false, Value: 0)));

        var snapshot = SchemaSnapshot.Export(sourceDb);
        var schema = Assert.Single(snapshot.Schemas);
        var procedure = Assert.Single(schema.Procedures);

        Assert.Equal("usp_sync_user", procedure.Name, ignoreCase: true);
        Assert.Equal("@userId", Assert.Single(procedure.RequiredIn).Name, ignoreCase: true);
        Assert.Equal("new", Assert.Single(procedure.OptionalIn).Value?.GetString());
        Assert.Equal("@affected", Assert.Single(procedure.OutParams).Name, ignoreCase: true);
        Assert.Equal("@returnValue", procedure.ReturnParam?.Name, ignoreCase: true);

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        Assert.True(targetDb.TryGetProcedure("usp_sync_user", out var replayedProcedure));
        Assert.NotNull(replayedProcedure);
        Assert.Equal("@userId", Assert.Single(replayedProcedure!.RequiredIn).Name, ignoreCase: true);
        Assert.Equal("new", Assert.Single(replayedProcedure.OptionalIn).Value);
        Assert.Equal("@affected", Assert.Single(replayedProcedure.OutParams).Name, ignoreCase: true);
        Assert.Equal("@returnValue", replayedProcedure.ReturnParam?.Name, ignoreCase: true);
        Assert.Equal(0, replayedProcedure.ReturnParam?.Value);
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
            "usp_refresh_report",
            new ProcedureDef(
                RequiredIn: [new ProcParam("@reportId", DbType.Int32)],
                OptionalIn: [],
                OutParams: []),
            schemaName: "reporting");

        var viewQuery = Assert.IsType<SqlCreateViewQuery>(SqlQueryParser.Parse(
            "CREATE VIEW active_users AS SELECT Id, Name FROM Users WHERE Id > 0",
            sourceDb.Dialect));
        sourceDb.AddView(viewQuery, "app");

        var snapshot = SchemaSnapshot.Export(sourceDb);

        Assert.Equal(3, snapshot.Schemas.Count);
        Assert.Contains(snapshot.Schemas, schema => schema.Name.Equals("app", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(snapshot.Schemas, schema => schema.Name.Equals("reporting", StringComparison.OrdinalIgnoreCase));

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        Assert.True(targetDb.TryGetTable("Users", out var appUsers, "app"));
        Assert.NotNull(appUsers);
        Assert.True(targetDb.TryGetView("active_users", out var appView, "app"));
        Assert.NotNull(appView);

        Assert.True(targetDb.TryGetSequence("seq_report", out var reportingSequence, "reporting"));
        Assert.NotNull(reportingSequence);
        Assert.Equal(9, reportingSequence!.CurrentValue);

        Assert.True(targetDb.TryGetProcedure("usp_refresh_report", out var reportingProcedure, "reporting"));
        Assert.NotNull(reportingProcedure);
        Assert.Equal("@reportId", Assert.Single(reportingProcedure!.RequiredIn).Name, ignoreCase: true);
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
        var reportingSchema = Assert.Single(snapshot.Schemas, schema => schema.Name.Equals("reporting", StringComparison.OrdinalIgnoreCase));
        var auditsTable = Assert.Single(reportingSchema.Tables, table => table.Name.Equals("audits", StringComparison.OrdinalIgnoreCase));
        var foreignKey = Assert.Single(auditsTable.ForeignKeys);

        Assert.Equal("users", foreignKey.RefTableName, ignoreCase: true);
        Assert.Equal("app", foreignKey.RefSchemaName, ignoreCase: true);

        var targetDb = new SqliteDbMock();
        SchemaSnapshot.Load(snapshot.ToJson(), targetDb);

        var replayedAudits = targetDb.GetTable("Audits", "reporting");
        var replayedForeignKey = replayedAudits.ForeignKeys["fk_audits_users"];

        Assert.Equal("users", replayedForeignKey.RefTable.TableName, ignoreCase: true);
        Assert.Equal("app", replayedForeignKey.RefTable.Schema.SchemaName, ignoreCase: true);
        var replayedReference = Assert.Single(replayedForeignKey.References);
        Assert.Equal("UserId", replayedReference.col.Name, ignoreCase: true);
        Assert.Equal("Id", replayedReference.refCol.Name, ignoreCase: true);
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

        Assert.Single(snapshot.Schemas);
        Assert.Contains("\"schemas\"", json, StringComparison.OrdinalIgnoreCase);

        var targetDb = new SqliteDbMock();
        targetDb.AddTable("Legacy", [new("Id", DbType.Int32, false)]);

        using var targetConnection = new SqliteConnectionMock(targetDb);
        targetConnection.ImportSchemaSnapshot(json);

        Assert.False(targetConnection.TryGetTable("Legacy", out _));
        Assert.True(targetConnection.TryGetTable("Users", out var usersTable));
        Assert.NotNull(usersTable);
        Assert.Equal(2, usersTable!.Columns.Count);
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

        Assert.Equal("legacy", targetConnection.Database, ignoreCase: true);

        targetConnection.ImportSchemaSnapshot(json);

        Assert.Equal("app", targetConnection.Database, ignoreCase: true);
        Assert.True(targetConnection.TryGetTable("Users", out var usersTable, "app"));
        Assert.NotNull(usersTable);
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

        Assert.True(snapshot.IsCompatibleWith(new SqliteDbMock(version: 5)));
        Assert.True(snapshot.IsCompatibleWith(new SqliteDbMock(version: 6)));
        Assert.False(snapshot.IsCompatibleWith(new SqliteDbMock(version: 4)));

        var dialectMismatchSnapshot = snapshot with { DialectName = "mysql" };
        Assert.False(dialectMismatchSnapshot.IsCompatibleWith(new SqliteDbMock(version: 6)));
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

        var ex = Assert.Throws<InvalidOperationException>(() => targetConnection.ImportSchemaSnapshot(json, ensureCompatibility: true));
        Assert.Contains("target version is 4", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(targetConnection.TryGetTable("Legacy", out var legacyTable));
        Assert.NotNull(legacyTable);
        Assert.False(targetConnection.TryGetTable("Users", out _));

        var mysqlSnapshot = SchemaSnapshot.Export(sourceDb) with { DialectName = "mysql" };
        ex = Assert.Throws<InvalidOperationException>(() => targetConnection.ImportSchemaSnapshot(mysqlSnapshot.ToJson(), ensureCompatibility: true));
        Assert.Contains("not compatible", ex.Message, StringComparison.OrdinalIgnoreCase);
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

        var ex = Assert.Throws<InvalidOperationException>(() => SchemaSnapshot.Load(json, targetDb, ensureCompatibility: true));
        Assert.Contains("target version is 4", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(targetDb.TryGetTable("Legacy", out var legacyTable));
        Assert.NotNull(legacyTable);
        Assert.False(targetDb.TryGetTable("Users", out _));
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

        Assert.True(snapshot.Matches(SchemaSnapshot.Export(sameStructureDb)));
        Assert.True(snapshot.Matches(sameStructureDb));

        using var sameStructureConnection = new SqliteConnectionMock(sameStructureDb);
        Assert.True(snapshot.Matches(sameStructureConnection));
        Assert.Equal(snapshot.GetFingerprint(), SchemaSnapshot.Export(sameStructureDb).GetFingerprint());

        sameStructureDb.GetTable("Users").CreateIndex("ix_users_name", ["Name"], unique: false);

        Assert.False(snapshot.Matches(sameStructureDb));
        Assert.NotEqual(snapshot.GetFingerprint(), SchemaSnapshot.Export(sameStructureDb).GetFingerprint());
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

        Assert.False(comparison.IsMatch);
        Assert.NotEmpty(comparison.Differences);
        Assert.Contains(comparison.Differences, difference => difference.Contains("Table 'Users'", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(comparison.Differences, difference => difference.Contains("Table in schema 'defaultschema' only in target: 'orders'", StringComparison.OrdinalIgnoreCase));

        var text = comparison.ToText();
        Assert.Contains("IsMatch: False", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Differences:", text, StringComparison.OrdinalIgnoreCase);
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

        Assert.Equal("sqlite", profile.DialectName, ignoreCase: true);
        Assert.Equal(6, profile.Version);
        Assert.Contains("tables", profile.SupportedObjects);
        Assert.Contains("views", profile.SupportedObjects);
        Assert.Contains("procedure-signatures", profile.SupportedObjects);
        Assert.Contains("trigger-bodies", profile.UnsupportedObjects);
        Assert.Contains("computed-default-expressions", profile.UnsupportedObjects);

        var text = profile.ToText();
        Assert.Contains("SupportedObjects:", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("UnsupportedObjects:", text, StringComparison.OrdinalIgnoreCase);

        using var connection = new SqliteConnectionMock(new SqliteDbMock(version: 6));
        var connectionProfile = connection.GetSchemaSnapshotSupportProfile();

        Assert.Equal("sqlite", connectionProfile.DialectName, ignoreCase: true);
        Assert.Equal(6, connectionProfile.Version);
        Assert.Contains("tables", connectionProfile.SupportedObjects);
        Assert.Contains("trigger-bodies", connectionProfile.UnsupportedObjects);

        var connectionText = connection.GetSchemaSnapshotSupportProfileText();
        Assert.Contains("SupportedObjects:", connectionText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("UnsupportedObjects:", connectionText, StringComparison.OrdinalIgnoreCase);
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
            "usp_refresh_report",
            new ProcedureDef(
                RequiredIn: [new ProcParam("@reportId", DbType.Int32)],
                OptionalIn: [new ProcParam("@mode", DbType.String, Required: false, Value: "full")],
                OutParams: []),
            schemaName: "reporting");

        var viewQuery = Assert.IsType<SqlCreateViewQuery>(SqlQueryParser.Parse(
            "CREATE VIEW active_users AS SELECT Id, Name FROM Users WHERE Id > 0",
            sourceDb.Dialect));
        sourceDb.AddView(viewQuery, "app");

        var snapshot = SchemaSnapshot.Export(sourceDb);
        var targetDb = new SqliteDbMock(version: 6);

        snapshot.ApplyTo(targetDb, ensureCompatibility: true);

        var replayedSnapshot = SchemaSnapshot.Export(targetDb);
        var comparison = snapshot.CompareTo(replayedSnapshot);

        Assert.True(comparison.IsMatch);
        Assert.Empty(comparison.Differences);
        Assert.Equal(snapshot.GetFingerprint(), replayedSnapshot.GetFingerprint());
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

        Assert.True(snapshot.IsCompatibleWith(targetConnection));

        snapshot.ApplyTo(targetConnection, ensureCompatibility: true);

        Assert.Equal("app", targetConnection.Database, ignoreCase: true);
        Assert.True(targetConnection.TryGetTable("Users", out var usersTable, "app"));
        Assert.NotNull(usersTable);
        Assert.Equal(3, usersTable!.NextIdentity);
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

            Assert.True(File.Exists(path));

            var fromFile = SchemaSnapshot.LoadFromFile(path);
            Assert.Single(fromFile.Schemas);

            var targetDb = new SqliteDbMock();
            targetDb.AddTable("Legacy", [new("Id", DbType.Int32, false)]);

            SchemaSnapshot.LoadFromFile(path, targetDb);

            Assert.False(targetDb.TryGetTable("Legacy", out _));
            var users = targetDb.GetTable("Users");
            Assert.Equal(9, users.NextIdentity);
            Assert.Equal("anonymous", users.Columns["Name"].DefaultValue);
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
            Assert.Contains("\"schemas\"", json, StringComparison.OrdinalIgnoreCase);

            var targetDb = new SqliteDbMock();
            targetDb.AddTable("Legacy", [new("Id", DbType.Int32, false)]);

            using var targetConnection = new SqliteConnectionMock(targetDb);
            targetConnection.ImportSchemaSnapshotFromFile(path);

            Assert.False(targetConnection.TryGetTable("Legacy", out _));
            Assert.True(targetConnection.TryGetTable("Users", out var usersTable));
            Assert.NotNull(usersTable);
            Assert.Equal(2, usersTable!.Columns.Count);
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
            Assert.True(jsonConnection.TryGetTable("Users", out var jsonUsers));
            Assert.NotNull(jsonUsers);

            using var fileConnection = new SqliteConnectionMock(new SqliteDbMock(version: 6));
            SchemaSnapshot.LoadFromFile(path, fileConnection, ensureCompatibility: true);
            Assert.True(fileConnection.TryGetTable("Users", out var fileUsers));
            Assert.NotNull(fileUsers);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }
}

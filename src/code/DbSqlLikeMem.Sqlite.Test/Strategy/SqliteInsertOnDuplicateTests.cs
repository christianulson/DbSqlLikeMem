namespace DbSqlLikeMem.Sqlite.Test.Strategy;

/// <summary>
/// EN: Covers INSERT ... ON DUPLICATE scenarios in the Sqlite mock.
/// PT-br: Cobre cenarios de INSERT ... ON DUPLICATE no mock Sqlite.
/// </summary>
public class SqliteInsertOnDuplicateTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies SQLite rejects MySQL-style INSERT ... ON DUPLICATE KEY syntax.
    /// PT-br: Verifica se o SQLite rejeita a sintaxe MySQL-style INSERT ... ON DUPLICATE KEY.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataSqliteVersion]
    public void Insert_OnDuplicate_ShouldInsertWhenNoConflict(int version)
    {
        var db = new SqliteDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        using var cnn = new SqliteConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'A') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        Action act = () =>
        {
            var q = SqlQueryParser.Parse(sql, db, db.Dialect);
            cnn.ExecuteInsert((SqlInsertQuery)q, new SqliteDataParameterCollectionMock(), db.Dialect);
        };

        act.Should().Throw<NotSupportedException>()
            .Which.Message.Should().Contain("ON DUPLICATE KEY UPDATE");
    }

    /// <summary>
    /// EN: Verifies SQLite rejects MySQL-style INSERT ... ON DUPLICATE KEY updates with VALUES().
    /// PT-br: Verifica se o SQLite rejeita atualizacoes MySQL-style INSERT ... ON DUPLICATE KEY com VALUES().
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataSqliteVersion]
    public void Insert_OnDuplicate_ShouldUpdateExistingRow_ByPrimaryKey_UsingValues(int version)
    {
        var db = new SqliteDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "OLD" } });

        using var cnn = new SqliteConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        Action act = () =>
        {
            var q = SqlQueryParser.Parse(sql, db, db.Dialect);
            cnn.ExecuteInsert((SqlInsertQuery)q, new SqliteDataParameterCollectionMock(), db.Dialect);
        };

        act.Should().Throw<NotSupportedException>()
            .Which.Message.Should().Contain("ON DUPLICATE KEY UPDATE");
    }

    /// <summary>
    /// EN: Verifies SQLite rejects MySQL-style INSERT ... ON DUPLICATE KEY updates matched by a unique index.
    /// PT-br: Verifica se o SQLite rejeita atualizacoes MySQL-style INSERT ... ON DUPLICATE KEY encontradas por um indice unico.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataSqliteVersion]
    public void Insert_OnDuplicate_ShouldUpdateExistingRow_ByUniqueIndex(int version)
    {
        var db = new SqliteDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Email", DbType.String, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.CreateIndex("UQ_Email", ["Email"], unique: true);

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "a@a.com" }, { 2, "A" } });

        using var cnn = new SqliteConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Email, Name) VALUES (2, 'a@a.com', 'B') " +
                  "ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        Action act = () =>
        {
            var q = SqlQueryParser.Parse(sql, db, db.Dialect);
            cnn.ExecuteInsert((SqlInsertQuery)q, new SqliteDataParameterCollectionMock(), db.Dialect);
        };

        act.Should().Throw<NotSupportedException>()
            .Which.Message.Should().Contain("ON DUPLICATE KEY UPDATE");
    }

    /// <summary>
    /// EN: Verifies SQLite rejects MySQL-style INSERT ... ON DUPLICATE KEY updates with mixed literals and parameters.
    /// PT-br: Verifica se o SQLite rejeita atualizacoes MySQL-style INSERT ... ON DUPLICATE KEY com literais e parametros mistos.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataSqliteVersion]
    public void Insert_OnDuplicate_ShouldUpdateUsingLiteralAndParam(int version)
    {
        var db = new SqliteDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "OLD" } });

        using var cnn = new SqliteConnectionMock(db);

        // usa @p0 no insert e @p1 no update, só exemplo
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO users (Id, Name) VALUES (@p0, @p1) " +
                          "ON DUPLICATE KEY UPDATE Name = 'FORCED'"
        };
        cmd.Parameters.Add(new SqliteParameter("p0", 1));
        cmd.Parameters.Add(new SqliteParameter("p1", "NEW"));

        Action act = () => cmd.ExecuteNonQuery();

        act.Should().Throw<NotSupportedException>()
            .Which.Message.Should().Contain("ON DUPLICATE KEY UPDATE");
    }

    /// <summary>
    /// EN: Verifies SQLite rejects MySQL-style INSERT ... ON DUPLICATE KEY updates that aggregate existing and incoming values.
    /// PT-br: Verifica se o SQLite rejeita atualizacoes MySQL-style INSERT ... ON DUPLICATE KEY que agregam valores existentes e recebidos.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataSqliteVersion]
    public void Insert_OnDuplicate_ShouldUpdateAggragating(int version)
    {
        var db = new SqliteDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Qtd", DbType.Int32, false);
        t.AddPrimaryKeyIndexes("id");

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 1 } });

        using var cnn = new SqliteConnectionMock(db);

        // usa @p0 no insert e @p1 no update, só exemplo
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = @"
INSERT INTO users (Id, Qtd) VALUES (@p0, @p1)
 ON DUPLICATE KEY UPDATE Qtd = users.Qtd + VALUES(Qtd)"
        };
        cmd.Parameters.Add(new SqliteParameter("p0", 1));
        cmd.Parameters.Add(new SqliteParameter("p1", 1));

        Action act = () => cmd.ExecuteNonQuery();

        act.Should().Throw<NotSupportedException>()
            .Which.Message.Should().Contain("ON DUPLICATE KEY UPDATE");
    }

    /// <summary>
    /// EN: Verifies ON CONFLICT DO NOTHING leaves the existing row unchanged.
    /// PT-br: Verifica se ON CONFLICT DO NOTHING mantem a linha existente sem alteracao.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataSqliteVersion]
    public void Insert_OnConflict_DoNothing_ShouldNotUpdate_WhenConflict(int version)
    {
        var db = new SqliteDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new SqliteConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON CONFLICT (Id) DO NOTHING";
        var q = SqlQueryParser.Parse(sql, db, db.Dialect);
        var affected = cnn.ExecuteInsert((SqlInsertQuery)q, new SqliteDataParameterCollectionMock(), db.Dialect);

        affected.AffectedRows.Should().Be(0);
        t.Should().ContainSingle();
        t[0][1].Should().Be("OLD");
    }

    /// <summary>
    /// EN: Verifies the ON CONFLICT DO UPDATE branch is skipped when the predicate is false.
    /// PT-br: Verifica se a ramificacao ON CONFLICT DO UPDATE e ignorada quando o predicado e falso.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataSqliteVersion]
    public void Insert_OnConflict_DoUpdateWhereFalse_ShouldSkipUpdate_WhenConflict(int version)
    {
        var db = new SqliteDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new SqliteConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name WHERE 1 = 0";
        var q = SqlQueryParser.Parse(sql, db, db.Dialect);
        var affected = cnn.ExecuteInsert((SqlInsertQuery)q, new SqliteDataParameterCollectionMock(), db.Dialect);

        affected.AffectedRows.Should().Be(0);
        t.Should().ContainSingle();
        t[0][1].Should().Be("OLD");
    }

    /// <summary>
    /// EN: Verifies the ON CONFLICT DO UPDATE branch applies when the predicate is true.
    /// PT-br: Verifica se a ramificacao ON CONFLICT DO UPDATE e aplicada quando o predicado e verdadeiro.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataSqliteVersion]
    public void Insert_OnConflict_DoUpdateWhereTrue_ShouldApplyUpdate_WhenConflict(int version)
    {
        var db = new SqliteDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new SqliteConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name WHERE users.id = EXCLUDED.id";
        var q = SqlQueryParser.Parse(sql, db, db.Dialect);
        var affected = cnn.ExecuteInsert((SqlInsertQuery)q, new SqliteDataParameterCollectionMock(), db.Dialect);

        affected.AffectedRows.Should().Be(1);
        t.Should().ContainSingle();
        t[0][1].Should().Be("NEW");
    }
}

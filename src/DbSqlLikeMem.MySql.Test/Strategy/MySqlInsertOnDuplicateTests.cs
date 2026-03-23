namespace DbSqlLikeMem.MySql.Test.Strategy;

/// <summary>
/// EN: Covers INSERT ... ON DUPLICATE scenarios in the MySql mock.
/// PT: Cobre cenarios de INSERT ... ON DUPLICATE no mock MySql.
/// </summary>
public class MySqlInsertOnDuplicateTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that INSERT ... ON DUPLICATE inserts when there is no conflict.
    /// PT: Verifica se INSERT ... ON DUPLICATE insere quando nao ha conflito.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldInsertWhenNoConflict(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        using var cnn = new MySqlConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'A') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        var q = SqlQueryParser.Parse(sql, db.Dialect);
        var affected = cnn.ExecuteInsert((SqlInsertQuery)q, new MySqlDataParameterCollectionMock(), db.Dialect);

        Assert.Equal(1, affected.AffectedRows);
        Assert.Single(t);
        Assert.Equal("A", t[0][1]);
    }

    /// <summary>
    /// EN: Verifies that INSERT ... ON DUPLICATE updates an existing row by primary key.
    /// PT: Verifica se INSERT ... ON DUPLICATE atualiza uma linha existente pela chave primaria.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateExistingRow_ByPrimaryKey_UsingValues(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "OLD" } });

        using var cnn = new MySqlConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        var q = SqlQueryParser.Parse(sql, db.Dialect);
        var affected = cnn.ExecuteInsert((SqlInsertQuery)q, new MySqlDataParameterCollectionMock(), db.Dialect);

        // MySQL real pode retornar 2 dependendo flags; no mock mantenha 1 ou 2, mas seja consistente.
        Assert.Equal("NEW", t[0][1]);
        Assert.Single(t);
    }

    /// <summary>
    /// EN: Verifies that a conflict update reports two affected rows.
    /// PT: Verifica se um update por conflito reporta duas linhas afetadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldReturnTwoAffectedRows_WhenConflictUpdates(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new MySqlConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        var q = SqlQueryParser.Parse(sql, db.Dialect);
        var affected = cnn.ExecuteInsert((SqlInsertQuery)q, new MySqlDataParameterCollectionMock(), db.Dialect);

        Assert.Equal(2, affected.AffectedRows);
        Assert.Single(t);
        Assert.Equal("NEW", t[0][1]);
    }

    /// <summary>
    /// EN: Verifies that INSERT ... ON DUPLICATE updates rows matched by a unique index.
    /// PT: Verifica se INSERT ... ON DUPLICATE atualiza linhas batidas por indice unico.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateExistingRow_ByUniqueIndex(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Email", DbType.String, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.CreateIndex("UQ_Email", ["Email"], unique: true);

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "a@a.com" }, { 2, "A" } });

        using var cnn = new MySqlConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Email, Name) VALUES (2, 'a@a.com', 'B') " +
                  "ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        var q = SqlQueryParser.Parse(sql, db.Dialect);
        cnn.ExecuteInsert((SqlInsertQuery)q, new MySqlDataParameterCollectionMock(), db.Dialect);

        Assert.Single(t);
        Assert.Equal(1, t[0][0]);          // Id original preservado
        Assert.Equal("B", t[0][2]);        // atualizado
    }

    /// <summary>
    /// EN: Verifies that INSERT ... ON DUPLICATE accepts literal and parameter values.
    /// PT: Verifica se INSERT ... ON DUPLICATE aceita valores literais e parametros.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateUsingLiteralAndParam(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "OLD" } });

        using var cnn = new MySqlConnectionMock(db);

        // usa @p0 no insert e @p1 no update, só exemplo
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "INSERT INTO users (Id, Name) VALUES (@p0, @p1) " +
                          "ON DUPLICATE KEY UPDATE Name = 'FORCED'"
        };
        cmd.Parameters.Add(new MySqlParameter("p0", 1));
        cmd.Parameters.Add(new MySqlParameter("p1", "NEW"));

        var rows = cmd.ExecuteNonQuery(); // tem que chamar ExecuteInsert internamente

        Assert.Equal("FORCED", t[0][1]);
    }

    /// <summary>
    /// EN: Verifies that INSERT ... ON DUPLICATE can update with aggregating expressions.
    /// PT: Verifica se INSERT ... ON DUPLICATE pode atualizar com expressoes agregadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateAggragating(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Qtd", DbType.Int32, false);
        t.AddPrimaryKeyIndexes("id");

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 1 } });

        using var cnn = new MySqlConnectionMock(db);

        // usa @p0 no insert e @p1 no update, só exemplo
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = @"
INSERT INTO users (Id, Qtd) VALUES (@p0, @p1)
 ON DUPLICATE KEY UPDATE Qtd = users.Qtd + VALUES(Qtd)"
        };
        cmd.Parameters.Add(new MySqlParameter("p0", 1));
        cmd.Parameters.Add(new MySqlParameter("p1", 1));

        var rows = cmd.ExecuteNonQuery(); // tem que chamar ExecuteInsert internamente

        Assert.Equal(2, t[0][1]);
    }
}

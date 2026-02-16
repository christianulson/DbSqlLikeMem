namespace DbSqlLikeMem.MySql.Test.Strategy;

/// <summary>
/// EN: Tests for INSERT ... ON DUPLICATE behavior.
/// PT: Testes para comportamento de INSERT ... ON DUPLICATE.
/// </summary>
public class MySqlInsertOnDuplicateTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Insert_OnDuplicate_ShouldInsertWhenNoConflict behavior.
    /// PT: Testa o comportamento de Insert_OnDuplicate_ShouldInsertWhenNoConflict.
    /// </summary>
    [Theory]
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

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("A", t[0][1]);
    }

    /// <summary>
    /// EN: Tests Insert_OnDuplicate_ShouldUpdateExistingRow_ByPrimaryKey_UsingValues behavior.
    /// PT: Testa o comportamento de Insert_OnDuplicate_ShouldUpdateExistingRow_ByPrimaryKey_UsingValues.
    /// </summary>
    [Theory]
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
    /// EN: Tests Insert_OnDuplicate_ShouldUpdateExistingRow_ByUniqueIndex behavior.
    /// PT: Testa o comportamento de Insert_OnDuplicate_ShouldUpdateExistingRow_ByUniqueIndex.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateExistingRow_ByUniqueIndex(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Email", DbType.String, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.CreateIndex(new IndexDef("UQ_Email", ["Email"], unique: true));

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
    /// EN: Tests Insert_OnDuplicate_ShouldUpdateUsingLiteralAndParam behavior.
    /// PT: Testa o comportamento de Insert_OnDuplicate_ShouldUpdateUsingLiteralAndParam.
    /// </summary>
    [Theory]
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
    /// EN: Tests Insert_OnDuplicate_ShouldUpdateAggragating behavior.
    /// PT: Testa o comportamento de Insert_OnDuplicate_ShouldUpdateAggragating.
    /// </summary>
    [Theory]
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

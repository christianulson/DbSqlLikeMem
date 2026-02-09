namespace DbSqlLikeMem.MySql.Test.Strategy;

/// <summary>
/// EN: Tests for INSERT ... ON DUPLICATE behavior.
/// PT: Testes para comportamento de INSERT ... ON DUPLICATE.
/// </summary>
public class MySqlInsertOnDuplicateTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    [Theory]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldInsertWhenNoConflict(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

        using var cnn = new MySqlConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'A') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        var q = SqlQueryParser.Parse(sql, db.Dialect);
        var affected = cnn.ExecuteInsert((SqlInsertQuery)q, new MySqlDataParameterCollectionMock(), db.Dialect);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("A", t[0][1]);
    }

    [Theory]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateExistingRow_ByPrimaryKey_UsingValues(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "OLD" } });

        using var cnn = new MySqlConnectionMock(db);

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";
        var q = SqlQueryParser.Parse(sql, db.Dialect);
        var affected = cnn.ExecuteInsert((SqlInsertQuery)q, new MySqlDataParameterCollectionMock(), db.Dialect);

        // MySQL real pode retornar 2 dependendo flags; no mock mantenha 1 ou 2, mas seja consistente.
        Assert.Equal("NEW", t[0][1]);
        Assert.Single(t);
    }

    [Theory]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateExistingRow_ByUniqueIndex(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Email"] = new ColumnDef(1, DbType.String, false);
        t.Columns["Name"] = new ColumnDef(2, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);
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

    [Theory]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateUsingLiteralAndParam(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

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

    [Theory]
    [MemberDataMySqlVersion]
    public void Insert_OnDuplicate_ShouldUpdateAggragating(int version)
    {
        var db = new MySqlDbMock(version);
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Qtd"] = new ColumnDef(1, DbType.Int32, false);
        t.PrimaryKeyIndexes.Add(0);

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
